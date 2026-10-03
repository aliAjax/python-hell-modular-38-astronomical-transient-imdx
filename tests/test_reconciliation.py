import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from src.domain import (
    Actor,
    ConflictError,
    NotFoundError,
    PermissionDenied,
    ValidationError,
)
from src.repository import SQLiteRepository
from src.reconciliation import ReconciliationService
from src.rules import RuleEngine
from src.service import DomainService


def batch(batch_id, source, entries):
    return {
        "batch_id": batch_id,
        "source": source,
        "results": [
            {"window_id": wid, "conclusion": conclusion}
            for wid, conclusion in entries
        ],
    }


class ReconciliationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "recon.db"
        self.service = DomainService(SQLiteRepository(self.db_path), RuleEngine())
        self.operator = Actor("operator-1", "operator")
        self.coordinator = Actor("coordinator-1", "coordinator")
        self.supervisor = Actor("supervisor-1", "supervisor")
        self.viewer = Actor("viewer-1", "viewer")

    def tearDown(self):
        self.tmp.cleanup()

    def _schedule_window(self, team="team-north", start="2026-10-03T10:00:00Z",
                         end="2026-10-03T11:00:00Z"):
        analyst = Actor("analyst-1", "analyst")
        source = self.service.create(
            analyst, "source", {"name": "Survey", "survey_name": "S"}
        )
        candidate = self.service.create(
            analyst,
            "candidate",
            {
                "source_id": source["id"],
                "event_id": "AT-%s-%s" % (team, start),
                "ra": 10,
                "dec": 20,
                "magnitude": 18,
                "transient_type": "unknown",
                "observed_at": "2026-10-01T00:00:00Z",
            },
        )
        telescope = self.service.create(
            self.coordinator,
            "telescope",
            {"name": "North 2m", "aperture_m": 2.0, "site_name": "NAO"},
        )
        observation = self.service.create(
            self.coordinator,
            "observation",
            {
                "candidate_id": candidate["id"],
                "telescope_id": telescope["id"],
                "team_id": team,
                "start_at": start,
                "end_at": end,
                "mode": "imaging",
            },
        )
        observation = self.service.transition(
            self.coordinator, observation["id"], "schedule", {"operator_id": "op-1"}
        )
        return observation

    def _audit_actions(self, entity_id=None):
        return [row["action"] for row in self.service.audit_log(entity_id)]

    # ------------------------------------------------------------------
    # Duplicate / out-of-order delivery
    # ------------------------------------------------------------------

    def test_duplicate_batch_is_booked_once(self):
        window = self._schedule_window()
        payload = batch("B1", "local", [(window["id"], "success")])

        first = self.service.ingest_result_batch(self.operator, payload)
        self.assertEqual(first["status"], "ingested")

        # Exact re-delivery of the same batch must not post a second time.
        replay = self.service.ingest_result_batch(self.operator, json.loads(json.dumps(payload)))
        self.assertEqual(replay["id"], first["id"])
        self.assertEqual(replay["data"]["processed_windows"], [window["id"]])
        self.assertTrue(replay["data"].get("replayed"))

        window = self.service.get(window["id"])
        self.assertEqual(window["status"], "completed")
        results = self.service.list("window_results")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["status"], "recorded")

    def test_out_of_order_same_conclusion_is_accepted(self):
        window = self._schedule_window()
        # Archive copy arrives first, the local copy arrives later (disorder).
        self.service.ingest_result_batch(
            self.operator, batch("BA", "archive", [(window["id"], "success")])
        )
        self.service.ingest_result_batch(
            self.operator, batch("BL", "local", [(window["id"], "success")])
        )
        refreshed = self.service.get(window["id"])
        self.assertEqual(refreshed["status"], "completed")
        self.assertEqual(set(refreshed["data"]["result_sources"]), {"local", "archive"})
        results = sorted(self.service.list("window_results"), key=lambda r: r["data"]["seq"])
        self.assertEqual([r["data"]["seq"] for r in results], [1, 2])
        self.assertEqual([r["data"]["source"] for r in results], ["archive", "local"])

    # ------------------------------------------------------------------
    # Success and failure handling
    # ------------------------------------------------------------------

    def test_success_updates_observation(self):
        window = self._schedule_window()
        self.service.ingest_result_batch(
            self.operator, batch("B2", "local", [(window["id"], "success")])
        )
        refreshed = self.service.get(window["id"])
        self.assertEqual(refreshed["status"], "completed")
        self.assertEqual(refreshed["data"]["result_conclusion"], "success")
        self.assertIn("result_success", self._audit_actions(window["id"]))

    def test_failure_releases_slots_and_requests_makeup(self):
        window = self._schedule_window()
        self.service.ingest_result_batch(
            self.operator, batch("B3", "local", [(window["id"], "failure")])
        )
        released = self.service.get(window["id"])
        self.assertEqual(released["status"], "released")
        self.assertIsNotNone(released["data"].get("makeup_observation_id"))
        makeup = self.service.get(released["data"]["makeup_observation_id"])
        self.assertEqual(makeup["status"], "requested")
        self.assertEqual(makeup["data"]["makeup_for_window"], window["id"])

        # The telescope and team time slot are free again: an overlapping
        # observation can be scheduled without conflict.
        candidate_id = self.service.list("candidates")[0]["id"]
        overlapper = self.service.create(
            self.coordinator,
            "observation",
            {
                "candidate_id": candidate_id,
                "telescope_id": released["data"]["telescope_id"],
                "team_id": "team-north",
                "start_at": "2026-10-03T10:45:00Z",
                "end_at": "2026-10-03T11:45:00Z",
                "mode": "imaging",
            },
        )
        scheduled = self.service.transition(
            self.coordinator, overlapper["id"], "schedule", {"operator_id": "op-1"}
        )
        self.assertEqual(scheduled["status"], "awaiting_result")

    # ------------------------------------------------------------------
    # Conflicting conclusions
    # ------------------------------------------------------------------

    def test_contradictory_conclusions_are_held_pending_confirmation(self):
        window = self._schedule_window()
        self.service.ingest_result_batch(
            self.operator, batch("B4L", "local", [(window["id"], "success")])
        )
        # Archive review disagrees with the local feed.
        self.service.ingest_result_batch(
            self.operator, batch("B4A", "archive", [(window["id"], "failure")])
        )
        held = self.service.get(window["id"])
        self.assertEqual(held["status"], "result_pending")
        self.assertEqual(held["data"]["result_state"], "pending_confirmation")
        # Arrival order is recorded.
        order = held["data"]["dispute"]["arrival_order"]
        self.assertEqual([item["source"] for item in order], ["local", "archive"])
        self.assertEqual([item["conclusion"] for item in order], ["success", "failure"])
        # Both contradictory conclusions are retained.
        results = self.service.list("window_results")
        self.assertEqual(len(results), 2)
        self.assertTrue(all(r["status"] == "pending_confirmation" for r in results))
        # No archive, no reschedule: slot is still held against new bookings.
        candidate_id = self.service.list("candidates")[0]["id"]
        blocker = self.service.create(
            self.coordinator,
            "observation",
            {
                "candidate_id": candidate_id,
                "telescope_id": held["data"]["telescope_id"],
                "team_id": "another-team",
                "start_at": "2026-10-03T10:30:00Z",
                "end_at": "2026-10-03T10:45:00Z",
                "mode": "imaging",
            },
        )
        with self.assertRaises(ConflictError):
            self.service.transition(
                self.coordinator, blocker["id"], "schedule", {"operator_id": "op-1"}
            )

    def test_failure_then_success_rolls_back_and_cancels_makeup(self):
        window = self._schedule_window()
        self.service.ingest_result_batch(
            self.operator, batch("B5L", "local", [(window["id"], "failure")])
        )
        released = self.service.get(window["id"])
        makeup_id = released["data"]["makeup_observation_id"]
        # Archive review claims the window actually succeeded.
        self.service.ingest_result_batch(
            self.operator, batch("B5A", "archive", [(window["id"], "success")])
        )
        held = self.service.get(window["id"])
        self.assertEqual(held["status"], "result_pending")
        makeup = self.service.get(makeup_id)
        self.assertEqual(makeup["status"], "withdrawn")
        self.assertTrue(held["data"]["slot_reacquired"])
        self.assertIn("result_conflict", self._audit_actions(window["id"]))

    def test_success_then_failure_holds_without_makeup_before_confirmation(self):
        window = self._schedule_window()
        self.service.ingest_result_batch(
            self.operator, batch("B6L", "local", [(window["id"], "success")])
        )
        self.service.ingest_result_batch(
            self.operator, batch("B6A", "archive", [(window["id"], "failure")])
        )
        held = self.service.get(window["id"])
        self.assertEqual(held["status"], "result_pending")
        # A failure has not been confirmed, so no make-up request exists yet.
        makeups = [
            o for o in self.service.list("observations")
            if o["data"].get("makeup_for_window") == window["id"]
        ]
        self.assertEqual(makeups, [])

    def test_confirmation_releases_and_creates_makeup(self):
        window = self._schedule_window()
        self.service.ingest_result_batch(
            self.operator, batch("B7L", "local", [(window["id"], "success")])
        )
        self.service.ingest_result_batch(
            self.operator, batch("B7A", "archive", [(window["id"], "failure")])
        )
        confirmed = self.service.confirm_window_result(
            self.coordinator, window["id"], "failure", "archive image shows cloud abort"
        )
        self.assertEqual(confirmed["status"], "released")
        self.assertEqual(confirmed["data"]["result_state"], "human_confirmed")
        self.assertIsNotNone(confirmed["data"]["makeup_observation_id"])
        # Both conclusions are still retained, now marked with the decision.
        results = self.service.list("window_results")
        self.assertEqual(len(results), 2)
        self.assertTrue(all(r["status"] == "confirmed_failure" for r in results))

    def test_confirmation_success_completes(self):
        window = self._schedule_window()
        self.service.ingest_result_batch(
            self.operator, batch("B8L", "local", [(window["id"], "failure")])
        )
        first_makeup = self.service.get(window["id"])["data"]["makeup_observation_id"]
        self.service.ingest_result_batch(
            self.operator, batch("B8A", "archive", [(window["id"], "success")])
        )
        confirmed = self.service.confirm_window_result(
            self.supervisor, window["id"], "success", "valid data confirmed by review"
        )
        self.assertEqual(confirmed["status"], "completed")
        self.assertEqual(self.service.get(first_makeup)["status"], "withdrawn")

    def test_confirmation_requires_role_and_reason(self):
        window = self._schedule_window()
        self.service.ingest_result_batch(
            self.operator, batch("B9L", "local", [(window["id"], "success")])
        )
        self.service.ingest_result_batch(
            self.operator, batch("B9A", "archive", [(window["id"], "failure")])
        )
        with self.assertRaises(PermissionDenied):
            self.service.confirm_window_result(
                self.operator, window["id"], "failure", "nope"
            )
        with self.assertRaises(ValidationError):
            self.service.confirm_window_result(
                self.coordinator, window["id"], "failure", "  "
            )
        # Rejections are audited.
        actions = self._audit_actions(window["id"])
        self.assertIn("result_confirmation_denied", actions)

    def test_late_contradiction_after_confirmation_is_rejected(self):
        window = self._schedule_window(team="team-c", start="2026-10-06T10:00:00Z",
                                       end="2026-10-06T11:00:00Z")
        self.service.ingest_result_batch(
            self.operator, batch("B17L", "local", [(window["id"], "success")])
        )
        self.service.ingest_result_batch(
            self.operator, batch("B17A", "archive", [(window["id"], "failure")])
        )
        confirmed = self.service.confirm_window_result(
            self.coordinator, window["id"], "success", "valid data"
        )
        self.assertEqual(confirmed["status"], "completed")
        with self.assertRaises(ConflictError):
            self.service.ingest_result_batch(
                self.operator, batch("B17C", "archive", [(window["id"], "failure")])
            )
        self.assertEqual(self.service.get(window["id"])["status"], "completed")
        self.assertIn("result_after_confirmation_rejected", self._audit_actions(window["id"]))

    # ------------------------------------------------------------------
    # Batch failure, retry, no half writes
    # ------------------------------------------------------------------

    def test_failed_batch_keeps_written_windows_and_resumes(self):
        w1 = self._schedule_window(team="team-1", start="2026-10-04T10:00:00Z",
                                   end="2026-10-04T11:00:00Z")
        w2 = self._schedule_window(team="team-2", start="2026-10-04T12:00:00Z",
                                   end="2026-10-04T13:00:00Z")
        w3 = self._schedule_window(team="team-3", start="2026-10-04T14:00:00Z",
                                   end="2026-10-04T15:00:00Z")
        payload = batch("B10", "local", [
            (w1["id"], "success"),
            ("window-does-not-exist", "failure"),
            (w3["id"], "failure"),
        ])
        with self.assertRaises(NotFoundError):
            self.service.ingest_result_batch(self.operator, payload)

        failed_batch = self.service.get("B10")
        self.assertEqual(failed_batch["status"], "failed")
        self.assertEqual(failed_batch["data"]["processed_windows"], [w1["id"]])
        self.assertEqual(failed_batch["data"]["failed_windows"], ["window-does-not-exist"])
        # w1 is fully written, w3 is untouched (no half window).
        self.assertEqual(self.service.get(w1["id"])["status"], "completed")
        self.assertEqual(self.service.get(w3["id"])["status"], "awaiting_result")

        # Retrying the original batch resumes at the failing entry and skips
        # already written windows, but it cannot succeed while it still names
        # the unknown window: the retry contract rejects a changed payload.
        corrected_payload = batch("B10", "local", [
            (w1["id"], "success"),
            (w2["id"], "failure"),
            (w3["id"], "failure"),
        ])
        with self.assertRaises(ConflictError):
            self.service.ingest_result_batch(self.operator, corrected_payload)

        # Correct operational retry: a new batch carries the remaining windows.
        done = self.service.ingest_result_batch(
            self.operator,
            batch("B10b", "local", [(w2["id"], "failure"), (w3["id"], "failure")]),
        )
        self.assertEqual(done["status"], "ingested")
        self.assertEqual(self.service.get(w2["id"])["status"], "released")
        self.assertEqual(self.service.get(w3["id"])["status"], "released")

    def test_retry_with_changed_payload_is_rejected_and_audited(self):
        window = self._schedule_window()
        payload = batch("B11", "local", [(window["id"], "success")])
        self.service.ingest_result_batch(self.operator, payload)
        with self.assertRaises(ConflictError):
            self.service.ingest_result_batch(
                self.operator, batch("B11", "local", [(window["id"], "failure")])
            )
        actions = self._audit_actions("B11")
        self.assertIn("result_batch_replay_mismatch", actions)

    # ------------------------------------------------------------------
    # Permissions, versioning, concurrency
    # ------------------------------------------------------------------

    def test_feed_requires_privileged_role(self):
        payload = batch("B12", "local", [("whatever", "success")])
        with self.assertRaises(PermissionDenied):
            self.service.ingest_result_batch(self.viewer, payload)
        actions = self._audit_actions("B12")
        self.assertIn("result_batch_denied", actions)

    def test_stale_batch_version_is_rejected(self):
        window = self._schedule_window()
        payload = batch("B13", "local", [(window["id"], "success")])
        booked = self.service.ingest_result_batch(self.operator, payload)
        with self.assertRaises(ConflictError):
            self.service.ingest_result_batch(
                self.operator, payload, expected_version=booked["version"] - 1
            )
        actions = self._audit_actions("B13")
        self.assertIn("result_batch_stale", actions)

    def test_second_submitter_is_always_rejected(self):
        window = self._schedule_window(team="team-b", start="2026-10-05T10:00:00Z",
                                       end="2026-10-05T11:00:00Z")
        payload = batch("B14b", "local", [(window["id"], "success")])
        first = self.service.ingest_result_batch(self.operator, payload)
        self.assertEqual(first["data"]["booked_by"], "operator-1")
        # A different operator submits the same batch after completion.
        with self.assertRaises(ConflictError):
            self.service.ingest_result_batch(Actor("operator-2", "operator"), payload)
        actions = self._audit_actions("B14b")
        self.assertIn("result_batch_duplicate_submitter", actions)

    def test_concurrent_identical_submission_only_one_wins(self):
        window = self._schedule_window()
        payload = batch("B14", "local", [(window["id"], "success")])
        outcomes = []

        def submit(actor_name):
            actor = Actor(actor_name, "operator")
            try:
                result = self.service.ingest_result_batch(actor, payload)
                outcomes.append(("ok", result["id"], result["data"].get("booked_by")))
            except ConflictError as exc:
                outcomes.append(("conflict", str(exc)))

        threads = [threading.Thread(target=submit, args=("op-a",)),
                   threading.Thread(target=submit, args=("op-b",))]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(len(outcomes), 2)
        statuses = sorted(outcome[0] for outcome in outcomes)
        self.assertEqual(statuses, ["conflict", "ok"])
        stored = self.service.get("B14")
        winners = {outcome[2] for outcome in outcomes if outcome[0] == "ok"}
        self.assertEqual(winners, {stored["data"]["booked_by"]})
        self.assertEqual(self.service.get(window["id"])["status"], "completed")

    def test_confirmation_stale_version_is_rejected(self):
        window = self._schedule_window()
        self.service.ingest_result_batch(
            self.operator, batch("B15L", "local", [(window["id"], "success")])
        )
        self.service.ingest_result_batch(
            self.operator, batch("B15A", "archive", [(window["id"], "failure")])
        )
        held = self.service.get(window["id"])
        with self.assertRaises(ConflictError):
            self.service.confirm_window_result(
                self.coordinator, window["id"], "failure", "late",
                expected_version=held["version"] - 1,
            )
        self.assertIn("result_confirmation_stale", self._audit_actions(window["id"]))

    # ------------------------------------------------------------------
    # Upgrade migration
    # ------------------------------------------------------------------

    def test_historical_scheduled_windows_become_awaiting_result(self):
        # Build a database at the pre-reconciliation schema and populate it
        # directly, as an old release would have done.
        legacy_path = Path(self.tmp.name) / "legacy.db"
        connection = sqlite3.connect(legacy_path)
        connection.executescript("""
            CREATE TABLE entities (
                id TEXT PRIMARY KEY, kind TEXT NOT NULL, status TEXT NOT NULL,
                version INTEGER NOT NULL, data TEXT NOT NULL,
                created_by TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
            );
            CREATE TABLE audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT, entity_id TEXT NOT NULL,
                actor_id TEXT NOT NULL, actor_role TEXT NOT NULL, action TEXT NOT NULL,
                from_status TEXT, to_status TEXT NOT NULL, detail TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE idempotency (
                actor_id TEXT NOT NULL, idem_key TEXT NOT NULL, entity_id TEXT NOT NULL,
                created_at TEXT NOT NULL, PRIMARY KEY(actor_id, idem_key)
            );
        """)
        rows = [
            ("obs-scheduled", "observation", "scheduled"),
            ("obs-completed", "observation", "completed"),
            ("obs-requested", "observation", "requested"),
        ]
        for entity_id, kind, status in rows:
            connection.execute(
                "INSERT INTO entities VALUES (?, ?, ?, 1, '{}', 'op', '2026-09-01T00:00:00',"
                " '2026-09-01T00:00:00')",
                (entity_id, kind, status),
            )
        connection.commit()
        connection.close()

        migrated_repo = SQLiteRepository(legacy_path)
        migrated_service = DomainService(migrated_repo, RuleEngine())

        waiting = migrated_service.get("obs-scheduled")
        self.assertEqual(waiting["status"], "awaiting_result")
        # A historical window without returns must not be mistaken for a result.
        self.assertNotIn("result_conclusion", waiting["data"])
        # Other statuses are untouched.
        self.assertEqual(migrated_service.get("obs-completed")["status"], "completed")
        self.assertEqual(migrated_service.get("obs-requested")["status"], "requested")

        actions = [
            row["action"]
            for row in migrated_service.audit_log("obs-scheduled")
        ]
        self.assertIn("migration_awaiting_result", actions)

        # An awaiting window still holds its slot and accepts returned results.
        done = migrated_service.ingest_result_batch(
            self.operator, batch("B16", "local", [("obs-scheduled", "failure")])
        )
        self.assertEqual(done["status"], "ingested")
        self.assertEqual(migrated_service.get("obs-scheduled")["status"], "released")

        # Re-opening the same database must not migrate again.
        SQLiteRepository(legacy_path)
        self.assertEqual(migrated_service.get("obs-scheduled")["status"], "released")


if __name__ == "__main__":
    unittest.main()
