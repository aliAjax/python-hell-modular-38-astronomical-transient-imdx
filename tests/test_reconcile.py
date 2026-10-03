import tempfile
import threading
import unittest
from pathlib import Path

from src.domain import Actor, ConflictError, PermissionDenied
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class ReconcileTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(
            SQLiteRepository(Path(self.tmp.name) / "reconcile.db"),
            RuleEngine(),
        )
        self.analyst = Actor("analyst-1", "analyst")
        self.operator = Actor("operator-1", "operator")
        self.coordinator = Actor("coordinator-1", "coordinator")
        self.admin = Actor("admin-1", "admin")

    def tearDown(self):
        self.tmp.cleanup()

    def _scheduled_observation(self, event_id="AT-2026-1"):
        source = self.service.create(
            self.analyst,
            "source",
            {"name": "Survey", "survey_name": "S"},
        )
        candidate = self.service.create(
            self.analyst,
            "candidate",
            {
                "source_id": source["id"],
                "event_id": event_id,
                "ra": 10,
                "dec": 20,
                "magnitude": 18,
                "transient_type": "unknown",
                "observed_at": "2026-09-27T00:00:00Z",
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
                "team_id": "team-north",
                "start_at": "2026-09-28T10:00:00Z",
                "end_at": "2026-09-28T11:00:00Z",
                "mode": "imaging",
            },
        )
        observation = self.service.transition(
            self.coordinator, observation["id"], "schedule", {"operator_id": "op-1"}
        )
        return observation, candidate, telescope

    def _batch(self, key, source, window_id, conclusion, payload=None, actor=None):
        if actor is None:
            actor = self.coordinator if source == "archive" else self.operator
        return self.service.submit_result_batch(
            actor,
            key,
            source,
            [{"window_id": window_id, "conclusion": conclusion, "payload": payload or {}}],
        )

    def test_valid_window_updates_observation(self):
        observation, _, _ = self._scheduled_observation()
        self._batch("local-1", "local", observation["id"], "success", {"magnitude": 17.1})
        self._batch("archive-1", "archive", observation["id"], "success", {"magnitude": 17.0})
        window = self.service.get_window(observation["id"])
        self.assertEqual(window["status"], "confirmed")
        self.assertEqual(window["first_source"], "local")
        updated = self.service.get(observation["id"])
        self.assertEqual(updated["status"], "completed")
        self.assertEqual(updated["data"]["result"]["magnitude"], 17.0)

    def test_failed_window_releases_slots_and_creates_makeup(self):
        observation, candidate, telescope = self._scheduled_observation()
        self._batch("local-2", "local", observation["id"], "failure", {"reason": "weather"})
        self._batch("archive-2", "archive", observation["id"], "failure", {"reason": "weather"})
        window = self.service.get_window(observation["id"])
        self.assertEqual(window["status"], "confirmed")
        released = self.service.get(observation["id"])
        self.assertEqual(released["status"], "requested")
        # telescope and team slots released
        self.assertNotIn("telescope_id", released["data"])
        self.assertNotIn("team_id", released["data"])
        self.assertNotIn("start_at", released["data"])
        # follow-up observation request generated for the same candidate
        follow_ups = [
            entity
            for entity in self.service.list("observation")
            if entity["data"].get("makeup_of") == observation["id"]
        ]
        self.assertEqual(len(follow_ups), 1)
        self.assertEqual(follow_ups[0]["status"], "requested")
        self.assertEqual(follow_ups[0]["data"]["candidate_id"], candidate["id"])

    def test_conflicting_conclusions_are_held_until_confirmed(self):
        observation, _, _ = self._scheduled_observation()
        self._batch("local-3", "local", observation["id"], "success", {"magnitude": 17.1})
        self._batch("archive-3", "archive", observation["id"], "failure", {"reason": "weather"})
        window = self.service.get_window(observation["id"])
        self.assertEqual(window["status"], "pending_confirmation")
        # both conclusions retained, arrival order recorded
        self.assertEqual(window["local_conclusion"]["conclusion"], "success")
        self.assertEqual(window["archive_conclusion"]["conclusion"], "failure")
        self.assertEqual(window["first_source"], "local")
        # no archive, no reschedule while pending
        held = self.service.get(observation["id"])
        self.assertEqual(held["status"], "scheduled")
        follow_ups = [
            entity
            for entity in self.service.list("observation")
            if entity["data"].get("makeup_of") == observation["id"]
        ]
        self.assertEqual(follow_ups, [])
        # confirm with the local conclusion
        confirmed = self.service.confirm_window(
            self.coordinator, observation["id"], "local"
        )
        self.assertEqual(confirmed["status"], "confirmed")
        done = self.service.get(observation["id"])
        self.assertEqual(done["status"], "completed")
        self.assertTrue(self.service.audit_log(observation["id"]))

    def test_arrival_order_recorded_when_archive_arrives_first(self):
        observation, _, _ = self._scheduled_observation()
        self._batch("archive-4", "archive", observation["id"], "success", {"magnitude": 17.0})
        self._batch("local-4", "local", observation["id"], "success", {"magnitude": 17.1})
        window = self.service.get_window(observation["id"])
        self.assertEqual(window["first_source"], "archive")
        self.assertEqual(window["status"], "confirmed")

    def test_duplicate_batch_is_idempotent(self):
        observation, _, _ = self._scheduled_observation()
        batch1, created1 = self._batch("local-5", "local", observation["id"], "success")
        self.assertTrue(created1)
        # second source arrives, then the first batch is redelivered
        self._batch("archive-5", "archive", observation["id"], "success")
        version_before = self.service.get(observation["id"])["version"]
        batch2, created2 = self._batch("local-5", "local", observation["id"], "success")
        self.assertFalse(created2)
        self.assertEqual(batch1["id"], batch2["id"])
        # no duplicate writes: observation unchanged, no makeup created
        self.assertEqual(self.service.get(observation["id"])["version"], version_before)
        self.assertEqual(
            [e for e in self.service.list("observation") if e["data"].get("makeup_of")],
            [],
        )

    def test_batch_resumes_after_failure_without_half_write(self):
        observation, _, _ = self._scheduled_observation()
        missing = "observation-does-not-exist"
        batch, created = self.service.submit_result_batch(
            self.operator,
            "batch-6",
            "local",
            [
                {"window_id": observation["id"], "conclusion": "success", "payload": {}},
                {"window_id": missing, "conclusion": "success", "payload": {}},
            ],
        )
        self.assertTrue(created)
        self.assertEqual(batch["status"], "partial")
        items = self.service.get_batch("batch-6")["items"]
        by_window = {item["window_id"]: item for item in items}
        self.assertEqual(by_window[observation["id"]]["status"], "applied")
        self.assertEqual(by_window[missing]["status"], "failed")
        # the applied window is kept; reconcile it directly with archive to
        # prove the first item fully wrote
        self._batch("archive-6", "archive", observation["id"], "success")
        self.assertEqual(self.service.get_window(observation["id"])["status"], "confirmed")
        # the missing window finally exists
        self.service.repository.create_entity(
            missing,
            "observation",
            "requested",
            {"candidate_id": "candidate-x", "mode": "imaging"},
            self.operator.user_id,
        )
        # retry the same batch: applied item skipped, failed item retried
        retry, created_retry = self.service.submit_result_batch(
            self.operator,
            "batch-6",
            "local",
            [
                {"window_id": observation["id"], "conclusion": "success", "payload": {}},
                {"window_id": missing, "conclusion": "success", "payload": {}},
            ],
        )
        self.assertFalse(created_retry)
        self.assertEqual(retry["status"], "applied")
        items = self.service.get_batch("batch-6")["items"]
        self.assertTrue(all(item["status"] == "applied" for item in items))
        # the newly created window was now reconciled
        self.assertEqual(self.service.get_window(missing)["status"], "pending")

    def test_concurrent_same_batch_only_one_submission_wins(self):
        observation, _, _ = self._scheduled_observation()
        barrier = threading.Barrier(2)
        results = []

        def submit(conclusion):
            barrier.wait()
            try:
                batch, created = self.service.submit_result_batch(
                    self.operator,
                    "batch-7",
                    "local",
                    [{"window_id": observation["id"], "conclusion": conclusion, "payload": {}}],
                )
                results.append(("ok", created))
            except ConflictError as exc:
                results.append(("conflict", str(exc)))

        t1 = threading.Thread(target=submit, args=("success",))
        t2 = threading.Thread(target=submit, args=("failure",))
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        self.assertEqual(sorted(r[0] for r in results), ["conflict", "ok"])
        # exactly one batch row was written
        self.assertEqual(len(self.service.list_batches()), 1)
        self.assertTrue(self.service.audit_log("batch-7"))

    def test_stale_version_submission_rejected_with_audit(self):
        observation, _, _ = self._scheduled_observation()
        self._batch("batch-8", "local", observation["id"], "success")
        with self.assertRaises(ConflictError):
            self.service.submit_result_batch(
                self.operator,
                "batch-8",
                "local",
                [{"window_id": observation["id"], "conclusion": "success", "payload": {}}],
                expected_version=999,
            )
        audits = self.service.audit_log("batch-8")
        self.assertTrue(
            any(
                entry["action"] == "submit_batch"
                and entry["to_status"] == "rejected"
                and entry["detail"]["reason"] == "version_conflict"
                for entry in audits
            )
        )

    def test_unauthorized_submission_rejected_with_audit(self):
        observation, _, _ = self._scheduled_observation()
        with self.assertRaises(PermissionDenied):
            self.service.submit_result_batch(
                Actor("viewer-1", "viewer"),
                "batch-9",
                "local",
                [{"window_id": observation["id"], "conclusion": "success", "payload": {}}],
            )
        audits = self.service.audit_log("batch-9")
        self.assertTrue(
            any(
                entry["action"] == "submit_batch"
                and entry["to_status"] == "rejected"
                and entry["detail"]["reason"] == "permission_denied"
                for entry in audits
            )
        )

    def test_upgrade_backfills_pending_returns_idempotently(self):
        observation, _, _ = self._scheduled_observation()
        # precondition: no reconcile row for the historical observation
        with self.assertRaises(Exception):
            self.service.get_window(observation["id"])
        result = self.service.upgrade_pending_returns(self.admin)
        self.assertEqual(result["pending_returns_created"], 1)
        window = self.service.get_window(observation["id"])
        self.assertEqual(window["status"], "pending_return")
        self.assertIsNone(window["local_conclusion"])
        self.assertIsNone(window["archive_conclusion"])
        # upgrade is idempotent
        again = self.service.upgrade_pending_returns(self.admin)
        self.assertEqual(again["pending_returns_created"], 0)
        self.assertEqual(len(self.service.list_windows("pending_return")), 1)

    def test_confirm_requires_pending_window(self):
        observation, _, _ = self._scheduled_observation()
        self._batch("local-10", "local", observation["id"], "success")
        self._batch("archive-10", "archive", observation["id"], "success")
        with self.assertRaises(ConflictError):
            self.service.confirm_window(self.coordinator, observation["id"], "local")

    def test_archive_only_submission_allowed_for_coordinator(self):
        observation, _, _ = self._scheduled_observation()
        with self.assertRaises(PermissionDenied):
            self.service.submit_result_batch(
                self.operator,
                "batch-11",
                "archive",
                [{"window_id": observation["id"], "conclusion": "success", "payload": {}}],
            )

    def test_conflict_held_when_archive_arrives_first(self):
        observation, _, _ = self._scheduled_observation()
        self._batch("archive-12", "archive", observation["id"], "failure", {"reason": "weather"})
        self._batch("local-12", "local", observation["id"], "success")
        window = self.service.get_window(observation["id"])
        self.assertEqual(window["status"], "pending_confirmation")
        self.assertEqual(window["first_source"], "archive")
        # explicit decision object is also accepted
        confirmed = self.service.confirm_window(
            self.coordinator,
            observation["id"],
            {"conclusion": "failure", "payload": {"reason": "weather"}},
        )
        self.assertEqual(confirmed["status"], "confirmed")
        released = self.service.get(observation["id"])
        self.assertEqual(released["status"], "requested")

    def test_upgraded_window_accepts_later_results(self):
        observation, _, _ = self._scheduled_observation()
        self.service.upgrade_pending_returns(self.admin)
        self.assertEqual(self.service.get_window(observation["id"])["status"], "pending_return")
        self._batch("local-13", "local", observation["id"], "success")
        self._batch("archive-13", "archive", observation["id"], "success")
        window = self.service.get_window(observation["id"])
        self.assertEqual(window["status"], "confirmed")
        self.assertEqual(self.service.get(observation["id"])["status"], "completed")


if __name__ == "__main__":
    unittest.main()
