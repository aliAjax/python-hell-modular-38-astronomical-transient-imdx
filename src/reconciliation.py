from uuid import uuid4

from .audit import AuditTrail
from .domain import (
    CommitAndRaise,
    ConflictError,
    NotFoundError,
    PermissionDenied,
    ValidationError,
)
from .rules import SLOT_HOLDING_STATUSES, measurements_overlap

FEED_SOURCES = ("local", "archive")
CONCLUSIONS = ("success", "failure")
# Control-system feeds are accepted from operators, coordinators and admins.
FEED_ROLES = ("operator", "coordinator", "admin")
# Only a human coordinator/supervisor can adjudicate a disputed window.
CONFIRM_ROLES = ("coordinator", "supervisor", "admin")

# Observation statuses from which a returned conclusion may still land.
RECEPTIVE_STATUSES = ("scheduled", "awaiting_result", "result_pending")
# Statuses that count as an established, undisputed conclusion.
SETTLED_STATUS = {"success": "completed", "failure": "released"}


class ReconciliationService:
    """Reconciles result batches returned by the telescope control system.

    The guarantees implemented here are:

    * a batch is booked exactly once - replays resume instead of re-posting;
    * each (window, source) conclusion is retained and arrival order is kept;
    * conflicting local/archive conclusions freeze the window pending human
      confirmation - neither archive nor reschedule happens before that;
    * a failed window releases the telescope and team time slot and raises a
      make-up observing request;
    * every window is committed in one transaction, so no half-written
      window survives a failure; already written windows survive a batch
      failure and are skipped when the original batch is retried.
    """

    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules
        self.audit = AuditTrail(repository)

    # ------------------------------------------------------------------
    # Batch ingestion
    # ------------------------------------------------------------------

    def ingest_result_batch(self, actor, payload, expected_version=None):
        if actor.role not in FEED_ROLES:
            self._deny(
                actor,
                str((payload or {}).get("batch_id", "unknown")),
                "role %s is not allowed to feed result batches" % actor.role,
            )
            raise PermissionDenied(
                "role %s is not allowed to feed result batches" % actor.role
            )
        normalized = self._normalize_batch(payload)
        batch_id = normalized["batch_id"]

        existing = self.repository.get_entity(batch_id)
        if existing is not None:
            if existing["kind"] != "result_batch":
                raise ConflictError("batch id is already used by another entity: " + batch_id)
            if expected_version is not None and int(expected_version) != existing["version"]:
                self._deny(
                    actor,
                    batch_id,
                    "stale batch version: submitted %s, current %s"
                    % (expected_version, existing["version"]),
                    action="result_batch_stale",
                )
                raise ConflictError(
                    "stale batch version: submitted %s, current %s"
                    % (expected_version, existing["version"])
                )
            owner = existing["data"].get("booked_by")
            if owner != actor.user_id:
                # A batch has a single writer: the feed that booked it may
                # replay it, but a second person submitting the same batch is
                # rejected even if the first submission already finished.
                self._deny(
                    actor,
                    batch_id,
                    "batch %s is already booked by %s; a second submitter is not accepted"
                    % (batch_id, owner),
                    action="result_batch_duplicate_submitter",
                )
                raise ConflictError(
                    "batch %s is already booked by %s" % (batch_id, owner)
                )
            return self._resume_batch(actor, existing, normalized)

        try:
            batch = self.repository.run_in_transaction(
                lambda tx: tx.insert(
                    batch_id,
                    "result_batch",
                    "processing",
                    {
                        "source": normalized["source"],
                        "results": normalized["results"],
                        "processed_windows": [],
                        "failed_windows": [],
                        "ingested_at": None,
                        "booked_by": actor.user_id,
                    },
                    actor.user_id,
                )
            )
        except ConflictError:
            # Another submitter won the race for the same batch id; the loser
            # is rejected with an audit trail instead of double posting.
            self._deny(actor, batch_id, "batch already booked by a concurrent submitter")
            raise

        return self._continue_batch(actor, batch, normalized)

    def _resume_batch(self, actor, batch, normalized):
        stored = [
            {"window_id": item["window_id"], "conclusion": item["conclusion"]}
            for item in batch["data"]["results"]
        ]
        if stored != normalized["results"]:
            self._deny(
                actor,
                batch["id"],
                "batch %s retried with a different payload; resubmit the original batch"
                % batch["id"],
                action="result_batch_replay_mismatch",
            )
            raise ConflictError(
                "batch %s was already booked; a retry must carry the original payload"
                % batch["id"]
            )
        resumed = self._continue_batch(actor, batch, normalized)
        resumed["data"]["replayed"] = True
        return resumed

    def _continue_batch(self, actor, batch, normalized):
        processed = list(batch["data"].get("processed_windows", []))
        window_id = None
        try:
            for index, item in enumerate(normalized["results"]):
                window_id = item["window_id"]
                conclusion = item["conclusion"]
                if window_id in processed:
                    continue
                result = self.repository.run_in_transaction(
                    lambda tx, w=window_id, c=conclusion, s=normalized["source"], i=index:
                    self._apply_window(tx, actor, batch["id"], w, c, s, i)
                )
                batch = result["batch"]
                processed = list(batch["data"]["processed_windows"])
        except Exception as exc:
            # Already written windows stay committed; record the failure so the
            # original batch can be retried and resumes at the next window.
            self._mark_batch_failed(actor, batch, window_id, str(exc))
            raise

        batch = self.repository.run_in_transaction(
            lambda tx: self._finish_batch(tx, actor, batch["id"])
        )
        return batch

    def _finish_batch(self, tx, actor, batch_id):
        batch = tx.get(batch_id)
        data = dict(batch["data"])
        data["ingested_at"] = tx.now
        updated = tx.update(batch, "ingested", data)
        tx.audit(
            batch_id, actor.user_id, actor.role,
            "result_batch_ingested", batch["status"], "ingested",
            {"processed_windows": data["processed_windows"], "count": len(data["results"])},
        )
        return updated

    def _mark_batch_failed(self, actor, batch, window_id, reason):
        def work(tx):
            current = tx.get(batch["id"])
            data = dict(current["data"])
            failed = list(data.get("failed_windows", []))
            if window_id not in failed:
                failed.append(window_id)
            data["failed_windows"] = failed
            data["last_error"] = {"window_id": window_id, "reason": reason, "at": tx.now}
            updated = tx.update(current, "failed", data)
            tx.audit(
                batch["id"], actor.user_id, actor.role,
                "result_batch_failed", current["status"], "failed",
                {"window_id": window_id, "reason": reason},
            )
            return updated

        try:
            self.repository.run_in_transaction(work)
        except Exception:
            # Failing to record the failure marker must not mask the original.
            pass

    # ------------------------------------------------------------------
    # Per-window reconciliation - one transaction, no half writes
    # ------------------------------------------------------------------

    def _apply_window(self, tx, actor, batch_id, window_id, conclusion, source, batch_index):
        batch = tx.get(batch_id)
        if window_id in (batch["data"].get("processed_windows", []) if batch else []):
            # A concurrent replay of the same owner already booked this window
            # inside an earlier serialized transaction.
            return {"batch": batch, "result": None, "observation": None, "skipped": True}

        observation = tx.get(window_id)
        if observation is None or observation["kind"] != "observation":
            raise NotFoundError("observation window does not exist: " + window_id)

        prior = self._window_results(tx, window_id)
        arrival_seq = len(prior) + 1
        from_same_source = [r for r in prior if r["data"].get("source") == source]

        established = self._established_conclusion(prior)
        already_disputed = observation["status"] == "result_pending" or any(
            r["status"] == "pending_confirmation" for r in prior
        )

        result_data = {
            "batch_id": batch_id,
            "window_id": window_id,
            "source": source,
            "conclusion": conclusion,
            "seq": arrival_seq,
            "batch_index": batch_index,
            "arrived_at": tx.now,
        }
        result_id = "wr-" + uuid4().hex[:16]

        if observation["data"].get("result_state") == "human_confirmed":
            # A human has already adjudicated this window; later feeds cannot
            # re-open it. Reject inside the window transaction after writing
            # only an audit row, which is committed while the domain change
            # stays absent (no half-written window).
            reason = (
                "window %s is already human-confirmed; late %s result rejected"
                % (window_id, source)
            )
            tx.audit(
                window_id, actor.user_id, actor.role,
                "result_after_confirmation_rejected",
                observation["status"], observation["status"],
                {"batch_id": batch_id, "source": source, "conclusion": conclusion,
                 "seq": arrival_seq, "reason": reason},
            )
            raise CommitAndRaise(ConflictError(reason))

        if from_same_source and from_same_source[-1]["data"].get("conclusion") == conclusion:
            # Re-delivery of this feed's own conclusion. While the window is
            # disputed the second delivery must still be retained as evidence,
            # so it only short-circuits when there is no open dispute.
            if not already_disputed:
                result_data["duplicate_of"] = from_same_source[-1]["id"]
                result = tx.insert(
                    result_id, "window_result", "duplicate", result_data, actor.user_id
                )
                tx.audit(
                    window_id, actor.user_id, actor.role,
                    "result_duplicate", observation["status"], observation["status"],
                    {"batch_id": batch_id, "source": source, "conclusion": conclusion,
                     "seq": arrival_seq, "result_id": result["id"]},
                )
                batch = self._advance_batch(tx, actor, batch_id, window_id)
                return {"batch": batch, "result": result, "observation": observation}

        conflicting = already_disputed or (
            established is not None and established != conclusion
        )
        if conflicting:
            return self._record_dispute(
                tx, actor, batch_id, window_id, observation, prior, result_id,
                result_data, conclusion, established,
            )

        if observation["status"] not in RECEPTIVE_STATUSES:
            # The window is already settled by effective results and the
            # incoming conclusion agrees with them: corroborating evidence.
            if established == conclusion:
                return self._record_agreement(
                    tx, actor, batch_id, window_id, observation, prior,
                    result_id, result_data, conclusion,
                )
            raise ConflictError(
                "cannot post a result for window %s in status %s"
                % (window_id, observation["status"])
            )

        # First effective conclusion for this window.
        result = tx.insert(result_id, "window_result", "recorded", result_data, actor.user_id)
        if conclusion == "success":
            updated = self._mark_success(tx, actor, observation, result)
        else:
            updated = self._mark_failure(tx, actor, observation, result, batch_id)
        batch = self._advance_batch(tx, actor, batch_id, window_id)
        return {"batch": batch, "result": result, "observation": updated}

    def _record_agreement(self, tx, actor, batch_id, window_id, observation, prior,
                         result_id, result_data, conclusion):
        # A second feed (or a genuine re-delivery from a new source) agrees
        # with the already settled conclusion: keep the record, stamp arrival
        # order, no state change to the window.
        result_data["agrees_with"] = sorted({
            r["id"] for r in prior if r["status"] != "duplicate"
        })
        result = tx.insert(
            result_id, "window_result", "recorded", result_data, actor.user_id
        )
        # Refresh the observation's provenance list.
        obs_data = dict(observation["data"])
        sources = list(obs_data.get("result_sources", []))
        if result_data["source"] not in sources:
            sources.append(result_data["source"])
        obs_data["result_sources"] = sources
        updated = tx.update(observation, observation["status"], obs_data)
        tx.audit(
            window_id, actor.user_id, actor.role,
            "result_corroborated", observation["status"], observation["status"],
            {"batch_id": batch_id, "source": result_data["source"],
             "conclusion": conclusion, "seq": result_data["seq"],
             "result_id": result["id"]},
        )
        batch = self._advance_batch(tx, actor, batch_id, window_id)
        return {"batch": batch, "result": result, "observation": updated}

    def _record_dispute(self, tx, actor, batch_id, window_id, observation, prior,
                        result_id, result_data, conclusion, established):
        # Both contradictory conclusions are retained and stamped with their
        # arrival order; the window is frozen pending human confirmation.
        result_data["pending_reason"] = "conflicting local/archive conclusions"
        result = tx.insert(
            result_id, "window_result", "pending_confirmation", result_data, actor.user_id
        )
        for earlier in prior:
            if earlier["status"] in ("recorded", "pending_confirmation"):
                data = dict(earlier["data"])
                data["pending_reason"] = "conflicting local/archive conclusions"
                tx.update(earlier, "pending_confirmation", data)
                tx.audit(
                    window_id, actor.user_id, actor.role,
                    "result_pending_hold", earlier["status"], "pending_confirmation",
                    {"result_id": earlier["id"], "arrival_seq": data["seq"]},
                )

        obs_data = dict(observation["data"])
        sources = self._source_sequences(tx, window_id)
        obs_data["result_state"] = "pending_confirmation"
        obs_data["dispute"] = {
            "established_conclusion": established,
            "incoming_conclusion": conclusion,
            "arrival_order": sources,
            "opened_at": tx.now,
            "opened_by_batch": batch_id,
        }

        from_status = observation["status"]
        updated = tx.update(observation, "result_pending", obs_data)

        makeup_id = None
        if established == "failure":
            # The earlier failure had already released the slot and raised a
            # make-up request; roll that back, cancel the request and hold the
            # slot again until a human confirms.
            makeup_id = obs_data.get("makeup_observation_id")
            if makeup_id:
                self._cancel_makeup(tx, actor, makeup_id, window_id, reheld=True)
            conflict_with = self._slot_conflict(tx, updated, ignore=(window_id, makeup_id))
            updated = tx.get(window_id)
            data = dict(updated["data"])
            data["slot_reacquired"] = conflict_with is None
            if conflict_with:
                data["slot_conflict_with"] = conflict_with
            updated = tx.update(updated, "result_pending", data)

        tx.audit(
            window_id, actor.user_id, actor.role,
            "result_conflict", from_status, "result_pending",
            {"batch_id": batch_id, "established": established,
             "incoming": conclusion, "arrival_order": sources,
             "makeup_cancelled": makeup_id, "new_result_id": result["id"]},
        )
        batch = self._advance_batch(tx, actor, batch_id, window_id)
        return {"batch": batch, "result": result, "observation": updated}

    def _mark_success(self, tx, actor, observation, result):
        data = dict(observation["data"])
        data["result_conclusion"] = "success"
        data["result_state"] = "confirmed_by_sources"
        data["completed_at"] = tx.now
        data["result_batch_id"] = result["data"]["batch_id"]
        data["result_sources"] = [result["data"]["source"]]
        updated = tx.update(observation, "completed", data)
        tx.audit(
            observation["id"], actor.user_id, actor.role,
            "result_success", observation["status"], "completed",
            {"batch_id": result["data"]["batch_id"], "source": result["data"]["source"],
             "result_id": result["id"]},
        )
        return updated

    def _mark_failure(self, tx, actor, observation, result, batch_id):
        data = dict(observation["data"])
        data["result_conclusion"] = "failure"
        data["result_state"] = "confirmed_by_sources"
        data["failed_at"] = tx.now
        data["released_at"] = tx.now
        data["result_batch_id"] = batch_id
        data["result_sources"] = [result["data"]["source"]]
        updated = tx.update(observation, "released", data)

        makeup = self._create_makeup(tx, actor, updated, batch_id)
        data = dict(updated["data"])
        data["makeup_observation_id"] = makeup["id"]
        updated = tx.update(updated, "released", data)
        tx.audit(
            observation["id"], actor.user_id, actor.role,
            "result_failure", observation["status"], "released",
            {"batch_id": batch_id, "source": result["data"]["source"],
             "result_id": result["id"], "makeup_observation_id": makeup["id"]},
        )
        return updated

    def _create_makeup(self, tx, actor, failed_window, batch_id):
        data = failed_window["data"]
        makeup_id = "mk-" + uuid4().hex[:16]
        makeup_data = {
            "candidate_id": data.get("candidate_id"),
            "telescope_id": data.get("telescope_id"),
            "team_id": data.get("team_id"),
            "scheduled_team": data.get("team_id"),
            "start_at": data.get("start_at"),
            "end_at": data.get("end_at"),
            "mode": data.get("mode"),
            "makeup_for_window": failed_window["id"],
            "makeup_for_batch": batch_id,
            "created_from_failure_at": tx.now,
        }
        makeup = tx.insert(makeup_id, "observation", "requested", makeup_data, actor.user_id)
        tx.audit(
            makeup_id, actor.user_id, actor.role,
            "makeup_requested", None, "requested",
            {"window_id": failed_window["id"], "batch_id": batch_id},
        )
        return makeup

    def _cancel_makeup(self, tx, actor, makeup_id, window_id, reheld):
        makeup = tx.get(makeup_id)
        if makeup is None or makeup["kind"] != "observation":
            return None
        if makeup["status"] != "requested":
            # Only a still-pending request can be withdrawn; a scheduled or
            # completed make-up is left untouched for the coordinator.
            return makeup
        data = dict(makeup["data"])
        data["withdrawn_reason"] = "window re-held pending conflicting result confirmation"
        data["withdrawn_at"] = tx.now
        updated = tx.update(makeup, "withdrawn", data)
        tx.audit(
            makeup_id, actor.user_id, actor.role,
            "makeup_cancelled", makeup["status"], "withdrawn",
            {"window_id": window_id, "reheld": reheld},
        )
        return updated

    # ------------------------------------------------------------------
    # Human adjudication
    # ------------------------------------------------------------------

    def resolve_window(self, actor, window_id, confirmed_conclusion, reason,
                       expected_version=None):
        if actor.role not in CONFIRM_ROLES:
            self._deny(
                actor, window_id,
                "role %s is not allowed to confirm disputed results" % actor.role,
                action="result_confirmation_denied",
            )
            raise PermissionDenied(
                "role %s is not allowed to confirm disputed results" % actor.role
            )
        if confirmed_conclusion not in CONCLUSIONS:
            raise ValidationError("confirmed_conclusion must be 'success' or 'failure'")
        if not str(reason or "").strip():
            raise ValidationError("confirmation reason is required")

        if expected_version is not None:
            current = self.repository.get_entity(window_id)
            if current is not None and current["version"] != int(expected_version):
                self._deny(
                    actor, window_id,
                    "stale window version: submitted %s, current %s"
                    % (expected_version, current["version"]),
                    action="result_confirmation_stale",
                )
                raise ConflictError(
                    "stale window version: submitted %s, current %s"
                    % (expected_version, current["version"])
                )

        def work(tx):
            observation = tx.get(window_id)
            if observation is None or observation["kind"] != "observation":
                raise NotFoundError("observation window does not exist: " + window_id)
            pending = [
                r for r in self._window_results(tx, window_id)
                if r["status"] == "pending_confirmation"
            ]
            if not pending:
                raise ConflictError("window %s has no disputed conclusion to confirm" % window_id)

            target_status = SETTLED_STATUS[confirmed_conclusion]
            data = dict(observation["data"])
            data["result_conclusion"] = confirmed_conclusion
            data["result_state"] = "human_confirmed"
            data["confirmed_by"] = actor.user_id
            data["confirmed_reason"] = reason
            data["confirmed_at"] = tx.now
            data.pop("dispute", None)
            data.pop("slot_conflict_with", None)
            data["slot_reacquired"] = True

            makeup_id = data.get("makeup_observation_id")
            if confirmed_conclusion == "failure":
                data["released_at"] = tx.now
                if not self._active_makeup_exists(tx, window_id):
                    makeup = self._create_makeup(
                        tx, actor, observation,
                        data.get("result_batch_id") or "confirmation",
                    )
                    makeup_id = makeup["id"]
                    data["makeup_observation_id"] = makeup_id
            else:
                data["completed_at"] = tx.now
                if makeup_id:
                    self._cancel_makeup(tx, actor, makeup_id, window_id, reheld=False)

            from_status = observation["status"]
            updated = tx.update(observation, target_status, data)

            kept = []
            for result in pending:
                result_data = dict(result["data"])
                result_data["confirmed_at"] = tx.now
                result_data["confirmed_by"] = actor.user_id
                result_data["confirmation_reason"] = reason
                next_state = (
                    "confirmed_success" if confirmed_conclusion == "success"
                    else "confirmed_failure"
                )
                tx.update(result, next_state, result_data)
                kept.append({"result_id": result["id"], "source": result_data["source"],
                             "conclusion": result_data["conclusion"], "seq": result_data["seq"]})

            tx.audit(
                window_id, actor.user_id, actor.role,
                "result_confirmed", from_status, target_status,
                {"confirmed_conclusion": confirmed_conclusion, "reason": reason,
                 "retained_conclusions": kept, "makeup_observation_id": makeup_id},
            )
            return updated

        return self.repository.run_in_transaction(work)

    def _active_makeup_exists(self, tx, window_id):
        for entity in tx.list_kind("observation"):
            if entity["data"].get("makeup_for_window") == window_id and \
                    entity["status"] in ("requested", "scheduled"):
                return True
        return False

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _window_results(self, tx, window_id):
        results = [
            entity for entity in tx.list_kind("window_result")
            if entity["data"].get("window_id") == window_id
        ]
        return sorted(results, key=lambda r: (r["data"].get("seq", 0), r["created_at"], r["id"]))

    def _source_sequences(self, tx, window_id):
        return [
            {"seq": r["data"].get("seq"), "source": r["data"].get("source"),
             "conclusion": r["data"].get("conclusion"), "arrived_at": r["data"].get("arrived_at"),
             "status": r["status"]}
            for r in self._window_results(tx, window_id)
        ]

    @staticmethod
    def _established_conclusion(prior):
        """Agreed conclusion among effective (non-duplicate) results."""
        effective = [r for r in prior if r["status"] != "duplicate"]
        if not effective:
            return None
        conclusions = {r["data"].get("conclusion") for r in effective}
        if len(conclusions) == 1:
            return effective[0]["data"].get("conclusion")
        return "conflict"

    def _advance_batch(self, tx, actor, batch_id, window_id):
        batch = tx.get(batch_id)
        data = dict(batch["data"])
        processed = list(data.get("processed_windows", []))
        if window_id not in processed:
            processed.append(window_id)
        data["processed_windows"] = processed
        data["failed_windows"] = [
            w for w in data.get("failed_windows", []) if w != window_id
        ]
        data.pop("last_error", None)
        return tx.update(batch, "processing", data)

    def _slot_conflict(self, tx, observation, ignore=()):
        data = observation["data"]
        for other in tx.list_kind("observation"):
            if other["id"] in ignore or other["id"] == observation["id"]:
                continue
            if other["status"] not in SLOT_HOLDING_STATUSES:
                continue
            same_telescope = other["data"].get("telescope_id") == data.get("telescope_id")
            same_team = other["data"].get("team_id") == data.get("team_id")
            if not (same_telescope or same_team):
                continue
            if measurements_overlap(
                data.get("start_at"), data.get("end_at"),
                other["data"].get("start_at"), other["data"].get("end_at"),
            ):
                return other["id"]
        return None

    def _normalize_batch(self, payload):
        if not isinstance(payload, dict):
            raise ValidationError("request body must be a JSON object")
        batch_id = str(payload.get("batch_id", "")).strip()
        if not batch_id:
            raise ValidationError("batch_id is required")
        source = payload.get("source")
        if source not in FEED_SOURCES:
            raise ValidationError("source must be one of: " + ", ".join(FEED_SOURCES))
        raw_results = payload.get("results")
        if raw_results is None:
            raw_results = payload.get("windows")
        if not isinstance(raw_results, list) or not raw_results:
            raise ValidationError("results must be a non-empty list")
        results = []
        seen = set()
        for item in raw_results:
            if not isinstance(item, dict):
                raise ValidationError("each result entry must be an object")
            window_id = str(item.get("window_id", "")).strip()
            conclusion = item.get("conclusion")
            if not window_id:
                raise ValidationError("result window_id is required")
            if conclusion not in CONCLUSIONS:
                raise ValidationError("result conclusion must be 'success' or 'failure'")
            if window_id in seen:
                raise ValidationError("window %s appears more than once in batch %s"
                                      % (window_id, batch_id))
            seen.add(window_id)
            results.append({"window_id": window_id, "conclusion": conclusion})
        return {
            "batch_id": batch_id,
            "source": source,
            "results": results,
        }

    def _deny(self, actor, target_id, reason, action="result_batch_denied"):
        try:
            self.repository.append_audit(
                target_id, actor.user_id, actor.role,
                action, None, "rejected", {"reason": reason},
            )
        except Exception:
            pass
