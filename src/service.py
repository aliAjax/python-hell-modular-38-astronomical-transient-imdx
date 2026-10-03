from uuid import uuid4

from .audit import AuditTrail
from .domain import ConflictError, NotFoundError, PermissionDenied, ValidationError
from .repository import _canonical_items
from .rules import RuleEngine


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    def create(self, actor, kind, data, idempotency_key=None):
        kind = self.rules.normalize_kind(kind)
        payload = dict(data or {})
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                entity = self.repository.get_entity(existing)
                if entity:
                    return entity
        validated = self.rules.validate_create(actor, kind, payload, self._lookup)
        if validated:
            payload.update(validated)
        entity_id = str(payload.pop("id", "") or uuid4())
        if self.repository.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        status = self.rules.initial_status(kind)
        entity = self.repository.create_entity(entity_id, kind, status, payload, actor.user_id)
        self.audit.record(entity_id, actor, "create", None, status, {"kind": kind})
        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, entity_id)
        return entity

    def transition(self, actor, entity_id, action, data=None, expected_version=None):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        expected = int(expected_version) if expected_version is not None else entity["version"]
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, dict(data or {}), self._lookup
        )
        merged = dict(entity["data"])
        merged.update(patch)
        updated = self.repository.update_entity(entity_id, expected, next_status, merged)
        self.audit.record(
            entity_id,
            actor,
            action,
            entity["status"],
            updated["status"],
            {"patch": patch},
        )
        return updated

    def get(self, entity_id):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        return entity

    def list(self, kind=None, status=None):
        if kind:
            kind = self.rules.normalize_kind(kind)
        return self.repository.list_entities(kind=kind, status=status)

    def audit_log(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)

    # ------------------------------------------------------------------
    # 望远镜控制系统回传批次对账
    # ------------------------------------------------------------------

    def _audit_reject(self, entity_id, actor, action, reason, expected=None, found=None):
        self.audit.record(
            entity_id or "unknown",
            actor,
            action,
            None,
            "rejected",
            {"reason": reason, "expected": expected, "found": found},
        )

    def submit_result_batch(self, actor, batch_key, source, items, expected_version=None):
        """Submit a telescope control result batch.

        Duplicate deliveries (same batch_key) are idempotent: windows
        already posted are kept and the remaining ones are retried as the
        same batch. Concurrent submissions of one batch_key are
        serialized at the database level; only one insert wins. Stale
        versions and permission failures are rejected and audited.
        """
        if not isinstance(batch_key, str) or not batch_key.strip():
            raise ValidationError("batch_key is required")
        try:
            self.rules.validate_batch_submit(actor, source, items)
        except PermissionDenied:
            self._audit_reject(batch_key, actor, "submit_batch", "permission_denied")
            raise
        existing = self.repository.get_batch_by_key(batch_key)
        if existing is not None:
            if expected_version is not None:
                if existing["version"] != int(expected_version):
                    self._audit_reject(
                        batch_key,
                        actor,
                        "submit_batch",
                        "version_conflict",
                        expected_version,
                        existing["version"],
                    )
                    raise ConflictError(
                        "batch version conflict: expected %s, found %s"
                        % (expected_version, existing["version"])
                    )
            if (
                existing["source"] != source
                or self.repository.canonical_batch_items(existing["id"]) != _canonical_items(items)
            ):
                self._audit_reject(batch_key, actor, "submit_batch", "batch_key_conflict")
                raise ConflictError("batch key already used with different content: " + batch_key)
            batch, created = existing, False
        else:
            if expected_version is not None:
                # Client expected a batch that does not exist: stale version.
                self._audit_reject(batch_key, actor, "submit_batch", "version_conflict", expected_version, None)
                raise ConflictError("batch does not exist: " + batch_key)
            try:
                batch, created = self.repository.create_batch(str(uuid4()), batch_key, source, items)
            except ConflictError:
                self._audit_reject(batch_key, actor, "submit_batch", "batch_key_conflict")
                raise
        applied = 0
        failed = 0
        for item in self.repository.list_batch_items(batch["id"]):
            if item["status"] == "applied":
                applied += 1
                continue
            _, outcome = self.repository.apply_batch_item(item["id"], actor)
            if outcome == "applied":
                applied += 1
            else:
                failed += 1
        if failed == 0:
            status = "applied"
        elif applied == 0:
            status = "failed"
        else:
            status = "partial"
        self.repository.update_batch_status(batch["id"], status, applied, failed)
        self.audit.record(
            batch_key,
            actor,
            "submit_batch",
            "received" if created else "replayed",
            status,
            {
                "batch_key": batch_key,
                "source": source,
                "created": created,
                "applied": applied,
                "failed": failed,
            },
        )
        return self.repository.get_batch_by_key(batch_key), created

    def confirm_window(self, actor, window_id, decision, expected_version=None):
        """Confirm a window whose local and archive conclusions conflict."""
        self.rules.validate_confirm(actor, decision)
        rec = self.repository.get_window_reconcile(window_id)
        if not rec:
            raise NotFoundError("window not found: " + window_id)
        if rec["status"] != "pending_confirmation":
            raise ConflictError("window is not pending confirmation: " + rec["status"])
        if expected_version is not None and rec["version"] != int(expected_version):
            self._audit_reject(
                window_id,
                actor,
                "confirm_window",
                "version_conflict",
                expected_version,
                rec["version"],
            )
            raise ConflictError(
                "window version conflict: expected %s, found %s"
                % (expected_version, rec["version"])
            )
        if decision in ("local", "archive"):
            chosen = rec[decision + "_conclusion"]
            if not chosen:
                raise ValidationError("no %s conclusion recorded for window %s" % (decision, window_id))
            conclusion = chosen["conclusion"]
            payload = chosen.get("payload") or {}
        else:
            conclusion = decision["conclusion"]
            payload = decision.get("payload") or {}
        self.repository.confirm_window(window_id, actor, conclusion, payload)
        self.audit.record(
            window_id,
            actor,
            "confirm_window",
            "pending_confirmation",
            "confirmed",
            {"decision": decision if isinstance(decision, str) else conclusion},
        )
        return self.repository.get_window_reconcile(window_id)

    def list_batches(self):
        return self.repository.list_batches()

    def get_batch(self, batch_key):
        batch = self.repository.get_batch_by_key(batch_key)
        if not batch:
            raise NotFoundError("batch not found: " + batch_key)
        batch["items"] = self.repository.list_batch_items(batch["id"])
        return batch

    def get_window(self, window_id):
        rec = self.repository.get_window_reconcile(window_id)
        if not rec:
            raise NotFoundError("window not found: " + window_id)
        return rec

    def list_windows(self, status=None):
        return self.repository.list_window_reconcile(status=status)

    def upgrade_pending_returns(self, actor):
        """Backfill historical observations to pending_return (idempotent)."""
        self.rules.validate_upgrade(actor)
        created = self.repository.upgrade_pending_returns()
        self.audit.record(
            "upgrade",
            actor,
            "upgrade",
            None,
            "pending_return",
            {"pending_returns_created": len(created), "window_ids": created},
        )
        return {"pending_returns_created": len(created), "window_ids": created}
