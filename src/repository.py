import json
import sqlite3
from datetime import datetime, timezone
from uuid import uuid4

from .domain import ConflictError, NotFoundError


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class SQLiteRepository:
    def __init__(self, path):
        self.path = str(path)
        self._initialize()

    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self):
        with self._connect() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS entities (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    data TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_entities_kind_status
                    ON entities(kind, status);
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_id TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    actor_role TEXT NOT NULL,
                    action TEXT NOT NULL,
                    from_status TEXT,
                    to_status TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_audit_entity
                    ON audit_log(entity_id, id);
                CREATE TABLE IF NOT EXISTS idempotency (
                    actor_id TEXT NOT NULL,
                    idem_key TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(actor_id, idem_key)
                );
                CREATE TABLE IF NOT EXISTS result_batches (
                    id TEXT PRIMARY KEY,
                    batch_key TEXT NOT NULL UNIQUE,
                    source TEXT NOT NULL,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    total INTEGER NOT NULL,
                    applied INTEGER NOT NULL,
                    failed INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS batch_items (
                    id TEXT PRIMARY KEY,
                    batch_id TEXT NOT NULL,
                    window_key TEXT NOT NULL,
                    source TEXT NOT NULL,
                    conclusion TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    status TEXT NOT NULL,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(batch_id, window_key)
                );
                CREATE TABLE IF NOT EXISTS window_reconcile (
                    window_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    local_conclusion TEXT,
                    archive_conclusion TEXT,
                    first_source TEXT,
                    version INTEGER NOT NULL,
                    updated_at TEXT NOT NULL
                );
            """)

    @staticmethod
    def _entity_from_row(row):
        return {
            "id": row["id"],
            "kind": row["kind"],
            "status": row["status"],
            "version": int(row["version"]),
            "data": json.loads(row["data"]),
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def create_entity(self, entity_id, kind, status, data, actor_id):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                "VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
                (entity_id, kind, status, payload, actor_id, now, now),
            )
        return self.get_entity(entity_id)

    def get_entity(self, entity_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
        return self._entity_from_row(row) if row else None

    def list_entities(self, kind=None, status=None):
        clauses = []
        params = []
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM entities" + where + " ORDER BY created_at, id", params
            ).fetchall()
        return [self._entity_from_row(row) for row in rows]

    def find_entities(self, kind, field, value):
        return [
            entity
            for entity in self.list_entities(kind=kind)
            if (entity["id"] == value if field == "id" else entity["data"].get(field) == value)
        ]

    def update_entity(self, entity_id, expected_version, status, data):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT version FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
            if not row:
                raise NotFoundError("entity not found: " + entity_id)
            current_version = int(row["version"])
            if expected_version is not None and current_version != int(expected_version):
                raise ConflictError(
                    "version conflict: expected %s, found %s"
                    % (expected_version, current_version)
                )
            connection.execute(
                "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
                "WHERE id = ? AND version = ?",
                (status, payload, now, entity_id, current_version),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_entity(entity_id)

    def append_audit(self, entity_id, actor_id, actor_role, action, from_status, to_status, detail):
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, to_status, detail, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    entity_id,
                    actor_id,
                    actor_role,
                    action,
                    from_status,
                    to_status,
                    json.dumps(detail, ensure_ascii=False, sort_keys=True),
                    utcnow(),
                ),
            )

    def list_audit(self, entity_id=None):
        with self._connect() as connection:
            if entity_id:
                rows = connection.execute(
                    "SELECT * FROM audit_log WHERE entity_id = ? ORDER BY id", (entity_id,)
                ).fetchall()
            else:
                rows = connection.execute("SELECT * FROM audit_log ORDER BY id").fetchall()
        return [
            {
                "id": row["id"],
                "entity_id": row["entity_id"],
                "actor_id": row["actor_id"],
                "actor_role": row["actor_role"],
                "action": row["action"],
                "from_status": row["from_status"],
                "to_status": row["to_status"],
                "detail": json.loads(row["detail"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def get_idempotency(self, actor_id, idem_key):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT entity_id FROM idempotency WHERE actor_id = ? AND idem_key = ?",
                (actor_id, idem_key),
            ).fetchone()
        return row["entity_id"] if row else None

    def save_idempotency(self, actor_id, idem_key, entity_id):
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO idempotency(actor_id, idem_key, entity_id, created_at) "
                "VALUES (?, ?, ?, ?)",
                (actor_id, idem_key, entity_id, utcnow()),
            )

    def ping(self):
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return True

    # ------------------------------------------------------------------
    # 望远镜控制系统回传批次对账
    # ------------------------------------------------------------------

    @staticmethod
    def _batch_from_row(row):
        return {
            "id": row["id"],
            "batch_key": row["batch_key"],
            "source": row["source"],
            "status": row["status"],
            "version": int(row["version"]),
            "total": int(row["total"]),
            "applied": int(row["applied"]),
            "failed": int(row["failed"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    @staticmethod
    def _item_from_row(row):
        return {
            "id": row["id"],
            "batch_id": row["batch_id"],
            "window_id": row["window_key"],
            "source": row["source"],
            "conclusion": row["conclusion"],
            "payload": json.loads(row["payload"]),
            "status": row["status"],
            "error": row["error"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    @staticmethod
    def _reconcile_from_row(row):
        return {
            "window_id": row["window_id"],
            "status": row["status"],
            "local_conclusion": json.loads(row["local_conclusion"]) if row["local_conclusion"] else None,
            "archive_conclusion": json.loads(row["archive_conclusion"]) if row["archive_conclusion"] else None,
            "first_source": row["first_source"],
            "version": int(row["version"]),
            "updated_at": row["updated_at"],
        }

    def get_batch_by_key(self, batch_key):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM result_batches WHERE batch_key = ?", (batch_key,)
            ).fetchone()
        return self._batch_from_row(row) if row else None

    def list_batches(self):
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM result_batches ORDER BY id").fetchall()
        return [self._batch_from_row(row) for row in rows]

    def list_batch_items(self, batch_id):
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM batch_items WHERE batch_id = ? ORDER BY id", (batch_id,)
            ).fetchall()
        return [self._item_from_row(row) for row in rows]

    def canonical_batch_items(self, batch_id):
        return sorted(
            (item["window_id"], item["conclusion"], json.dumps(item["payload"], sort_keys=True))
            for item in self.list_batch_items(batch_id)
        )

    def create_batch(self, batch_id, batch_key, source, items):
        """Create a batch and its pending items.

        Concurrent submissions of the same batch_key are serialized by
        BEGIN IMMEDIATE and the UNIQUE constraint: exactly one insert
        wins; the loser re-reads and either replays idempotently or raises
        ConflictError on content mismatch.
        """
        now = utcnow()
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "INSERT INTO result_batches(id, batch_key, source, status, version, total, applied, failed, created_at, updated_at) "
                    "VALUES (?, ?, ?, 'processing', 1, ?, 0, 0, ?, ?)",
                    (batch_id, batch_key, source, len(items), now, now),
                )
                for item in items:
                    connection.execute(
                        "INSERT INTO batch_items(id, batch_id, window_key, source, conclusion, payload, status, created_at, updated_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)",
                        (
                            str(uuid4()),
                            batch_id,
                            item["window_id"],
                            source,
                            item["conclusion"],
                            json.dumps(item.get("payload") or {}, ensure_ascii=False, sort_keys=True),
                            now,
                            now,
                        ),
                    )
        except sqlite3.IntegrityError:
            existing = self.get_batch_by_key(batch_key)
            if existing and existing["source"] == source and self.canonical_batch_items(existing["id"]) == _canonical_items(items):
                return existing, False
            raise ConflictError("batch key already used with different content: " + batch_key)
        return self.get_batch_by_key(batch_key), True

    def update_batch_status(self, batch_id, status, applied, failed):
        with self._connect() as connection:
            connection.execute(
                "UPDATE result_batches SET status = ?, applied = ?, failed = ?, "
                "version = version + 1, updated_at = ? WHERE id = ?",
                (status, applied, failed, utcnow(), batch_id),
            )

    def get_window_reconcile(self, window_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM window_reconcile WHERE window_id = ?", (window_id,)
            ).fetchone()
        return self._reconcile_from_row(row) if row else None

    def list_window_reconcile(self, status=None):
        clauses = []
        params = []
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM window_reconcile" + where + " ORDER BY window_id", params
            ).fetchall()
        return [self._reconcile_from_row(row) for row in rows]

    def _audit_in_tx(self, connection, entity_id, actor, action, from_status, to_status, detail):
        connection.execute(
            "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, to_status, detail, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                entity_id,
                actor.user_id,
                actor.role,
                action,
                from_status,
                to_status,
                json.dumps(detail, ensure_ascii=False, sort_keys=True),
                utcnow(),
            ),
        )

    def _apply_outcome(self, connection, obs_row, conclusion, payload, window_id, actor, now):
        """Apply a confirmed conclusion to the observation in the caller's transaction.

        success -> observation completed with the returned result.
        failure -> release telescope/team slots (back to requested, clear
                   scheduled fields) and create a follow-up observation request.
        """
        obs = self._entity_from_row(obs_row)
        data = dict(obs["data"])
        if conclusion == "success":
            data["result"] = payload
            data["result_confirmed_at"] = now
            new_status = "completed"
            action = "complete"
        else:
            for field in ("telescope_id", "team_id", "start_at", "end_at"):
                data.pop(field, None)
            data["last_failure"] = payload
            data["schedule_released_at"] = now
            new_status = "requested"
            action = "release"
        cur = connection.execute(
            "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
            "WHERE id = ? AND version = ?",
            (new_status, json.dumps(data, ensure_ascii=False, sort_keys=True), now, window_id, obs["version"]),
        )
        if cur.rowcount != 1:
            raise ConflictError("observation was modified concurrently: " + window_id)
        self._audit_in_tx(
            connection,
            window_id,
            actor,
            action,
            obs["status"],
            new_status,
            {"conclusion": conclusion, "payload": payload},
        )
        if conclusion == "failure":
            follow_id = str(uuid4())
            follow_data = {
                "candidate_id": data.get("candidate_id"),
                "mode": data.get("mode"),
                "makeup_of": window_id,
                "reason": "补观测：窗口失败，释放望远镜与观测队时段后重新申请",
            }
            connection.execute(
                "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                "VALUES (?, 'observation', 'requested', 1, ?, ?, ?, ?)",
                (follow_id, json.dumps(follow_data, ensure_ascii=False, sort_keys=True), actor.user_id, now, now),
            )
            self._audit_in_tx(
                connection,
                follow_id,
                actor,
                "create",
                None,
                "requested",
                {"kind": "observation", "makeup_of": window_id},
            )
            # the follow-up observation awaits its own results
            connection.execute(
                "INSERT INTO window_reconcile(window_id, status, local_conclusion, archive_conclusion, first_source, version, updated_at) "
                "VALUES (?, 'pending_return', NULL, NULL, NULL, 1, ?)",
                (follow_id, now),
            )

    def apply_batch_item(self, item_id, actor):
        """Apply one batch item atomically.

        Returns (item, outcome) where outcome is 'applied', 'skipped'
        (already applied in an earlier delivery) or 'failed'. A failed
        item is rolled back to 'failed' in its own transaction so the
        batch can be retried with the same key without leaving a
        half-written state.
        """
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT * FROM batch_items WHERE id = ?", (item_id,)
                ).fetchone()
                if not row:
                    raise NotFoundError("batch item not found: " + item_id)
                item = self._item_from_row(row)
                if item["status"] == "applied":
                    connection.rollback()
                    return item, "skipped"
                window_id = item["window_id"]
                obs_row = connection.execute(
                    "SELECT * FROM entities WHERE id = ? AND kind = 'observation'",
                    (window_id,),
                ).fetchone()
                if not obs_row:
                    raise NotFoundError("window not found: " + window_id)
                now = utcnow()
                rec = connection.execute(
                    "SELECT * FROM window_reconcile WHERE window_id = ?", (window_id,)
                ).fetchone()
                local = None
                archive = None
                first_source = None
                status = "pending"
                if rec:
                    local = json.loads(rec["local_conclusion"]) if rec["local_conclusion"] else None
                    archive = json.loads(rec["archive_conclusion"]) if rec["archive_conclusion"] else None
                    first_source = rec["first_source"]
                    status = rec["status"]
                conclusion_obj = {
                    "conclusion": item["conclusion"],
                    "payload": item["payload"],
                    "batch_id": item["batch_id"],
                    "arrived_at": now,
                }
                if item["source"] == "local":
                    local = conclusion_obj
                else:
                    archive = conclusion_obj
                if first_source is None:
                    first_source = item["source"]
                if local and archive:
                    if local["conclusion"] == archive["conclusion"]:
                        status = "confirmed"
                        # 归档复核为事后权威结论，一致时以归档数据为准
                        self._apply_outcome(
                            connection,
                            obs_row,
                            archive["conclusion"],
                            archive["payload"] or local["payload"],
                            window_id,
                            actor,
                            now,
                        )
                    else:
                        # Conflicting conclusions: keep both, record arrival
                        # order, hold pending confirmation. No archive and no
                        # reschedule until a human confirms.
                        status = "pending_confirmation"
                else:
                    status = "pending"
                local_json = json.dumps(local, ensure_ascii=False, sort_keys=True) if local else None
                archive_json = json.dumps(archive, ensure_ascii=False, sort_keys=True) if archive else None
                if rec:
                    connection.execute(
                        "UPDATE window_reconcile SET status = ?, local_conclusion = ?, archive_conclusion = ?, "
                        "first_source = ?, version = version + 1, updated_at = ? WHERE window_id = ?",
                        (status, local_json, archive_json, first_source, now, window_id),
                    )
                else:
                    connection.execute(
                        "INSERT INTO window_reconcile(window_id, status, local_conclusion, archive_conclusion, first_source, version, updated_at) "
                        "VALUES (?, ?, ?, ?, ?, 1, ?)",
                        (window_id, status, local_json, archive_json, first_source, now),
                    )
                connection.execute(
                    "UPDATE batch_items SET status = 'applied', error = NULL, updated_at = ? WHERE id = ?",
                    (now, item_id),
                )
                self._audit_in_tx(
                    connection,
                    window_id,
                    actor,
                    "window_result",
                    None,
                    status,
                    {
                        "batch_id": item["batch_id"],
                        "source": item["source"],
                        "conclusion": item["conclusion"],
                        "first_source": first_source,
                    },
                )
            return item, "applied"
        except Exception as exc:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "UPDATE batch_items SET status = 'failed', error = ?, updated_at = ? "
                    "WHERE id = ? AND status != 'applied'",
                    (str(exc), utcnow(), item_id),
                )
                connection.commit()
            return None, "failed"

    def confirm_window(self, window_id, actor, conclusion, payload):
        """Apply a human-confirmed conclusion to a conflicted window."""
        now = utcnow()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rec = connection.execute(
                "SELECT * FROM window_reconcile WHERE window_id = ?", (window_id,)
            ).fetchone()
            if not rec:
                raise NotFoundError("window not found: " + window_id)
            if rec["status"] != "pending_confirmation":
                raise ConflictError("window is not pending confirmation: " + rec["status"])
            obs_row = connection.execute(
                "SELECT * FROM entities WHERE id = ? AND kind = 'observation'",
                (window_id,),
            ).fetchone()
            if not obs_row:
                raise NotFoundError("window not found: " + window_id)
            self._apply_outcome(connection, obs_row, conclusion, payload, window_id, actor, now)
            connection.execute(
                "UPDATE window_reconcile SET status = 'confirmed', version = version + 1, updated_at = ? "
                "WHERE window_id = ?",
                (now, window_id),
            )
            connection.commit()
        return self.get_window_reconcile(window_id)

    def upgrade_pending_returns(self):
        """Backfill observations that predate the result-tracking tables.

        Every observation without a reconcile row is marked pending_return;
        no historical result is assumed. Idempotent.
        """
        created = []
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            obs_ids = [
                row["id"]
                for row in connection.execute(
                    "SELECT id FROM entities WHERE kind = 'observation'"
                ).fetchall()
            ]
            existing = {
                row["window_id"]
                for row in connection.execute(
                    "SELECT window_id FROM window_reconcile"
                ).fetchall()
            }
            now = utcnow()
            for obs_id in obs_ids:
                if obs_id in existing:
                    continue
                connection.execute(
                    "INSERT INTO window_reconcile(window_id, status, local_conclusion, archive_conclusion, first_source, version, updated_at) "
                    "VALUES (?, 'pending_return', NULL, NULL, NULL, 1, ?)",
                    (obs_id, now),
                )
                created.append(obs_id)
            connection.commit()
        return created


def _canonical_items(items):
    return sorted(
        (
            item["window_id"],
            item["conclusion"],
            json.dumps(item.get("payload") or {}, sort_keys=True),
        )
        for item in items
    )
