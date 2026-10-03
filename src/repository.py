import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone

from .domain import ConflictError, NotFoundError


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class _Transaction:
    """Read/write handle bound to one BEGIN IMMEDIATE transaction."""

    def __init__(self, connection, now):
        self._connection = connection
        self.now = now

    def _entity(self, row):
        return SQLiteRepository._entity_from_row(row)

    def get(self, entity_id):
        row = self._connection.execute(
            "SELECT * FROM entities WHERE id = ?", (entity_id,)
        ).fetchone()
        return self._entity(row) if row else None

    def list_kind(self, kind):
        rows = self._connection.execute(
            "SELECT * FROM entities WHERE kind = ? ORDER BY created_at, id", (kind,)
        ).fetchall()
        return [self._entity(row) for row in rows]

    def insert(self, entity_id, kind, status, data, actor_id, version=1):
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        try:
            self._connection.execute(
                "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (entity_id, kind, status, version, payload, actor_id, self.now, self.now),
            )
        except sqlite3.IntegrityError as exc:
            raise ConflictError("entity already exists: " + entity_id) from exc
        entity = self.get(entity_id)
        if entity is None:
            raise NotFoundError("entity not found after insert: " + entity_id)
        return entity

    def update(self, entity, status, data):
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        current = self.get(entity["id"])
        if current is None:
            raise NotFoundError("entity not found: " + entity["id"])
        if current["version"] != entity["version"]:
            raise ConflictError(
                "version conflict: expected %s, found %s"
                % (entity["version"], current["version"])
            )
        self._connection.execute(
            "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
            "WHERE id = ? AND version = ?",
            (status, payload, self.now, entity["id"], entity["version"]),
        )
        return self.get(entity["id"])

    def audit(self, entity_id, actor_id, actor_role, action, from_status, to_status, detail):
        self._connection.execute(
            "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, to_status, detail, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                entity_id,
                actor_id,
                actor_role,
                action,
                from_status,
                to_status,
                json.dumps(detail or {}, ensure_ascii=False, sort_keys=True),
                self.now,
            ),
        )


class SQLiteRepository:
    def __init__(self, path):
        self.path = str(path)
        self._initialize()

    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    @contextmanager
    def _auto_connection(self):
        """Commit/rollback and always close the connection.

        sqlite3's own context manager commits but never closes the connection,
        which leaked write locks under load; this one closes it.
        """
        connection = self._connect()
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def _initialize(self):
        with self._auto_connection() as connection:
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
                CREATE TABLE IF NOT EXISTS schema_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
            """)
        self._migrate()

    def _migrate(self):
        """Bring databases created before result reconciliation up to date.

        Historical scheduled observations have no return records on file, so
        they must not be treated as if they already had results: they become
        ``awaiting_result`` instead. Only a database created by an older
        version is migrated; a fresh database starts at the current version.
        """
        with self._auto_connection() as connection:
            row = connection.execute(
                "SELECT value FROM schema_meta WHERE key = 'schema_version'"
            ).fetchone()
            if row is not None:
                return
            existing = connection.execute("SELECT COUNT(*) AS n FROM entities").fetchone()
            if existing["n"] == 0:
                connection.execute(
                    "INSERT INTO schema_meta(key, value) VALUES ('schema_version', '2')"
                )
                return
            rows = connection.execute(
                "SELECT id FROM entities WHERE kind = 'observation' AND status = 'scheduled'"
            ).fetchall()
            now = utcnow()
            for observation_row in rows:
                connection.execute(
                    "UPDATE entities SET status = 'awaiting_result', updated_at = ? WHERE id = ?",
                    (now, observation_row["id"]),
                )
                connection.execute(
                    "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, "
                    "from_status, to_status, detail, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        observation_row["id"],
                        "system",
                        "system",
                        "migration_awaiting_result",
                        "scheduled",
                        "awaiting_result",
                        json.dumps(
                            {"reason": "historical window without returned results"},
                            ensure_ascii=False,
                            sort_keys=True,
                        ),
                        now,
                    ),
                )
            connection.execute(
                "INSERT INTO schema_meta(key, value) VALUES ('schema_version', '2')"
            )

    def run_in_transaction(self, callback):
        """Run callback with a single BEGIN IMMEDIATE transaction.

        The callback receives a _Transaction handle. Any exception rolls the
        whole transaction back, so a window can never be left half written.
        A CommitAndRaise control-flow exception instead commits the audit rows
        written inside and re-raises its carried cause.
        """
        from .domain import CommitAndRaise

        connection = self._connect()
        now = utcnow()
        try:
            connection.execute("BEGIN IMMEDIATE")
            try:
                result = callback(_Transaction(connection, now))
            except CommitAndRaise as control:
                connection.commit()
                raise control.cause
            connection.commit()
            return result
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

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
        try:
            with self._auto_connection() as connection:
                connection.execute(
                    "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                    "VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
                    (entity_id, kind, status, payload, actor_id, now, now),
                )
        except sqlite3.IntegrityError as exc:
            raise ConflictError("entity already exists: " + entity_id) from exc
        return self.get_entity(entity_id)

    def get_entity(self, entity_id):
        with self._auto_connection() as connection:
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
        with self._auto_connection() as connection:
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
        with self._auto_connection() as connection:
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
        with self._auto_connection() as connection:
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
        with self._auto_connection() as connection:
            row = connection.execute(
                "SELECT entity_id FROM idempotency WHERE actor_id = ? AND idem_key = ?",
                (actor_id, idem_key),
            ).fetchone()
        return row["entity_id"] if row else None

    def save_idempotency(self, actor_id, idem_key, entity_id):
        with self._auto_connection() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO idempotency(actor_id, idem_key, entity_id, created_at) "
                "VALUES (?, ?, ?, ?)",
                (actor_id, idem_key, entity_id, utcnow()),
            )

    def ping(self):
        with self._auto_connection() as connection:
            connection.execute("SELECT 1").fetchone()
        return True
