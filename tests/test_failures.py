import tempfile
import unittest
from pathlib import Path

from src.domain import Actor, ConflictError, PermissionDenied
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class FailureTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(
            SQLiteRepository(Path(self.tmp.name) / "failures.db"),
            RuleEngine(),
        )
        self.analyst = Actor("analyst-1", "analyst")

    def tearDown(self):
        self.tmp.cleanup()

    def _candidate(self, event_id="AT-1", key=None):
        source = self.service.create(
            self.analyst,
            "source",
            {"name": "Survey", "survey_name": "S"},
        )
        return self.service.create(
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
            idempotency_key=key,
        )

    def test_duplicate_event_key_is_conflict(self):
        first = self._candidate("same")
        source_id = first["data"]["source_id"]
        with self.assertRaises(ConflictError):
            self.service.create(
                self.analyst,
                "candidate",
                {
                    "source_id": source_id,
                    "event_id": "same",
                    "ra": 11,
                    "dec": 21,
                    "magnitude": 17,
                    "transient_type": "unknown",
                    "observed_at": "2026-09-27T00:01:00Z",
                },
            )

    def test_permission_and_version_conflicts(self):
        candidate = self._candidate("permission")
        with self.assertRaises(PermissionDenied):
            self.service.transition(
                Actor("viewer", "viewer"),
                candidate["id"],
                "triage",
                {"reason": "review"},
            )
        with self.assertRaises(ConflictError):
            self.service.transition(
                self.analyst,
                candidate["id"],
                "triage",
                {"reason": "review"},
                expected_version=999,
            )

    def test_idempotent_candidate_creation(self):
        first = self._candidate("idem", "same-key")
        # The same source is selected again because the helper creates a new source for a fresh key.
        second = self._candidate("idem-2", "same-key")
        self.assertEqual(first["id"], second["id"])


if __name__ == "__main__":
    unittest.main()
