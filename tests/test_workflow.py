import tempfile
import unittest
from pathlib import Path

from src.domain import Actor
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class WorkflowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(
            SQLiteRepository(Path(self.tmp.name) / "workflow.db"),
            RuleEngine(),
        )

    def tearDown(self):
        self.tmp.cleanup()

    def test_candidate_to_completed_observation(self):
        analyst = Actor("analyst-1", "analyst")
        coordinator = Actor("coordinator-1", "coordinator")
        source = self.service.create(
            analyst,
            "source",
            {"name": "Survey Alpha", "survey_name": "ZTF-like"},
        )
        candidate = self.service.create(
            analyst,
            "candidate",
            {
                "source_id": source["id"],
                "event_id": "AT-2026-001",
                "ra": 120.5,
                "dec": -12.25,
                "magnitude": 17.2,
                "transient_type": "supernova",
                "observed_at": "2026-09-27T01:00:00Z",
            },
        )
        candidate = self.service.transition(
            analyst,
            candidate["id"],
            "merge_measurement",
            {
                "measurement": {
                    "observed_at": "2026-09-27T02:00:00Z",
                    "ra": 120.5002,
                    "dec": -12.2498,
                    "magnitude": 16.8,
                }
            },
        )
        self.assertEqual(candidate["data"]["merged_measurement_count"], 2)
        self.assertGreater(candidate["data"]["priority_score"], 30)

        telescope = self.service.create(
            coordinator,
            "telescope",
            {"name": "North 2m", "aperture_m": 2.0, "site_name": "NAO"},
        )
        observation = self.service.create(
            coordinator,
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
            coordinator, observation["id"], "schedule", {"operator_id": "op-1"}
        )
        observation = self.service.transition(
            Actor("operator-1", "operator"), observation["id"], "complete", {"quality": "good"}
        )
        self.assertEqual(observation["status"], "completed")
        self.assertTrue(self.service.audit_log(observation["id"]))


if __name__ == "__main__":
    unittest.main()
