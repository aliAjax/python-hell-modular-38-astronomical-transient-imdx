import unittest

from src.domain import ConflictError, ValidationError
from src.rules import RuleEngine, calculate_priority, measurements_overlap


class RulesTest(unittest.TestCase):
    def test_priority_prefers_bright_and_high_value_events(self):
        self.assertGreater(calculate_priority(16.0, "grb"), calculate_priority(19.0, "variable"))
        with self.assertRaises(ValidationError):
            calculate_priority("not-a-number", "unknown")

    def test_overlap_is_half_open(self):
        self.assertTrue(measurements_overlap("10", "12", "11", "13"))
        self.assertFalse(measurements_overlap("10", "11", "11", "12"))

    def test_duplicate_event_and_measurement_are_rejected(self):
        rules = RuleEngine()
        candidate = {
            "id": "candidate-1",
            "kind": "candidate",
            "status": "detected",
            "data": {"measurements": [{"observed_at": "2026-09-27T01:00:00Z"}]},
        }
        with self.assertRaises(ConflictError):
            rules.validate_transition(
                type("Actor", (), {"role": "analyst", "user_id": "a"})(),
                candidate,
                "merge_measurement",
                {
                    "measurement": {
                        "observed_at": "2026-09-27T01:00:00Z",
                        "ra": 1,
                        "dec": 2,
                        "magnitude": 18,
                    }
                },
            )


if __name__ == "__main__":
    unittest.main()
