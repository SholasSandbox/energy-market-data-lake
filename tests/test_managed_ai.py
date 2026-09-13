from __future__ import annotations

import copy
import json
import unittest
from pathlib import Path

from energy_market.ai_orchestration import validate_payload
from energy_market.managed_ai import normalize_ai_insight_reference_objects
from energy_market.news_ai import build_ai_insight


ROOT = Path(__file__).resolve().parents[1]


class ManagedAIReferenceNormalizationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.bundle = json.loads(
            (ROOT / "docs/evidence/ai/ai_input_bundle_v1.sample.json").read_text(
                encoding="utf-8",
            ),
        )

    def test_missing_reference_is_restored_from_matching_input_record(self) -> None:
        payload = build_ai_insight(self.bundle)
        raw_reference = payload["insights"][0]["energy_references"][0]
        expected_reference = raw_reference.pop("reference")
        raw_reference["value"] = "105.17 GBP/MWh"

        normalized = normalize_ai_insight_reference_objects(
            payload,
            bundle=self.bundle,
        )

        self.assertNotIn("reference", raw_reference)
        self.assertIn("value", raw_reference)
        normalized_reference = normalized["insights"][0]["energy_references"][0]
        self.assertEqual(normalized_reference["reference"], expected_reference)
        self.assertNotIn("value", normalized_reference)
        self.assertEqual(validate_payload(normalized, "ai_insight"), [])

    def test_missing_reference_remains_invalid_when_provenance_is_unsafe(self) -> None:
        scenarios = {
            "source mismatch": lambda bundle, reference: reference.update(
                {"source": "unmatched-source"},
            ),
            "unknown metric": lambda bundle, reference: reference.update(
                {"metric": "unknown_metric"},
            ),
            "multiple records": lambda bundle, reference: bundle["energy_input"][
                "records"
            ].append(copy.deepcopy(bundle["energy_input"]["records"][0])),
        }

        for name, mutate in scenarios.items():
            with self.subTest(name=name):
                bundle = copy.deepcopy(self.bundle)
                payload = build_ai_insight(bundle)
                raw_reference = payload["insights"][0]["energy_references"][0]
                raw_reference.pop("reference")
                mutate(bundle, raw_reference)

                normalized = normalize_ai_insight_reference_objects(
                    payload,
                    bundle=bundle,
                )

                normalized_reference = normalized["insights"][0][
                    "energy_references"
                ][0]
                self.assertNotIn("reference", normalized_reference)
                self.assertTrue(
                    any(
                        "'reference' is a required property" in error
                        for error in validate_payload(normalized, "ai_insight")
                    ),
                )


if __name__ == "__main__":
    unittest.main()
