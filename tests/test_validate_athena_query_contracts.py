from __future__ import annotations

import copy
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import validate_athena_query_contracts as contracts  # noqa: E402


class AthenaQueryContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.manifest = contracts.load_manifest(contracts.DEFAULT_MANIFEST)
        query_file = ROOT / cls.manifest["query_file"]
        cls.blocks = contracts.parse_query_blocks(query_file.read_text(encoding="utf-8"))

    def test_repository_contract_is_valid(self) -> None:
        self.assertEqual([], contracts.validate_contracts(self.manifest, self.blocks))

    def test_mutating_query_is_rejected(self) -> None:
        blocks = list(self.blocks)
        first = blocks[0]
        blocks[0] = contracts.QueryBlock(first.query_id, first.title, "DELETE FROM x;")
        errors = contracts.validate_contracts(self.manifest, blocks)
        self.assertTrue(any("mutating keyword is forbidden: DELETE" in error for error in errors))

    def test_manifest_table_drift_is_rejected(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["queries"][0]["tables"] = ["unexpected_table"]
        errors = contracts.validate_contracts(manifest, self.blocks)
        self.assertTrue(any("table mismatch" in error for error in errors))

    def test_nonsequential_manifest_ids_are_rejected(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["queries"][1]["id"] = 99
        errors = contracts.validate_contracts(manifest, self.blocks)
        self.assertTrue(any("Manifest query IDs must be sequential" in error for error in errors))


if __name__ == "__main__":
    unittest.main()
