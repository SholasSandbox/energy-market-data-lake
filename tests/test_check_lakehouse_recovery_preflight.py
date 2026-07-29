import hashlib
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from check_lakehouse_recovery_preflight import (  # noqa: E402
    build_report,
    inspect_artifacts,
    load_spec,
    validate_spec,
)


class RecoveryPreflightTests(unittest.TestCase):
    def test_repository_preflight_passes_or_warns_only_for_git_state(self):
        spec = load_spec(REPO_ROOT / "recovery" / "lakehouse-recovery-preflight.json")

        report = build_report(spec, REPO_ROOT, "2026-07-24T12:00:00+00:00")

        self.assertIn(report["status"], {"pass", "warning"})
        self.assertEqual(report["checks"]["artifact_inventory"], "pass")
        self.assertEqual(report["checks"]["athena_query_contracts"], "pass")
        self.assertEqual(report["repository"]["artifact_count"], 20)
        self.assertEqual(report["errors"], [])

    def test_artifact_inventory_records_size_and_sha256(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            artifact = root / "example.txt"
            artifact.write_text("recovery input\n", encoding="utf-8")
            spec = {
                "required_artifacts": [
                    {
                        "path": "example.txt",
                        "category": "test",
                        "recovery_role": "test input",
                    }
                ]
            }

            inspected, errors = inspect_artifacts(spec, root)

            self.assertEqual(errors, [])
            self.assertEqual(inspected[0]["size_bytes"], 15)
            self.assertEqual(
                inspected[0]["sha256"],
                hashlib.sha256(b"recovery input\n").hexdigest(),
            )

    def test_missing_artifact_fails_inventory(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            spec = {
                "required_artifacts": [
                    {
                        "path": "missing.txt",
                        "category": "test",
                        "recovery_role": "missing input",
                    }
                ]
            }

            inspected, errors = inspect_artifacts(spec, Path(temporary_directory))

            self.assertEqual(inspected, [])
            self.assertEqual(errors, ["required artifact is missing: missing.txt"])

    def test_spec_rejects_path_traversal_duplicate_and_set_objectives(self):
        artifact = {
            "path": "../outside.txt",
            "category": "test",
            "recovery_role": "unsafe input",
        }
        spec = {
            "version": 1,
            "claim_boundary": "local-repository-preflight-only",
            "business_objectives": {"rto": "4 hours", "rpo": "not-set"},
            "required_artifacts": [artifact, artifact.copy()],
        }

        errors = validate_spec(spec)

        self.assertIn("business_objectives.rto must remain not-set", errors)
        self.assertIn(
            "required_artifacts[0].path must stay inside the repository", errors
        )
        self.assertIn("duplicate artifact path: ../outside.txt", errors)


if __name__ == "__main__":
    unittest.main()
