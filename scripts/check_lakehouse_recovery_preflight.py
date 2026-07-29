#!/usr/bin/env python3
"""Build a local-only Lakehouse recovery-readiness preflight report."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from validate_athena_query_contracts import (
    load_manifest as load_query_manifest,
    parse_query_blocks,
    validate_contracts as validate_query_contracts,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SPEC = REPO_ROOT / "recovery" / "lakehouse-recovery-preflight.json"


def load_spec(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError("preflight specification must be a JSON object")
    return value


def validate_spec(spec: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if spec.get("version") != 1:
        errors.append("version must be 1")
    if not isinstance(spec.get("name"), str) or not spec["name"].strip():
        errors.append("name must be a non-empty string")
    if spec.get("claim_boundary") != "local-repository-preflight-only":
        errors.append("claim_boundary must be local-repository-preflight-only")
    if spec.get("query_contract_check") is not True:
        errors.append("query_contract_check must be true")

    objectives = spec.get("business_objectives")
    if not isinstance(objectives, dict):
        errors.append("business_objectives must be an object")
    else:
        for objective in ("rto", "rpo"):
            if objectives.get(objective) != "not-set":
                errors.append(f"business_objectives.{objective} must remain not-set")

    artifacts = spec.get("required_artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        errors.append("required_artifacts must be a non-empty array")
        return errors

    seen: set[str] = set()
    for index, artifact in enumerate(artifacts):
        label = f"required_artifacts[{index}]"
        if not isinstance(artifact, dict):
            errors.append(f"{label} must be an object")
            continue
        path = artifact.get("path")
        if not isinstance(path, str) or not path.strip():
            errors.append(f"{label}.path must be a non-empty string")
        else:
            candidate = Path(path)
            if candidate.is_absolute() or ".." in candidate.parts:
                errors.append(f"{label}.path must stay inside the repository")
            if path in seen:
                errors.append(f"duplicate artifact path: {path}")
            seen.add(path)
        for key in ("category", "recovery_role"):
            if not isinstance(artifact.get(key), str) or not artifact[key].strip():
                errors.append(f"{label}.{key} must be a non-empty string")
    return errors


def inspect_artifacts(
    spec: dict[str, Any], repo_root: Path
) -> tuple[list[dict[str, Any]], list[str]]:
    inspected: list[dict[str, Any]] = []
    errors: list[str] = []
    root = repo_root.resolve()

    for artifact in spec.get("required_artifacts", []):
        if not isinstance(artifact, dict) or not isinstance(artifact.get("path"), str):
            continue
        relative_path = artifact["path"]
        candidate = (root / relative_path).resolve()
        try:
            candidate.relative_to(root)
        except ValueError:
            errors.append(f"artifact escapes repository: {relative_path}")
            continue
        if not candidate.is_file():
            errors.append(f"required artifact is missing: {relative_path}")
            continue
        content = candidate.read_bytes()
        if not content:
            errors.append(f"required artifact is empty: {relative_path}")
            continue
        inspected.append(
            {
                "category": artifact.get("category"),
                "path": relative_path,
                "recovery_role": artifact.get("recovery_role"),
                "sha256": hashlib.sha256(content).hexdigest(),
                "size_bytes": len(content),
            }
        )
    return inspected, errors


def git_state(repo_root: Path) -> tuple[dict[str, Any], list[str]]:
    errors: list[str] = []

    def run(*args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["git", "-C", str(repo_root), *args],
            check=False,
            capture_output=True,
            text=True,
        )

    revision = run("rev-parse", "HEAD")
    status = run("status", "--porcelain")
    if revision.returncode != 0:
        errors.append("could not resolve the repository HEAD revision")
    if status.returncode != 0:
        errors.append("could not inspect the repository working tree")

    dirty_entries = [line for line in status.stdout.splitlines() if line]
    return (
        {
            "head_revision": revision.stdout.strip() or None,
            "working_tree_clean": not dirty_entries,
            "working_tree_change_count": len(dirty_entries),
        },
        errors,
    )


def query_contract_state(repo_root: Path) -> list[str]:
    """Run the existing static Athena validator against repository-local inputs."""
    try:
        manifest = load_query_manifest(repo_root / "athena" / "query-contracts.json")
        query_file = repo_root / str(manifest.get("query_file", ""))
        blocks = parse_query_blocks(query_file.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return [str(exc)]
    return validate_query_contracts(manifest, blocks)


def build_report(
    spec: dict[str, Any],
    repo_root: Path,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    errors = validate_spec(spec)
    artifacts, artifact_errors = inspect_artifacts(spec, repo_root)
    errors.extend(artifact_errors)

    query_errors: list[str] = []
    query_check_status = "not-run"
    if spec.get("query_contract_check") is True:
        query_errors = query_contract_state(repo_root)
        errors.extend(f"query contract: {error}" for error in query_errors)
        query_check_status = "fail" if query_errors else "pass"

    git, git_errors = git_state(repo_root)
    errors.extend(git_errors)
    warnings: list[str] = []
    if not git["working_tree_clean"]:
        warnings.append(
            "working tree differs from HEAD; the report hashes current files, "
            "not an approved recovery baseline"
        )

    status = "fail" if errors else "warning" if warnings else "pass"
    return {
        "schema_version": 1,
        "generated_at_utc": generated_at_utc
        or datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "status": status,
        "preflight_name": spec.get("name"),
        "claim_boundary": spec.get("claim_boundary"),
        "business_objectives": spec.get("business_objectives"),
        "repository": {
            "artifact_count": len(artifacts),
            "git": git,
        },
        "checks": {
            "artifact_inventory": "fail" if artifact_errors else "pass",
            "athena_query_contracts": query_check_status,
        },
        "artifacts": artifacts,
        "warnings": warnings,
        "errors": errors,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC)
    parser.add_argument(
        "--output",
        type=Path,
        help="write the JSON report to this existing directory",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        spec = load_spec(args.spec)
        report = build_report(spec, REPO_ROOT)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"Recovery preflight could not run: {exc}", file=sys.stderr)
        return 1

    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        if not args.output.parent.is_dir():
            print(
                f"Recovery preflight output directory does not exist: {args.output.parent}",
                file=sys.stderr,
            )
            return 1
        args.output.write_text(payload, encoding="utf-8")
        print(
            f"Recovery preflight: {report['status']} "
            f"({report['repository']['artifact_count']} artifacts); "
            f"report={args.output}"
        )
    else:
        print(payload, end="")
    return 1 if report["status"] == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
