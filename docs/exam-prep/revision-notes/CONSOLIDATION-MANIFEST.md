<!-- markdownlint-disable MD013 MD060 -->

# SAP-C02 Revision-Note Consolidation Manifest

**Consolidated:** 2026-08-07
**Destination:** `docs/exam-prep/revision-notes/`

## Moved Canonical Material

| Previous location | New location | Disposition |
|---|---|---|
| Standalone `cert-revision/sap-c02-revision-notes-v2` directory | `revision-notes/core/` | Seventeen numbered chapters, pack README, transformation log, and source manifest moved; former source directory removed after it became empty |
| Separate `aws-sap-c02-governance` repository | `revision-notes/governance/` | Five `SAP-C02_*.md` revision and mental-model notes moved; exercises, repository instructions, assessment material, and runbooks left in place |
| Downloads `sap-c02-domain-3-deep-dive.md` | `revision-notes/domain-deep-dives/` | Canonical Domain 3 guide moved |

## Removed Redundant Copies

Removal was recoverable: both files were moved to the user's Trash rather than
permanently deleted.

| Redundant file | Reason | Recoverable Trash name |
|---|---|---|
| `cert-revision/sap-c02-revision-notes/` | Superseded v1 pack organized around compressed domain summaries; the v2 pack explicitly rebuilt and expanded this material around service scenarios | `sap-c02-revision-notes-v1-superseded-20260807/` |
| `sap-c02-revision-pack-v2.md` | Synchronized concatenation of the seventeen canonical numbered chapters and their metadata; maintaining it created a second editable copy | `sap-c02-revision-pack-v2-duplicate-20260807.md` |
| Downloads `SAP-C02_Mental_Model_Reference_Diagrams.md` | Superseded by the later corrected governance-repository version | `SAP-C02_Mental_Model_Reference_Diagrams-superseded-20260807.md` |

## Deliberate Exclusions

The following were not moved because they are not revision-note duplicates:

- `docs/planning/sap-c02-readiness-tracker.md` and other planning controllers;
- mock examinations, answer sheets, frozen submissions, assessment reviews,
  and wrong-answer logs;
- Lakehouse ADRs, diagrams, evidence, implementation, and operational runbooks;
- governance-repository exercises, CLI runbooks, and repository instructions;
- Python/serverless tutorial lessons and implementation evidence; and
- stale or working tracker copies in Downloads.

## Evidence Boundary

Consolidation improves discoverability and removes competing revision copies.
It does not create new learner-recall evidence, alter any mock score, or prove
live AWS or Lakehouse implementation.
