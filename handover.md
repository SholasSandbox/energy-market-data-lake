# Energy Market Data Lakehouse Handover

**Prepared:** 2026-08-09<br>
**Controlling plan:** `docs/planning/sap-c02-readiness-tracker.md`<br>
**Transition:** Cross-domain revision-note calibration complete; Full Mock 006 remains next<br>
**AWS changes:** None<br>
**Publication status:** Relevant SAP-C02 documentation committed and published to `origin/main` on 2026-08-09; unrelated Mermaid editor instructions excluded

## Objective

Consolidate the canonical SAP-C02 revision notes under
`docs/exam-prep/revision-notes/`, calibrate their reusable decision rules to
the current official SAP-C02 guide and representative AWS Skill Builder
question depth, and preserve the protected two-full-mock cadence.

Read `AGENTS.md`, this handover, and the controlling tracker before making
further changes.

## Current State

### SAP-C02 readiness

- Full Mocks 001-005 scored 73/75, 71/75, 75/75, 70/75, and 73/75.
- Mock 005 scored 73/75 in 108 minutes: 46/48 single-response, 27/27
  exact-match multiple-response, every domain above 93%, and 72 minutes
  remaining.
- Mock 005 left two narrow misses: ARC single-response over-selection and
  Transfer Family AS2 service selection.
- AWS Skill Builder official-practice attempt 2 passed at scaled score 775
  against a 750 threshold: 45 questions were keyed correct, 30 incorrect and
  none skipped. The recorded 12h29 duration makes it calibration evidence, not
  timed booking-gate evidence; the export did not contain the four domain-score
  values.
- The quantitative gate is strongly exceeded, but the learner's evidence gate
  now prohibits a booking decision before Full Mock 008. The one-mock extension
  provides a third independent transfer opportunity for the Skill Builder
  remediation without changing the September exam window.
- Full Mock 006 is the next independent broad check. The two-mock cadence stays
  protected; revision-note maintenance must not displace it.

### Revision-note library

The canonical revision and mental-model notes now live under:

```text
docs/exam-prep/revision-notes/
  core/
  governance/
  domain-deep-dives/
  targeted-lessons/
```

Trackers, mock examinations, frozen submissions, reviews, and wrong-answer
evidence remain in their role-specific locations. No byte-for-byte duplicate
Markdown file exists inside the revision-note library.

## Completed Material Changes

- Moved the five targeted Networking, Route 53, Resilience/DR,
  non-relational-database, and migration lessons from the exam-prep root into
  `revision-notes/targeted-lessons/`; updated local references without moving
  assessment or planning artifacts.
- Added an exam-depth method based on direction, mode, enforcement boundary,
  and decisive operating qualifier.
- Added reusable CloudFront-only origin controls, device-specific edge-content
  routing, low-change three-tier replatforming, S3 bulk encryption and Macie,
  Session Manager access boundaries, cluster placement/ENA/EFA selection, and
  organization-scale security service boundaries.
- Added recognition and elimination depth for less-common services currently
  listed in the official SAP-C02 scope, while retaining deeper treatment for
  recurrent, high-weight architecture patterns.
- Implemented all five previously parked revision-maintenance reminders:
  public subnet versus public IPv4, Direct Connect VIF/gateway paths,
  CloudFront origin-failover method limits, Resolver endpoint minimums, and
  DynamoDB MREC/MRSC boundaries.
- Corrected stale material: Object Lock can be enabled on an existing
  versioned general-purpose S3 bucket; Aurora failover wording now matches the
  current documented typical value; Amazon Data Firehose uses its current
  name; retired QLDB and discontinued CloudWatch Evidently are not active
  service-selection cues.
- Added a second calibration set covering Object Lock compliance versus
  governance and existing-version handling, Intelligent-Tiering archive
  restore ceilings, organization-wide SCP recovery, current KMS-versus-CloudHSM
  Level 3 selection, and complete tightly coupled HPC bundles.
- Added a third representative Skill Builder calibration set covering shared
  multi-AZ files, cost-aware Spot placement, Access Analyzer trust zones,
  Cognito custom/guest identity transitions, low-change broker migration, S3
  lifecycle selection, scaling-policy signals, OU inheritance, AWS Batch,
  Connect/Lex/Lambda composition, TGW segmentation, DAX cost patterns, Global
  Accelerator static IPs, and Secrets Manager rotation strategies.
- Added a fourth representative Skill Builder calibration set covering ALB
  request-count scaling, serverless Oracle order processing, AWS Transform MGN,
  lifecycle cost sequences, durable HTTP-to-SQS acceptance, online-versus-
  appliance transfer timing, organization conformance packs, CloudTrail
  integrity, AMI indirection, Directory Service MFA, KMS deletion alerts,
  local-Region S3 replication, Service Catalog, exact OAC authorization, and ARC
  routing-control automation.
- Added a fifth representative calibration set covering WorkSpaces with AD
  Connector, SCP inheritance scope, MemoryDB durability, forecasted Budgets
  actions, tag-policy enforcement limits, Security Hub Regional aggregation,
  Redis-versus-Memcached failover, NAT Gateway AZ scope, public-apex latency
  aliases, low-volume private cross-Region S3 access, and Aurora managed RPO.
- Continued that calibration with the two-sided cross-account AssumeRole model
  and coordinated Route 53, compute and RDS pilot-light recovery automation.
- Completed the supplied AWS Skill Builder 75-question calibration through
  Questions 54-75. Added only reusable service-composition, operational and
  elimination rules; the question text itself was not copied into the notes.
- Replaced the partial viewport/PDF evidence with the complete exported
  assessment workbook and created
  `docs/exam-prep/aws-skill-builder-sap-c02-assessment-review-20260809.md`.
  The review records all 30 keyed misses as reusable rules and prioritizes the
  eleven misses that were marked confident.
- Retained a byte-for-byte local-only copy of the complete answer-bearing
  workbook at
  `docs/exam-prep/aws-skill-builder-sap-c02-assessment-full-answer-set-20260809.xlsx`
  and indexed its private status in the exam-prep README. The file is
  deliberately Git-ignored because it contains the proprietary full assessment
  and the learner's displayed name. The original Downloads copy remains
  unchanged. Workbook restyling is parked because the required spreadsheet
  authoring runtime was unavailable in this session.
- Added
  `docs/exam-prep/aws-skill-builder-sap-c02-answer-difference-audit-20260809.md`.
  It checks all 30 learner-selection versus exported-key differences, preserves
  the supplied rationale as a concise paraphrase, records an independent
  verdict and gives a reusable learner takeaway. Twenty-eight keys stand;
  Question 7 is now learner-correct under current S3 behaviour; Question 10's
  key is dated, although the learner's policy-edit answer remains wrong.
- Corrected my earlier independent conclusions for tag-policy/SCP composition,
  Security Hub organization integration, Firewall Manager Network Firewall
  selection, single-user secret rotation and Db2 DMS-plus-SCT replatforming.
  My earlier `A,D` recommendation for Skill Builder Question 74 was wrong; the
  learner's `A,E` selection was correct.
- Preserved current-service truth where the assessment key is dated: Object
  Lock can now be enabled on existing buckets, and current KMS HSMs meet FIPS
  Level 3. Those two older keys are explicitly isolated rather than memorized.
- Added current official source references and updated revision dates for files
  changed in this pass.

The supplied AWS Skill Builder questions were used as calibration evidence,
not copied into the revision notes. The durable output is transferable
scenario logic and elimination rules.

## Evidence and Tool Boundary

- The current official SAP-C02 exam guide and in-scope service list were
  reviewed.
- Relevant official AWS documentation was checked for CloudFront origins,
  Lambda@Edge device handling, Session Manager, S3 encryption/Object Lock,
  placement groups, ENA/EFA, Direct Connect, Resolver endpoints, Aurora, and
  DynamoDB global-table consistency.
- The complete `.xlsx` export was inspected read-only. It contains one sheet,
  all 75 question markers, learner selections, AWS keyed answers, rationales
  and source links. The prior PDF contained only one browser viewport and is
  not the controlling evidence.
- This is source-note evidence only. It creates no new blind-recall score,
  timed mock result, booking authorization, Lakehouse implementation evidence,
  or live AWS evidence.

## Validation

The following checks passed on 2026-08-09:

- `git diff --check`;
- `jq empty docs/exam-prep/revision-notes/core/source-manifest.json`;
- repository-local Markdown link-target validation across `docs/exam-prep`
  and `docs/planning`;
- byte-for-byte duplicate check across all Markdown files in
  `docs/exam-prep/revision-notes`.

No AWS API or deployment command was run.

## Git State

- Branch: `main`; the relevant SAP-C02 documentation set was committed and
  pushed to `origin/main` in this state transition.
- The five targeted-note relocations were committed as moves into
  `docs/exam-prep/revision-notes/targeted-lessons/`; they are not lost files.
- The pre-existing `.github/copilot-instructions.md` and
  `.github/instructions/mermaid.instructions.md` Mermaid editor changes were
  deliberately excluded because they are unrelated to SAP-C02 readiness.
- The local full-answer workbook remains present but Git-ignored; only its
  paraphrased review and mismatch audit were published.
- No discard, reset, AWS API call, or AWS infrastructure mutation occurred.

## Known Risks and Open Items

- Five strong full mocks are persuasive evidence, but the chosen longitudinal
  gate now requires Mocks 006, 007, and 008 before a booking decision.
- The current exam guide is non-exhaustive and contains some legacy-listed
  services. Those receive recognition depth, not automatic endorsement for a
  new live architecture.
- The revision pack is intentionally not a textbook. New expansion should be
  driven by independent mock evidence or a clearly incomplete high-weight
  decision boundary.
- The Skill Builder export exposes broad historical misses, including eleven
  confident misses, but the stronger recent full-mock series remains the
  controlling readiness evidence. Use Mock 006 to test transfer rather than
  opening another broad content-expansion cycle.
- Migration remains the only explicitly incomplete major revision matrix in
  the booking checklist; address it only in capacity left after the mock
  cadence.
- Because the worktree contains mixed prior and current changes, any future
  commit needs an explicit staged-scope review.

## Next Tracker-Ordered Priority

Review the two narrow Mock 005 misses if that has not already been completed,
then complete Full Mock 006. Preserve the two-full-mock weekly cadence and use
only remaining capacity for the migration matrix. Do not make a booking
decision before Mock 008.

State transition status: **documentation milestone and publication occurred**.
The cross-domain revision-note pass, Skill Builder mismatch audit, and Mock 008
booking-gate extension are published. The exam-readiness programme remains in
longitudinal validation, with Full Mock 006 next. No booking or AWS state
transition occurred.

## Suggested New-Session Prompt

```text
Work in /Users/[redacted-user]/Workspace/cloud-projects/energy-market-data-lake.

Read AGENTS.md, handover.md, and
docs/planning/sap-c02-readiness-tracker.md before changing anything.

Continue the tracker-ordered SAP-C02 programme. Full Mocks 001-005 scored
73/75, 71/75, 75/75, 70/75, and 73/75. The cross-domain revision-note
calibration is published. Full Mock 006 is next; preserve the
two-mock cadence, review genuine misses, and do not make a booking decision
before Mock 008. Do not deploy or modify AWS, commit, push, discard, or resume
parked tutorial/container/AI work without explicit authorization.
```
