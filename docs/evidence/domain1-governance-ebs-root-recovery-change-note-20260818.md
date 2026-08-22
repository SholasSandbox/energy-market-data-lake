# Domain 1 Governance Change Note - EBS Root Recovery - 2026-08-18

## Status

Completed. Both user-confirmed EBS volumes were permanently deleted in
`eu-west-2`; the user then separately authorized termination of the remaining
stopped, diskless EC2 instance. The root-user SCP was restored and verified
after each bounded recovery window.

## Trigger

The root user for workload account `464975959576` received an explicit SCP
deny for `ec2:DescribeInstances` in `eu-west-2`. The denial named
`DenyRootUserActions-LakehouseWorkloads` / `p-dv2ss5us`, preventing the account
owner from inspecting the EBS volume that is believed to be generating an
unwanted charge.

## Approval and Scope

The user explicitly requested the policy correction required to complete this
cost-control cleanup. The authorized exception is limited to temporarily
detaching only `p-dv2ss5us` from `ou-gbyf-m6ppfmpq` / `Lakehouse Workloads OU`.
The OU currently contains only workload account `464975959576`.

The change did not modify policy content, account placement, IAM identities,
the no-leaving-organization SCP, or any other AWS control. The user first
confirmed deletion of `vol-0aca77f029e4cb5d2`, then explicitly expanded the
deletion scope to both discovered volumes, and finally explicitly requested
termination of `i-0ab1ed417121d828d`.

## Fresh Prechange Evidence

At `2026-08-18T18:04:55Z`, the management-account `org-admin` session confirmed:

- management account `349687196588` is the active Organizations authority;
- workload account `464975959576` is `ACTIVE` in
  `ou-gbyf-m6ppfmpq` / `Lakehouse Workloads OU`;
- that OU contains no other account;
- `p-dv2ss5us` targets only that OU; and
- the OU has `FullAWSAccess`, `DenyLeavingOrganization-LakehouseWorkloads`,
  and `DenyRootUserActions-LakehouseWorkloads` attached.

The structured prechange record is
`docs/evidence/domain1-governance-ebs-root-recovery-prechange-20260818.json`.

## Recovery Change

```bash
aws organizations detach-policy \
  --profile org-admin \
  --policy-id p-dv2ss5us \
  --target-id ou-gbyf-m6ppfmpq
```

Immediately verify that `p-dv2ss5us` has no targets while
`DenyLeavingOrganization-LakehouseWorkloads` and `FullAWSAccess` remain
attached to the OU.

Three short detach windows completed successfully on 2026-08-18. Immediate
verification during each window returned no targets for `p-dv2ss5us`; the
other two SCPs remained attached.

## Root-Session Validation

After the detach, the workload root session could read EC2 in `eu-west-2`.
The console inventory found:

- `vol-0aca77f029e4cb5d2`: `gp2`, 80 GiB, created 2025-10-17,
  `available`, untagged, unattached, unencrypted, and with no source snapshot;
- `vol-02280579617081ef0`: `gp2`, 8 GiB, `in-use`, attached as the root volume
  of stopped instance `i-0ab1ed417121d828d` / `saa-lab-ssh-instance`; and
- no snapshots owned by the account in `eu-west-2`.

The first confirmed deletion removed `vol-0aca77f029e4cb5d2`. After the user
explicitly requested deletion of both volumes, the root volume
`vol-02280579617081ef0` was detached from the stopped instance and became
`available`; the console then confirmed its permanent deletion.

Deleting the root volume did not itself terminate `i-0ab1ed417121d828d`. The
instance initially remained stopped and unusable without its root disk. In a
separate follow-up action, the user explicitly authorized termination. The
console confirmed successful initiation and then displayed the instance state
as `Terminated`.

The final instance details showed no public IPv4 address and no Elastic IP
address. The terminated record can remain visible temporarily in the EC2
console; that retention does not indicate a running instance.

## Rollback / Restoration

```bash
aws organizations attach-policy \
  --profile org-admin \
  --policy-id p-dv2ss5us \
  --target-id ou-gbyf-m6ppfmpq
```

After restoration, verify that `p-dv2ss5us` again has exactly one target and
that the workload account remains active in the same OU.

Final live verification confirmed:

- `p-dv2ss5us` targets exactly `ou-gbyf-m6ppfmpq`;
- the OU has `DenyRootUserActions-LakehouseWorkloads`,
  `DenyLeavingOrganization-LakehouseWorkloads`, and `FullAWSAccess` attached;
- workload account `464975959576` remains `ACTIVE` in that OU;
- AWS displayed successful deletion confirmations for both volumes;
- `i-0ab1ed417121d828d` reached `Terminated`, with no public IPv4 or Elastic IP
  address shown in its final details; and
- the earlier snapshot inventory contained no snapshots owned by this account
  in `eu-west-2`.

CloudTrail in `us-east-1` recorded the two temporary `DetachPolicy` events as
`6e86c11a-8334-44b0-a3d4-99bc2f7399c5` and
`77d078b6-da2f-4dc0-94ec-3c4d849767ea`. It recorded the first restoration as
`912b4edc-d6c4-4586-b0cc-99fce864def4`, and the second restoration as
`0e37f84f-8ddf-4f61-8f7b-69aca664ef42`. The follow-up termination window was
recorded as `376f192e-d1c3-4b42-9d59-6c6cbab94522` for `DetachPolicy` and
`82305752-0e10-4c92-aa40-1207ac3febd8` for `AttachPolicy`. Live Organizations
target and policy inventories independently verified the final restoration.

The structured postchange record is
`docs/evidence/domain1-governance-ebs-root-recovery-postchange-20260818.json`.

## Risk Boundary

While detached, the workload account root user is no longer restricted by the
root-user SCP. Each exception was kept brief, limited to the authorized
cost-control action, and closed by restoring the guardrail before unrelated
work.

## SAP-C02 Relevance

This is Domain 1 governance and Domain 3 cost-optimization evidence: a
preventive OU-scoped SCP is relaxed through a bounded, reversible recovery
path with fresh target inventory and explicit restoration checks.
