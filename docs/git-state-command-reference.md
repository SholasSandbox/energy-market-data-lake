# Git State Command Reference

Use this when you need to check branch state, commit a completed slice, push a
branch, merge a branch, or clean up after a merged PR.

## 1. Check Where You Are

```bash
cd /Users/shola/Workspace/cloud-projects/energy-market-data-lake

git rev-parse --show-toplevel
git remote -v
git status --short --branch
```

Clean `main` should look like:

```text
## main...origin/main
```

Clean feature branch should look like:

```text
## feature/name...origin/feature/name
```

If you accidentally run `git switch -d <branch>`, you have detached `HEAD`; you
have not deleted the branch. Return to a normal branch with:

```bash
git switch main
```

## 2. Start A Fresh Feature Branch

```bash
git switch main
git pull --ff-only origin main
git switch -c feature/name-of-work
git status --short --branch
```

## 3. State, Boundary, And Fence

Use this language when a slice needs a clean handoff.

A **state** is the durable resting condition of the project after a phase. For
example: managed workflow routing is deployed, the dashboard is healthy, and
schedules remain disabled.

A **transition** is the move from one state to another. For example:
manual-only managed workflow operation moves toward schedule enablement
preflight.

A **boundary** is the scoped slice of work allowed for that transition. For
example: schedule enablement preflight is decision-only and no-apply.

The **fence** is the practical safety contract around the boundary. It lists
what must not be crossed by accident. For example:

- no Terraform apply
- no EventBridge schedule enablement
- no Step Functions execution
- no Bedrock invocation
- no S3 write
- no CloudFront invalidation

When standing at a boundary, confirm:

- current state
- next state
- actions allowed inside the slice
- actions outside the fence
- evidence that proves the next state
- stop condition if the evidence does not support moving forward

Long AWS/evidence slices should also leave a token-risk handoff before a
context limit becomes likely:

```text
Current state:
Branch:
Committed/pushed:
PR:
Evidence:
Clean/dirty tree:
Next safe action:
Do not do:
```

## 4. Review And Commit A Completed Slice

```bash
git status --short --branch
git diff --check
git diff --stat

git add <files-to-commit>
git diff --cached --check
git diff --cached --stat

git commit -m "type: short description"
git status --short --branch
```

Common commit prefixes:

- `feat:` for user-facing implementation.
- `fix:` for bug fixes.
- `docs:` for documentation and diagram-only changes.
- `chore:` for maintenance.

## 5. Push A Feature Branch

```bash
git push -u origin HEAD
git status --short --branch
```

Expected clean pushed branch:

```text
## feature/name-of-work...origin/feature/name-of-work
```

## 6. Check PR And Merge Status

List PRs for the current branch:

```bash
gh pr list --head "$(git branch --show-current)" --state all
```

View merge state for the current branch:

```bash
gh pr view "$(git branch --show-current)" \
  --json number,title,state,mergedAt,mergeCommit,url
```

Use Codex to generate PR titles, bodies, and merge-path commentary when the PR
needs a good narrative.

## 7. Merge A Branch Locally

Use this only when you are intentionally merging locally instead of using the
GitHub PR merge button.

```bash
git switch main
git pull --ff-only origin main

git merge --no-ff origin/feature/name-of-work \
  -m "merge: feature/name-of-work"

git status --short --branch
git push origin main

git status --short --branch
```

Expected state before push:

```text
## main...origin/main [ahead 1]
```

Expected state after push:

```text
## main...origin/main
```

You can also merge a local branch name if it still exists locally:

```bash
git merge --no-ff feature/name-of-work \
  -m "merge: feature/name-of-work"
```

Using `origin/feature/name-of-work` is useful when the branch is already pushed
and you want to merge exactly what remote GitHub has.

`git checkout main` is the older equivalent of `git switch main`.

If Git reports conflicts, resolve the files it lists, then complete the merge:

```bash
git status --short
# edit conflicted files

git add <resolved-files>
git commit
```

After the local merge is pushed and confirmed, delete the feature branch:

```bash
git branch -d feature/name-of-work
git push origin --delete feature/name-of-work
```

## 8. After A PR Is Merged

```bash
git switch main
git pull --ff-only origin main

git branch -d feature/name-of-work
git push origin --delete feature/name-of-work

git status --short --branch
```

Expected final state:

```text
## main...origin/main
```

## 9. Useful Safety Checks

Show branches already merged into local `main`:

```bash
git branch --merged main
```

Show the last few commits:

```bash
git log --oneline --decorate -5
```

Show files changed but not committed:

```bash
git diff --name-only
```

Show files staged for commit:

```bash
git diff --cached --name-only
```

Show whether you are on a branch or in detached `HEAD`:

```bash
git branch --show-current
git status --short --branch
```
