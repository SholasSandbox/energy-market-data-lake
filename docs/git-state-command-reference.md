# Git State Command Reference

Use this when you need to check branch state, commit a completed slice, push a
branch, or clean up after a merged PR.

## 1. Check Where You Are

```bash
cd /Users/[redacted-user]/Workspace/cloud-projects/energy-market-data-lake

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

## 2. Start A Fresh Feature Branch

```bash
git switch main
git pull --ff-only origin main
git switch -c feature/name-of-work
git status --short --branch
```

## 3. Review And Commit A Completed Slice

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

## 4. Push A Feature Branch

```bash
git push -u origin HEAD
git status --short --branch
```

Expected clean pushed branch:

```text
## feature/name-of-work...origin/feature/name-of-work
```

## 5. Check PR And Merge Status

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

## 6. After A PR Is Merged

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

## 7. Useful Safety Checks

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
