#!/usr/bin/env bash
set -euo pipefail

status=0

check_pattern() {
  local label="$1"
  local pattern="$2"

  if git grep -nE "$pattern" -- . ':!scripts/check_public_evidence_redaction.sh'; then
    printf '\n%s\n' "Public evidence redaction check failed: ${label}" >&2
    status=1
  fi
}

check_pattern "raw email address" '[[:alnum:]._%+-]+@[[:alnum:].-]+\.[[:alpha:]]{2,}'
check_pattern "raw phone number" '\+[0-9][0-9 ()-]{7,}'
check_pattern "local user home path" '/Users/[A-Za-z0-9._-]+'

if git grep -nE '"(Email|EmailAddress|PhoneNumber|AddressLine1|PostalCode)"[[:space:]]*:[[:space:]]*"[^[]' -- docs/evidence docs/planning docs/runbooks; then
  printf '\n%s\n' "Public evidence redaction check failed: raw AWS contact/address JSON value" >&2
  status=1
fi

if (( status != 0 )); then
  printf '%s\n' "Move exact private evidence outside the public repo, then commit only redacted summaries." >&2
fi

exit "$status"
