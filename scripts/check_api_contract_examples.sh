#!/usr/bin/env bash
set -euo pipefail

required_headers=(
  "Scenario:"
  "User goal:"
  "API contract:"
  "Forbidden:"
  "Regression signal:"
  "Review questions:"
)

check_file() {
  local file="$1"
  local missing=0
  for header in "${required_headers[@]}"; do
    if ! rg -q "^//! ${header}" "$file"; then
      printf 'missing header "%s" in %s\n' "$header" "$file" >&2
      missing=1
    fi
  done
  return "$missing"
}

failed=0
while IFS= read -r file; do
  check_file "$file" || failed=1
done < <(rg --files crates | rg 'examples/api_contract_s[0-9].*\.rs$' | sort)

while IFS= read -r file; do
  check_file "$file" || failed=1
done < <(rg --files docs/scenarios/api_gaps | rg 'api_contract_s[0-9].*\.rs$' | sort)

exit "$failed"
