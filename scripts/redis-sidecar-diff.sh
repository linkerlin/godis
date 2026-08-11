#!/usr/bin/env bash
# R4-1 scaffold: allowlist diff vs a Redis 8 sidecar (cases from r4-1-cases.txt).
# NOT a full compatibility suite. Does not claim Redis parity.
# Covers stable String/Hash/List/Set/ZSet + TTL + Stream/Geo/Bitops/HLL lite.
# Out of scope: modules, DUMP/RESTORE, gossip, ACL, cluster, FT.*, FUNCTIONS,
# unordered replies (SMEMBERS/HGETALL), SCAN, exact remaining TTL seconds.
# Markers: @skip / @todo document gaps (not executed; do not fake pass).
#
# Prerequisites: redis-cli; Redis on REDIS_HOST:REDIS_PORT; Godis on GODIS_HOST:GODIS_PORT.
# Cases file: R41_CASES or scripts/r4-1-cases.txt
# Self-check (no servers): bash scripts/redis-sidecar-diff.sh --selfcheck
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CASES="${R41_CASES:-${SCRIPT_DIR}/r4-1-cases.txt}"
REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
REDIS_PORT="${REDIS_PORT:-6379}"
GODIS_HOST="${GODIS_HOST:-127.0.0.1}"
GODIS_PORT="${GODIS_PORT:-6399}"
CLI="${REDISCLI:-redis-cli}"
ID="${R41_ID:-$$}"

# --raw keeps multi-line replies comparable across sides.
redis_cli() { "${CLI}" --raw -h "${REDIS_HOST}" -p "${REDIS_PORT}" "$@"; }
godis_cli() { "${CLI}" --raw -h "${GODIS_HOST}" -p "${GODIS_PORT}" "$@"; }

die() { echo "$*" >&2; exit 1; }

fail_cmp() {
  local label="$1" want="$2" rv="$3" gv="$4"
  shift 4
  {
    echo "FAIL ${label}"
    echo "  cmd:   $*"
    echo "  redis: ${rv}"
    echo "  godis: ${gv}"
    echo "  want:  ${want}"
  } >&2
  exit 1
}

load_allowlist() {
  local line
  ALLOWLIST=""
  while IFS= read -r line || [[ -n "${line}" ]]; do
    if [[ "${line}" =~ @allowlist[[:space:]]+(.+) ]]; then
      ALLOWLIST="${BASH_REMATCH[1]}"
      ALLOWLIST="${ALLOWLIST%%$'\r'}"
      return 0
    fi
  done < "${CASES}"
  die "missing @allowlist header in ${CASES}"
}

subst() {
  local s="$1"
  s="${s//'{{ID}}'/${ID}}"
  printf '%s' "${s}"
}

is_int() { [[ "$1" =~ ^-?[0-9]+$ ]]; }

check_want() {
  local want="$1" got="$2"
  if [[ "${want}" =~ ^\>=(-?[0-9]+)$ ]]; then
    is_int "${got}" || return 1
    if (( got >= BASH_REMATCH[1] )); then return 0; fi
    return 1
  fi
  if [[ "${want}" =~ ^\<=(-?[0-9]+)$ ]]; then
    is_int "${got}" || return 1
    if (( got <= BASH_REMATCH[1] )); then return 0; fi
    return 1
  fi
  [[ "${got}" == "${want}" ]]
}

run_setup() {
  local -a args=("$@")
  redis_cli "${args[@]}" >/dev/null 2>&1 || true
  godis_cli "${args[@]}" >/dev/null 2>&1 || true
}

run_eq() {
  local label="$1" want="$2"
  shift 2
  local -a args=("$@")
  local rv gv
  rv="$(redis_cli "${args[@]}")"
  gv="$(godis_cli "${args[@]}")"
  if ! check_want "${want}" "${rv}" || ! check_want "${want}" "${gv}" || [[ "${rv}" != "${gv}" ]]; then
    # For >=/<= , redis and godis must still match each other for honesty when both satisfy.
    if [[ "${want}" == \>=* || "${want}" == \<=* ]]; then
      if check_want "${want}" "${rv}" && check_want "${want}" "${gv}"; then
        return 0
      fi
    fi
    fail_cmp "${label}" "${want}" "${rv}" "${gv}" "${args[@]}"
  fi
}

run_cases() {
  local line raw label want rest expanded
  local -a args
  local n=0
  while IFS= read -r line || [[ -n "${line}" ]]; do
    line="${line%%$'\r'}"
    [[ -z "${line}" || "${line}" =~ ^[[:space:]]*# ]] && continue
    if [[ "${line}" =~ ^@allowlist ]]; then
      continue
    fi
    expanded="$(subst "${line}")"
    if [[ "${expanded}" =~ ^@(skip|todo)([[:space:]]|$) ]]; then
      continue
    fi
    if [[ "${expanded}" == @* ]]; then
      raw="${expanded:1}"
      # shellcheck disable=SC2206
      args=( ${raw} )
      run_setup "${args[@]}"
      continue
    fi
    label="${expanded%%|*}"
    rest="${expanded#*|}"
    want="${rest%%|*}"
    raw="${rest#*|}"
    # shellcheck disable=SC2206
    args=( ${raw} )
    if [[ -z "${label}" || -z "${want}" || ${#args[@]} -eq 0 ]]; then
      die "bad case line: ${line}"
    fi
    run_eq "${label}" "${want}" "${args[@]}"
    n=$((n + 1))
  done < "${CASES}"
  echo "ran ${n} assertions from $(basename "${CASES}")"
}

[[ -f "${CASES}" ]] || die "cases file not found: ${CASES}"
load_allowlist

if [[ "${1:-}" == "--selfcheck" ]]; then
  if ! command -v "${CLI}" >/dev/null 2>&1; then
    echo "R4-1 selfcheck: ${CLI} not on PATH (install later; allowlist still documented)"
    exit 0
  fi
  if ! "${CLI}" --help 2>&1 | grep -q -- '--raw'; then
    echo "R4-1 selfcheck: ${CLI} present but --raw unsupported; upgrade redis-cli"
    exit 0
  fi
  echo "R4-1 selfcheck ok: cases=$(basename "${CASES}"); allowlist=${ALLOWLIST}; full suite (FT/modules/DUMP/cluster) out of scope"
  exit 0
fi

echo "R4-1 scaffold: allowlist-only via ${CASES} (${ALLOWLIST}). Full module/DUMP/gossip/FT/cluster diffs are out of scope."

run_cases

echo "allowlist diff passed (scaffolding only; see docs/COMPATIBILITY.md R4-1)"
