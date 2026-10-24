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
FAILS=0

# --raw keeps multi-line replies comparable across sides.
# REDISCLI may carry arguments (e.g. "docker exec -i r41-sidecar redis-cli");
# word-split it once so both helpers invoke the real argv. stdin is detached:
# docker exec -i would otherwise consume the case file the read-loop streams.
read -ra CLI_ARR <<< "${REDISCLI:-redis-cli}"
redis_cli() { "${CLI_ARR[@]}" --raw -h "${REDIS_HOST}" -p "${REDIS_PORT}" "${AUTH_ARGS[@]}" "$@" </dev/null; }
godis_cli() { "${CLI_ARR[@]}" --raw -h "${GODIS_HOST}" -p "${GODIS_PORT}" "${AUTH_ARGS[@]}" "$@" </dev/null; }

# AUTH_ARGS carries the inline-auth flags installed by the @auth directive:
# one token → default-user password (-a), two tokens → named user (--user/--pass).
AUTH_ARGS=()

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
  if [[ "${R41_CONTINUE:-0}" == "1" ]]; then
    FAILS=$((FAILS + 1))
    return 0
  fi
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

# split_args performs quote-aware whitespace splitting for case command lines,
# so paths like `$.users[?(@.age > 20)].name` (quoted) stay one argv.
# An explicit empty argument ("") is preserved (e.g. CONFIG SET requirepass "").
split_args() {
  local s="$1" cur="" quote="" out=() quoted=0
  local i c
  for ((i = 0; i < ${#s}; i++)); do
    c="${s:i:1}"
    if [[ -n "${quote}" ]]; then
      if [[ "${c}" == "${quote}" ]]; then
        quote=""
        quoted=1
      else
        cur+="${c}"
      fi
    else
      case "${c}" in
        " "|$'\t')
          if [[ -n "${cur}" || ${quoted} -eq 1 ]]; then
            out+=("${cur}")
            cur=""
            quoted=0
          fi
          ;;
        "'"|'"')
          if [[ -z "${cur}" ]]; then
            quote="${c}"
          else
            cur+="${c}" # quotes inside a JSON literal are data, not delimiters
          fi
          ;;
        *)
          cur+="${c}"
          ;;
      esac
    fi
  done
  if [[ -n "${cur}" || ${quoted} -eq 1 ]]; then
    out+=("${cur}")
  fi
  printf '%s\n' "${out[@]}"
}

# Expand literal \n in WANT to a real newline (multi-line --raw replies).
# Token $-1 → empty field (redis-cli --raw prints nothing for null bulk).
# Token $empty → entirely empty reply (empty multi-bulk / empty string).
expand_want() {
  local s="$1"
  s="${s//\\n/$'\n'}"
  s="${s//\$-1/}"
  if [[ "${s}" == '$empty' ]]; then
    s=""
  fi
  printf '%s' "${s}"
}

normalize_cli_out() {
  local s="$1"
  s="${s//$'\r'/}"
  # Trim trailing newlines only (keep internal newlines).
  while [[ "${s}" == *$'\n' ]]; do s="${s%$'\n'}"; done
  printf '%s' "${s}"
}

is_int() { [[ "$1" =~ ^-?[0-9]+$ ]]; }

check_want() {
  local want="$1" got="$2"
  if [[ "${want}" =~ ^\>=(-?[0-9]+)$ ]]; then
    local bound="${BASH_REMATCH[1]}"
    is_int "${got}" || return 1
    if (( got >= bound )); then return 0; fi
    return 1
  fi
  if [[ "${want}" =~ ^\<=(-?[0-9]+)$ ]]; then
    local bound="${BASH_REMATCH[1]}"
    is_int "${got}" || return 1
    if (( got <= bound )); then return 0; fi
    return 1
  fi
  [[ "${got}" == "${want}" ]]
}

run_setup() {
  local -a args=("$@")
  redis_cli "${args[@]}" >/dev/null 2>&1 || true
  godis_cli "${args[@]}" >/dev/null 2>&1 || true
}

# run_admin executes an authenticated setup command on both sides with inline
# AUTH (-a): every redis-cli invocation is a fresh connection. Output is
# discarded. Used for sections that must flip server-wide auth state
# (requirepass) and restore it afterwards.
run_admin() {
  local pw="$1"
  shift
  local -a args=("$@")
  redis_cli -a "${pw}" --no-auth-warning "${args[@]}" >/dev/null 2>&1 || true
  godis_cli -a "${pw}" "${args[@]}" >/dev/null 2>&1 || true
}

run_eq() {
  local label="$1" want="$2"
  shift 2
  local -a args=("$@")
  local rv gv
  want="$(expand_want "${want}")"
  rv="$(normalize_cli_out "$(redis_cli "${args[@]}")")"
  gv="$(normalize_cli_out "$(godis_cli "${args[@]}")")"
  if ! check_want "${want}" "${rv}" || ! check_want "${want}" "${gv}" || [[ "${rv}" != "${gv}" ]]; then
    # For >=/<= , allow sides to differ by clock skew when both satisfy the bound.
    if [[ "${want}" == ">="* || "${want}" == "<="* ]]; then
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
      # @auth [<user> <pass> | <pass>]: inline auth for the assertion commands
      # that follow; bare @auth clears it.
      if [[ "${expanded}" =~ ^@auth[[:space:]]*$ ]]; then
        AUTH_ARGS=()
        continue
      fi
      if [[ "${expanded}" =~ ^@auth([[:space:]]|$) ]]; then
        mapfile -t args < <(split_args "${expanded#@auth}")
        if [[ ${#args[@]} -ge 2 ]]; then
          AUTH_ARGS=(--user "${args[0]}" --pass "${args[1]}" --no-auth-warning)
        elif [[ ${#args[@]} -eq 1 ]]; then
          AUTH_ARGS=(-a "${args[0]}" --no-auth-warning)
        else
          AUTH_ARGS=()
        fi
        continue
      fi
      raw="${expanded:1}"
      mapfile -t args < <(split_args "${raw}")
      # @admin <password> <command...>: authenticated setup (see run_admin).
      if [[ ${#args[@]} -gt 0 && "${args[0],,}" == "admin" ]]; then
        if [[ ${#args[@]} -lt 2 ]]; then
          die "bad @admin line: ${line}"
        fi
        run_admin "${args[1]}" "${args[@]:2}"
        continue
      fi
      # @CMD marks an explicit setup command; strip the marker before invoking
      # redis-cli (other @-prefixed lines are raw setup commands).
      if [[ ${#args[@]} -gt 0 && "${args[0],,}" == "cmd" ]]; then
        args=("${args[@]:1}")
      fi
      run_setup "${args[@]}"
      continue
    fi
    label="${expanded%%|*}"
    rest="${expanded#*|}"
    want="${rest%%|*}"
    raw="${rest#*|}"
    mapfile -t args < <(split_args "${raw}")
    if [[ -z "${label}" || -z "${want}" || ${#args[@]} -eq 0 ]]; then
      die "bad case line: ${line}"
    fi
    run_eq "${label}" "${want}" "${args[@]}"
    n=$((n + 1))
  done < "${CASES}"
  echo "ran ${n} assertions from $(basename "${CASES}") (failures=${FAILS})"
}

[[ -f "${CASES}" ]] || die "cases file not found: ${CASES}"
load_allowlist

if [[ "${1:-}" == "--selfcheck" ]]; then
  if ! command -v "${CLI_ARR[0]}" >/dev/null 2>&1; then
    echo "R4-1 selfcheck: ${CLI_ARR[0]} not on PATH (install later; allowlist still documented)"
    exit 0
  fi
  if ! "${CLI_ARR[@]}" --help 2>&1 | grep -q -- '--raw'; then
    echo "R4-1 selfcheck: ${CLI_ARR[0]} present but --raw unsupported; upgrade redis-cli"
    exit 0
  fi
  echo "R4-1 selfcheck ok: cases=$(basename "${CASES}"); allowlist=${ALLOWLIST}; full suite (FT/modules/DUMP/cluster) out of scope"
  exit 0
fi

echo "R4-1 scaffold: allowlist-only via ${CASES} (${ALLOWLIST}). Full module/DUMP/gossip/FT/cluster diffs are out of scope."

run_cases
if (( FAILS > 0 )); then
  exit 1
fi
echo "allowlist diff passed (scaffolding only; see docs/COMPATIBILITY.md R4-1)"
