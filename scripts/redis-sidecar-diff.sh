#!/usr/bin/env bash
# R4-1 scaffold: optional tiny allowlist diff vs a Redis 8 sidecar.
# NOT a full compatibility suite. Does not claim Redis parity.
# Allowlist only: PING / ECHO / SET / GET / STRLEN / APPEND / DEL / EXISTS / INCR / DECR / TYPE
# Out of scope: modules, DUMP/RESTORE, gossip, ACL, cluster, FT.*, FUNCTIONS.
#
# Prerequisites: redis-cli; Redis on REDIS_HOST:REDIS_PORT; Godis on GODIS_HOST:GODIS_PORT.
# Self-check (no servers): bash scripts/redis-sidecar-diff.sh --selfcheck
set -euo pipefail

REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
REDIS_PORT="${REDIS_PORT:-6379}"
GODIS_HOST="${GODIS_HOST:-127.0.0.1}"
GODIS_PORT="${GODIS_PORT:-6399}"
CLI="${REDISCLI:-redis-cli}"

redis_cli() { "${CLI}" -h "${REDIS_HOST}" -p "${REDIS_PORT}" "$@"; }
godis_cli() { "${CLI}" -h "${GODIS_HOST}" -p "${GODIS_PORT}" "$@"; }

die() { echo "$*" >&2; exit 1; }

eq_both() {
  local label="$1" want="$2"
  shift 2
  local rv gv
  rv="$(redis_cli "$@")"
  gv="$(godis_cli "$@")"
  if [[ "${rv}" != "${want}" || "${gv}" != "${want}" ]]; then
    die "${label} mismatch: redis=${rv} godis=${gv} want=${want}"
  fi
}

if [[ "${1:-}" == "--selfcheck" ]]; then
  if ! command -v "${CLI}" >/dev/null 2>&1; then
    echo "R4-1 selfcheck: ${CLI} not on PATH (install later; allowlist still documented)"
    exit 0
  fi
  echo "R4-1 selfcheck ok: allowlist=PING,ECHO,SET,GET,STRLEN,APPEND,DEL,EXISTS,INCR,DECR,TYPE; full suite out of scope"
  exit 0
fi

echo "R4-1 scaffold: allowlist-only (PING/ECHO/SET/GET/STRLEN/APPEND/DEL/EXISTS/INCR/DECR/TYPE). Full module/DUMP/gossip diffs are out of scope."

r_ping="$(redis_cli PING)"
g_ping="$(godis_cli PING)"
if [[ "${r_ping}" != "PONG" || "${g_ping}" != "PONG" ]]; then
  die "PING mismatch: redis=${r_ping} godis=${g_ping}"
fi

eq_both "ECHO" "r41-ok" ECHO "r41-ok"

key="sidecar:allowlist:$$"
val="ok"

redis_cli DEL "${key}" >/dev/null || true
godis_cli DEL "${key}" >/dev/null || true

eq_both "SET" "OK" SET "${key}" "${val}"
eq_both "GET" "${val}" GET "${key}"
eq_both "STRLEN" "2" STRLEN "${key}"
eq_both "APPEND" "3" APPEND "${key}" "!"
eq_both "GET-appended" "ok!" GET "${key}"
eq_both "EXISTS" "1" EXISTS "${key}"
eq_both "TYPE" "string" TYPE "${key}"

nkey="sidecar:allowlist:n:$$"
redis_cli DEL "${nkey}" >/dev/null || true
godis_cli DEL "${nkey}" >/dev/null || true
eq_both "INCR" "1" INCR "${nkey}"
eq_both "INCR2" "2" INCR "${nkey}"
eq_both "DECR" "1" DECR "${nkey}"
eq_both "GET-n" "1" GET "${nkey}"

eq_both "DEL" "1" DEL "${key}"
eq_both "EXISTS-after-del" "0" EXISTS "${key}"
eq_both "DEL-n" "1" DEL "${nkey}"

echo "allowlist diff passed (scaffolding only; see docs/COMPATIBILITY.md R4-1)"
