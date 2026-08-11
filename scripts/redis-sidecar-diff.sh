#!/usr/bin/env bash
# R4-1 scaffold: optional tiny allowlist diff vs a Redis 8 sidecar.
# NOT a full compatibility suite. Does not claim Redis parity.
# Prerequisites: redis-cli; Redis on REDIS_HOST:REDIS_PORT; Godis on GODIS_HOST:GODIS_PORT.
set -euo pipefail

REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
REDIS_PORT="${REDIS_PORT:-6379}"
GODIS_HOST="${GODIS_HOST:-127.0.0.1}"
GODIS_PORT="${GODIS_PORT:-6399}"
CLI="${REDISCLI:-redis-cli}"

redis_cli() { "${CLI}" -h "${REDIS_HOST}" -p "${REDIS_PORT}" "$@"; }
godis_cli() { "${CLI}" -h "${GODIS_HOST}" -p "${GODIS_PORT}" "$@"; }

echo "R4-1 scaffold: allowlist-only (PING/SET/GET). Full module/DUMP/gossip diffs are out of scope."

r_ping="$(redis_cli PING)"
g_ping="$(godis_cli PING)"
if [[ "${r_ping}" != "PONG" || "${g_ping}" != "PONG" ]]; then
  echo "PING mismatch: redis=${r_ping} godis=${g_ping}" >&2
  exit 1
fi

key="sidecar:allowlist:$$"
val="ok"
redis_cli DEL "${key}" >/dev/null || true
godis_cli DEL "${key}" >/dev/null || true
redis_cli SET "${key}" "${val}" | grep -q OK
godis_cli SET "${key}" "${val}" | grep -q OK
r_get="$(redis_cli GET "${key}")"
g_get="$(godis_cli GET "${key}")"
if [[ "${r_get}" != "${val}" || "${g_get}" != "${val}" ]]; then
  echo "GET mismatch: redis=${r_get} godis=${g_get}" >&2
  exit 1
fi

echo "allowlist diff passed (scaffolding only; see docs/COMPATIBILITY.md R4-1)"
