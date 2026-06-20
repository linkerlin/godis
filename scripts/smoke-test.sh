#!/usr/bin/env bash
set -euo pipefail

HOST="${GODIS_HOST:-127.0.0.1}"
PORT="${GODIS_PORT:-6399}"
CLI="${REDISCLI:-redis-cli}"

run() {
  echo "+ redis-cli -h ${HOST} -p ${PORT} $*"
  "${CLI}" -h "${HOST}" -p "${PORT}" "$@"
}

run PING | grep -q PONG
run SET smoke:key smoke:value | grep -q OK
run GET smoke:key | grep -q smoke:value
run INFO server | grep -qi godis
run ACL WHOAMI | grep -q default

echo "smoke test passed"
