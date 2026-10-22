#!/usr/bin/env bash
# E6-2 random differential test: type-aware random command sequences dual-sent
# to Godis and a Redis 8.10 sidecar (twin containers), replies compared per
# step and full state dumped at checkpoints. Failures auto-minimize to a
# replayable file under tmp/rnddiff/.
#
# Usage:
#   bash scripts/rnd-diff.sh                    # 100k steps, seed 42
#   STEPS=20000 SEED=7 bash scripts/rnd-diff.sh
#   REPLAY=tmp/rnddiff/fail-*.txt bash scripts/rnd-diff.sh
#
# Needs a running docker daemon. State summary uses per-key TYPE + normalized
# dumps (cross-engine DEBUG DIGEST is not comparable — different algorithms).
set -euo pipefail
export MSYS_NO_PATHCONV=1

cd "$(dirname "$0")/.."
STEPS="${STEPS:-100000}"
SEED="${SEED:-42}"
GODIS_PORT="${GODIS_PORT:-16380}"
REDIS_PORT="${REDIS_PORT:-16379}"
NET="rnd-net-$$"
GR="rnd-redis-$$"
GG="rnd-godis-$$"

cleanup() {
  docker rm -f "$GR" "$GG" >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if [[ -z "${REPLAY:-}" ]]; then
  docker build -q -t godis:bench .
  docker network create "$NET" >/dev/null
  docker run -d --name "$GR" --network "$NET" -p "$REDIS_PORT:6379" redis:8.10-alpine >/dev/null
  docker run -d --name "$GG" --network "$NET" -p "$GODIS_PORT:6399" godis:bench >/dev/null
  sleep 2
fi

go run ./scripts/rnddiff \
  --godis "127.0.0.1:${GODIS_PORT}" --redis "127.0.0.1:${REDIS_PORT}" \
  --steps "$STEPS" --seed "$SEED" ${REPLAY:+--replay "$REPLAY"}
