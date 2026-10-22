#!/usr/bin/env bash
# E5-1 performance matrix: Godis (std/gnet × AOF × eviction × pipeline) vs Redis,
# in twin containers on one docker network so both sides see the same kernel,
# network stack and benchmark client. Results feed docs/PERFORMANCE.md.
#
# Usage:
#   bash scripts/bench.sh                      # quick profile (default cells)
#   BENCH_PROFILE=full bash scripts/bench.sh   # full Cartesian matrix
#   BENCH_REPEAT=3 bash scripts/bench.sh       # repeat every cell N times (variance)
#   REDIS_IMAGE=redis:8-alpine bash scripts/bench.sh
#   BENCH_N=1000000 BENCH_CLIENTS=50 BENCH_TESTS=set,get
#
# Output: tmp/bench/results.tsv — one row per test per repeat run:
#   label engine aof eviction pipeline test ops p50_ms p99_ms avg_ms
# Needs a running docker daemon. Out of scope: host-native engines on Windows,
# multi-host clusters, >512MB values.
set -euo pipefail
# Git Bash on Windows rewrites -v host:container args (colon lists); turn it off.
export MSYS_NO_PATHCONV=1

cd "$(dirname "$0")/.."
WORK="tmp/bench"
RESULTS="${RESULTS:-$WORK/results.tsv}"
PROFILE="${BENCH_PROFILE:-quick}"
REPEAT="${BENCH_REPEAT:-1}"
CLIENTS="${BENCH_CLIENTS:-50}"
OPS="${BENCH_N:-100000}"
TESTS="${BENCH_TESTS:-set,get}"
GODIS_IMAGE="${GODIS_IMAGE:-godis:bench}"
LRU_MEM=33554432 # 32MB cap for eviction cells; 50k-key bench footprint exceeds it

mkdir -p "$WORK"
: > "$RESULTS"

# Prefer an explicit image, else the newest 8.x tag the registry offers.
pick_redis_image() {
  local t
  for t in "${REDIS_IMAGE:-x}" redis:8.10-alpine redis:8-alpine redis:alpine; do
    [[ "$t" == "x" ]] && continue
    if docker image inspect "$t" >/dev/null 2>&1 || docker pull -q "$t" >/dev/null 2>&1; then
      echo "$t"; return 0
    fi
  done
  echo "no usable redis image" >&2; return 1
}
REDIS_IMG="$(pick_redis_image)"
echo "redis image: $REDIS_IMG  godis image: $GODIS_IMAGE"

# Windows Git Bash needs an absolute drive path for -v; Linux/CI passes through.
mount_path() {
  local p; p="$(realpath -m "$1")"
  command -v cygpath >/dev/null 2>&1 && cygpath -m "$p" || echo "$p"
}

NET="bench-$$"
docker network create "$NET" >/dev/null
SRV="bench-srv-$$"
cleanup() {
  docker rm -f "$SRV" >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_up() { # host port
  local i
  for i in $(seq 1 30); do
    if [[ "$(docker run --rm --network "$NET" "$REDIS_IMG" redis-cli -h "$1" -p "$2" ping 2>/dev/null)" == "PONG" ]]; then
      return 0
    fi
    sleep 1
  done
  echo "server $1:$2 did not answer PING" >&2; return 1
}

start_server() { # engine aof eviction conf-file
  local engine="$1" aof="$2" ev="$3" conf="$4"
  docker rm -f "$SRV" >/dev/null 2>&1 || true
  if [[ "$engine" == "redis" ]]; then
    local args=(redis-server --save "" --appendonly no --maxmemory-policy noeviction)
    [[ "$aof" != "off" ]] && args+=(--appendonly yes --appendfsync "$aof")
    [[ "$ev" == "lru" ]] && args+=(--maxmemory "$LRU_MEM" --maxmemory-policy allkeys-lru)
    docker run -d --name "$SRV" --network "$NET" "$REDIS_IMG" "${args[@]}" >/dev/null
    wait_up "$SRV" 6379
  else
    {
      echo "bind 0.0.0.0"
      echo "port 6399"
      echo "dir /data"
      echo "appendonly $([[ "$aof" == "off" ]] && echo no || echo yes)"
      [[ "$aof" != "off" ]] && echo "appendfsync $aof"
      [[ "$aof" != "off" ]] && echo "appendfilename appendonly.aof"
      echo "maxmemory $([[ "$ev" == "lru" ]] && echo "$LRU_MEM" || echo 0)"
      echo "maxmemory-policy $([[ "$ev" == "lru" ]] && echo allkeys-lru || echo noeviction)"
      [[ "$engine" == "gnet" ]] && echo "use-gnet yes"
    } > "$conf"
    docker run -d --name "$SRV" --network "$NET" \
      -v "$(mount_path "$conf"):/etc/godis/godis.conf:ro" "$GODIS_IMAGE" >/dev/null
    wait_up "$SRV" 6399
  fi
}

# run_bench <label> <engine(std|gnet|redis)> <aof(off|everysec|always)> <eviction(off|lru)> <pipeline> [keyspace]
run_bench() {
  local label="$1" engine="$2" aof="$3" ev="$4" P="$5" keyspace="${6:-0}"
  start_server "$engine" "$aof" "$ev" "$WORK/$label.conf"

  local -a extra=()
  [[ "$keyspace" != "0" ]] && extra+=(-r "$keyspace")
  local port; [[ "$engine" == "redis" ]] && port=6379 || port=6399

  local rep r line m
  for rep in $(seq 1 "$REPEAT"); do
    r="$(docker run --rm --network "$NET" "$REDIS_IMG" redis-benchmark \
      -h "$SRV" -p "$port" -t "$TESTS" -c "$CLIENTS" -n "$OPS" -d 3 -P "$P" \
      ${extra[@]+"${extra[@]}"} 2>/dev/null)" || true
    # redis-benchmark 8.x per-test tail:
    #   Summary:
    #     throughput summary: N requests per second
    #     latency summary (msec):
    #             avg  min  p50  p95  p99  max
    #           0.053 0.016 0.047 ...      (values line)
    # The "====== TEST ======" header may share its line with progress output,
    # so match it anywhere in the line.
    awk -v label="$label" -v engine="$engine" -v aof="$aof" -v ev="$ev" -v P="$P" '
      match($0, /====== [A-Za-z_0-9]+ ======/) { test=substr($0, RSTART+7, RLENGTH-14) }
      /throughput summary:/ { ops=$3 }
      /latency summary \(msec\):/ { inlat=1; next }
      inlat && $1 ~ /^[0-9.]+$/ {
        printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", label, engine, aof, ev, P, test, ops, $3, $5, $1
        inlat=0
      }
    ' <<< "$r" >> "$RESULTS"
  done
  docker rm -f "$SRV" >/dev/null
}

cells=()
if [[ "$PROFILE" == "full" ]]; then
  for e in std gnet; do for a in off everysec always; do for p in 1 16 64; do
    cells+=("$e-$a-p$p|$e|$a|off|$p|0" "$e-$a-lru-p$p|$e|$a|lru|$p|50000")
  done; done; done
  for a in off everysec; do for p in 1 16; do
    cells+=("redis-$a-p$p|redis|$a|off|$p|0")
  done; done
else
  # quick: representative cells — engine spread, AOF cost, pipeline scaling,
  # one eviction-under-pressure cell, one deep-pipeline cell, redis baseline.
  cells=(
    "std-off-p1|std|off|off|1|0"
    "std-off-p16|std|off|off|16|0"
    "std-everysec-p1|std|everysec|off|1|0"
    "std-everysec-p16|std|everysec|off|16|0"
    "std-always-p1|std|always|off|1|0"
    "std-off-lru-p16|std|off|lru|16|50000"
    "redis-off-lru-p16|redis|off|lru|16|50000"
    "gnet-off-p1|gnet|off|off|1|0"
    "gnet-off-p16|gnet|off|off|16|0"
    "gnet-everysec-p16|gnet|everysec|off|16|0"
    "gnet-off-p64|gnet|off|off|64|0"
    "redis-off-p1|redis|off|off|1|0"
    "redis-off-p16|redis|off|off|16|0"
    "redis-everysec-p1|redis|everysec|off|1|0"
    "redis-everysec-p16|redis|everysec|off|16|0"
  )
fi

for cell in "${cells[@]}"; do
  IFS='|' read -r label engine aof ev p ks <<< "$cell"
  [[ -n "${BENCH_ONLY:-}" && "$label" != "$BENCH_ONLY" ]] && continue
  echo "== $label"
  run_bench "$label" "$engine" "$aof" "$ev" "$p" "$ks"
done

echo "done -> $RESULTS"
