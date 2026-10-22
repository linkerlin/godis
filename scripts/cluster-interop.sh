#!/usr/bin/env bash
# E7-1 cluster interop, third-party-client leg: boots a two-node godis cluster
# in twin containers (one docker network) and drives it with the real
# redis-cli -c from redis:8.10-alpine — MOVED redirect following, the exact
# contract smart clients rely on. All addresses are container names so node-to
# node leader forwarding and client redirects use the same topology.
#
# Usage:  bash scripts/cluster-interop.sh     (needs docker)
# Protocol-level assertions without docker:  go test ./e7cluster
set -euo pipefail
export MSYS_NO_PATHCONV=1

cd "$(dirname "$0")/.."
P1="${P1:-16381}"; P2="${P2:-16382}"
R1="${R1:-16481}"; R2="${R2:-16482}"
NET="cinterop-$$"
N1="godis-n1-$$"; N2="godis-n2-$$"
WORK="tmp/cluster-interop"

mkdir -p "$WORK"
mount_path() { command -v cygpath >/dev/null 2>&1 && cygpath -m "$(realpath -m "$1")" || realpath -m "$1"; }

cleanup() {
  docker rm -f "$N1" "$N2" >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker build -q -t godis:bench . >/dev/null
docker network create "$NET" >/dev/null

cat > "$WORK/n1.conf" <<EOF
bind 0.0.0.0
port $P1
dir /data
announce-host c1
cluster-enable yes
cluster-as-seed yes
raft-listen-address c1:$R1
save ""
appendonly no
EOF
cat > "$WORK/n2.conf" <<EOF
bind 0.0.0.0
port $P2
dir /data
announce-host c2
cluster-enable yes
cluster-seed c1:$P1
raft-listen-address c2:$R2
save ""
appendonly no
EOF

docker run -d --name "$N1" --hostname c1 --network-alias c1 --network "$NET" \
  -v "$(mount_path "$WORK/n1.conf"):/etc/godis/godis.conf:ro" godis:bench >/dev/null
sleep 2
docker run -d --name "$N2" --hostname c2 --network-alias c2 --network "$NET" \
  -v "$(mount_path "$WORK/n2.conf"):/etc/godis/godis.conf:ro" godis:bench >/dev/null

CLI() { docker run --rm --network "$NET" redis:8.10-alpine redis-cli -h "$1" -p "$2" "${@:3}"; }

for i in $(seq 1 50); do
  sleep 0.3
  CLI "$N1" "$P1" PING >/dev/null 2>&1 && CLI "$N2" "$P2" PING >/dev/null 2>&1 && break
  [[ $i == 50 ]] && { echo "nodes did not come up"; docker logs "$N1" 2>&1 | tail -5; docker logs "$N2" 2>&1 | tail -5; exit 1; }
done

# split the ring: node1 keeps lower half, node2 claims the upper half
CLI "$N1" "$P1" CLUSTER DELSLOTSRANGE 8192 16383 >/dev/null
for i in $(seq 1 100); do
  sleep 0.3
  CLI "$N2" "$P2" CLUSTER ADDSLOTSRANGE 8192 16383 >/dev/null 2>&1 && break
done

converged=0
for i in $(seq 1 100); do
  sleep 0.3
  s1=$(CLI "$N1" "$P1" CLUSTER SLOTS | tr '\n' ' ')
  s2=$(CLI "$N2" "$P2" CLUSTER SLOTS | tr '\n' ' ')
  if [[ "$s1" == "$s2" && "$s1" == *"$P2"* ]]; then converged=1; break; fi
done
echo "== CLUSTER SLOTS (node1) =="; CLI "$N1" "$P1" CLUSTER SLOTS
echo "== CLUSTER SLOTS (node2) =="; CLI "$N2" "$P2" CLUSTER SLOTS
if (( converged == 0 )); then
  echo "TOPOLOGY DIVERGENCE: node views differ or upper half missing (open E7 issue)"
  exit 1
fi

echo "== redis-cli -c MOVED redirect =="
key=""
for k in interop1 interop2 interop3 interop4 interop5 interop6 interop7 interop8; do
  slot=$(CLI "$N1" "$P1" CLUSTER KEYSLOT "$k" | tr -d '\r')
  if (( slot >= 8192 )); then key="$k"; break; fi
done
[[ -n "$key" ]] || { echo "no upper-half key found"; exit 1; }
CLI "$N1" "$P1" -c SET "$key" hello-from-cli
echo -n "direct read from node2: "
CLI "$N2" "$P2" GET "$key"
echo -n "redirect read via node1 -c: "
CLI "$N1" "$P1" -c GET "$key"

echo "== CLUSTER INFO (node1) =="
CLI "$N1" "$P1" CLUSTER INFO | head -4
echo "cluster interop smoke OK"
