#!/usr/bin/env bash
set -euo pipefail

LAB_NET="kvnet"
JAEGER_NODE="jaeger"
JAEGER_IP="10.0.0.211"
JAEGER_MASK="24"
JAEGER_IFACE="eth1"
COLLECTOR_NODE="otelcol"

echo "[1/7] Start Jaeger container only..."
kathara lstart "${JAEGER_NODE}"

echo "[2/7] Attach Jaeger to ${LAB_NET}..."
kathara lconfig -n "${JAEGER_NODE}" --add "${LAB_NET}"

echo "[3/7] Wait for ${JAEGER_IFACE}..."
for i in $(seq 1 10); do
  if kathara exec "${JAEGER_NODE}" -- sh -lc "ip link show ${JAEGER_IFACE} >/dev/null 2>&1"; then
    break
  fi
  sleep 1
done

echo "[4/7] Configure ${JAEGER_IFACE}..."
kathara exec "${JAEGER_NODE}" -- sh -lc "
  ip addr add ${JAEGER_IP}/${JAEGER_MASK} dev ${JAEGER_IFACE} 2>/dev/null || true
  ip link set ${JAEGER_IFACE} up
"

echo "[5/7] Wait for Jaeger..."
for i in $(seq 1 20); do
  if kathara exec "${JAEGER_NODE}" -- sh -lc "ss -lnt | grep -q ':16686'"; then
    echo 'Jaeger is ready.'
    break
  fi
  sleep 1
done

echo "[6/7] Start collector..."
kathara lstart "${COLLECTOR_NODE}"

echo "[7/7] Start remaining nodes..."
kathara lstart --exclude "${JAEGER_NODE}" "${COLLECTOR_NODE}"

echo "Jaeger UI: http://localhost:16686"