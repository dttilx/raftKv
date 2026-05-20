#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

WAIT_SECS="${RAFTKV_WAIT_SECS:-30}"
ELECTION_PAUSE="${RAFTKV_ELECTION_PAUSE:-3}"

if [[ ! -f "$CONF" ]]; then
  echo "config not found: $CONF" >&2
  exit 1
fi

read_ports_from_conf "$CONF"

tcp_probe() {
  local host="$1"
  local port="$2"
  if command -v nc >/dev/null 2>&1; then
    nc -z "$host" "$port" 2>/dev/null
    return $?
  fi
  (echo >/dev/tcp/"$host"/"$port") >/dev/null 2>&1
}

deadline=$((SECONDS + WAIT_SECS))
while ((SECONDS < deadline)); do
  all_ok=1
  for port in "${PORTS[@]}"; do
    if ! tcp_probe 127.0.0.1 "$port"; then
      all_ok=0
      break
    fi
  done
  if [[ $all_ok -eq 1 ]]; then
    echo "all ${#PORTS[@]} ports listening: ${PORTS[*]}"
    sleep "$ELECTION_PAUSE"
    exit 0
  fi
  sleep 0.2
done

echo "timeout after ${WAIT_SECS}s; ports not ready: ${PORTS[*]}" >&2
exit 1
