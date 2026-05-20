#!/usr/bin/env bash
set -euo pipefail

# Create a simple 2-1 network partition on localhost ports from conf.
# This script relies on iptables (Linux) and assumes nodes run on 127.0.0.1.
#
# Usage:
#   sudo ./net_partition_2_1.sh apply  test.conf 0 1 2
#   sudo ./net_partition_2_1.sh clear  test.conf
#
# Partition: groupA={0,1}, groupB={2}. Drop traffic between groups.

ACTION="${1:-}"
CONF_FILE="${2:-test.conf}"

if [[ "$ACTION" != "apply" && "$ACTION" != "clear" ]]; then
  echo "Usage: sudo $0 {apply|clear} <conf_file> [a1 a2 b1]"
  exit 2
fi

get_port() {
  local idx="$1"
  # config file format: node0ip=... node0port=...
  awk -F= -v k="node${idx}port" '$1==k{print $2}' "$CONF_FILE"
}

if [[ "$ACTION" == "clear" ]]; then
  iptables -D INPUT  -m comment --comment "raft_partition" -j DROP 2>/dev/null || true
  iptables -D OUTPUT -m comment --comment "raft_partition" -j DROP 2>/dev/null || true
  echo "cleared partition rules"
  exit 0
fi

A1="${3:-0}"
A2="${4:-1}"
B1="${5:-2}"

PA1="$(get_port "$A1")"
PA2="$(get_port "$A2")"
PB1="$(get_port "$B1")"

if [[ -z "$PA1" || -z "$PA2" || -z "$PB1" ]]; then
  echo "failed to parse ports from $CONF_FILE"
  exit 1
fi

echo "apply partition: A={${A1},${A2}} B={${B1}}"
echo "ports: A=($PA1,$PA2) B=($PB1)"

# Drop traffic between A and B (both directions)
iptables -I OUTPUT -p tcp -d 127.0.0.1 --dport "$PB1" -m comment --comment "raft_partition" -j DROP
iptables -I OUTPUT -p tcp -d 127.0.0.1 --dport "$PA1" -m comment --comment "raft_partition" -j DROP
iptables -I OUTPUT -p tcp -d 127.0.0.1 --dport "$PA2" -m comment --comment "raft_partition" -j DROP

iptables -I INPUT -p tcp -s 127.0.0.1 --sport "$PB1" -m comment --comment "raft_partition" -j DROP
iptables -I INPUT -p tcp -s 127.0.0.1 --sport "$PA1" -m comment --comment "raft_partition" -j DROP
iptables -I INPUT -p tcp -s 127.0.0.1 --sport "$PA2" -m comment --comment "raft_partition" -j DROP

echo "partition rules installed (comment=raft_partition)"

