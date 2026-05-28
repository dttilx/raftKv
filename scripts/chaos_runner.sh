#!/usr/bin/env bash
# =============================================================================
# chaos_runner.sh — 混沌场景编排 + 一致性校验 + JSON 报告
# 默认流程：
#   1) cluster_up
#   2) consistency baseline
#   3) 注入故障
#   4) consistency after
#   5) recover（如需要）
#   6) 输出 report.json
# =============================================================================
set -euo pipefail
source "$(dirname "$0")/common.sh"

SCENARIO="${RAFTKV_CHAOS_SCENARIO:-kill-restart}"
ROUNDS="${RAFTKV_CHAOS_ROUNDS:-1}"
CONSISTENCY_KEYS="${RAFTKV_CONSISTENCY_KEYS:-400}"
CONSISTENCY_ROUNDS="${RAFTKV_CONSISTENCY_ROUNDS:-1}"
NODE_INDEX="${RAFTKV_CHAOS_NODE:-1}"
SLEEP_SECS="${RAFTKV_CHAOS_SLEEP:-6}"
REPORT_PATH="${RAFTKV_CHAOS_REPORT:-logs/chaos_report.json}"
SUMMARY_PATH="${RAFTKV_CHAOS_SUMMARY:-logs/chaos_summary.md}"

usage() {
  cat <<'EOF'
Usage: chaos_runner.sh [options]

Options:
  --scenario <kill-restart|partition|delay-loss>
  --rounds <N>
  --keys <N>
  --check-rounds <N>
  --node <idx>
  --sleep <secs>
  --report <path>
  --summary <path>
  -h, --help
EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --scenario) SCENARIO="${2:-}"; shift 2 ;;
      --rounds) ROUNDS="${2:-}"; shift 2 ;;
      --keys) CONSISTENCY_KEYS="${2:-}"; shift 2 ;;
      --check-rounds) CONSISTENCY_ROUNDS="${2:-}"; shift 2 ;;
      --node) NODE_INDEX="${2:-}"; shift 2 ;;
      --sleep) SLEEP_SECS="${2:-}"; shift 2 ;;
      --report) REPORT_PATH="${2:-}"; shift 2 ;;
      --summary) SUMMARY_PATH="${2:-}"; shift 2 ;;
      -h|--help) usage; exit 0 ;;
      *) echo "unknown arg: $1" >&2; usage; exit 2 ;;
    esac
  done
}

json_escape() {
  local s="$1"
  s="${s//\\/\\\\}"
  s="${s//\"/\\\"}"
  s="${s//$'\n'/\\n}"
  printf '%s' "$s"
}

must_have_bin() {
  if [[ ! -x "$RAFTKV_ROOT/bin/kv_consistency" ]]; then
    echo "bin/kv_consistency not found; build first" >&2
    exit 1
  fi
}

run_consistency() {
  local tag="$1"
  local out_file="$2"
  set +e
  "$RAFTKV_ROOT/bin/kv_consistency" -c "$CONF" -k "$CONSISTENCY_KEYS" -r "$CONSISTENCY_ROUNDS" \
    >"$out_file" 2>&1
  local rc=$?
  set -e
  echo "$rc"
}

inject_fault() {
  local round="$1"
  case "$SCENARIO" in
    kill-restart)
      "$(dirname "$0")/fault_inject.sh" \
        --scenario kill-restart --node "$NODE_INDEX" --sleep "$SLEEP_SECS" --conf "$CONF"
      ;;
    partition)
      "$(dirname "$0")/fault_inject.sh" --scenario partition --action inject --node "$NODE_INDEX" --conf "$CONF"
      sleep "$SLEEP_SECS"
      "$(dirname "$0")/fault_inject.sh" --scenario partition --action recover --node "$NODE_INDEX" --conf "$CONF"
      ;;
    delay-loss)
      "$(dirname "$0")/fault_inject.sh" --scenario delay-loss --action inject
      sleep "$SLEEP_SECS"
      "$(dirname "$0")/fault_inject.sh" --scenario delay-loss --action recover
      ;;
    *)
      echo "unsupported scenario: $SCENARIO" >&2
      exit 2
      ;;
  esac
  echo "round=$round scenario=$SCENARIO injected"
}

main() {
  parse_args "$@"
  must_have_bin
  mkdir -p "$LOG_DIR"
  mkdir -p "$(dirname "$REPORT_PATH")"
  mkdir -p "$(dirname "$SUMMARY_PATH")"

  export RAFTKV_CLEAN_PERSIST=1
  "$(dirname "$0")/cluster_up.sh"

  local started_at ended_at total=0 pass=0 fail=0
  started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  local rounds_json=""

  for ((i = 1; i <= ROUNDS; ++i)); do
    total=$((total + 1))
    base_file="${LOG_DIR}/chaos_round_${i}_baseline.log"
    post_file="${LOG_DIR}/chaos_round_${i}_post.log"

    base_rc="$(run_consistency "baseline" "$base_file")"
    if [[ "$base_rc" -ne 0 ]]; then
      fail=$((fail + 1))
      rounds_json+="{\"round\":$i,\"baseline_rc\":$base_rc,\"fault_rc\":-1,\"post_rc\":-1,\"ok\":false,\"reason\":\"baseline_failed\"},"
      continue
    fi

    fault_rc=0
    set +e
    inject_fault "$i"
    fault_rc=$?
    set -e
    if [[ "$fault_rc" -ne 0 ]]; then
      fail=$((fail + 1))
      rounds_json+="{\"round\":$i,\"baseline_rc\":0,\"fault_rc\":$fault_rc,\"post_rc\":-1,\"ok\":false,\"reason\":\"fault_inject_failed\"},"
      continue
    fi

    post_rc="$(run_consistency "post" "$post_file")"
    if [[ "$post_rc" -eq 0 ]]; then
      pass=$((pass + 1))
      rounds_json+="{\"round\":$i,\"baseline_rc\":0,\"fault_rc\":0,\"post_rc\":0,\"ok\":true},"
    else
      fail=$((fail + 1))
      rounds_json+="{\"round\":$i,\"baseline_rc\":0,\"fault_rc\":0,\"post_rc\":$post_rc,\"ok\":false,\"reason\":\"post_check_failed\"},"
    fi
  done

  ended_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  rounds_json="[${rounds_json%,}]"
  cat >"$REPORT_PATH" <<EOF
{
  "startedAt": "$(json_escape "$started_at")",
  "endedAt": "$(json_escape "$ended_at")",
  "scenario": "$(json_escape "$SCENARIO")",
  "totalRounds": $total,
  "passRounds": $pass,
  "failRounds": $fail,
  "consistencyKeys": $CONSISTENCY_KEYS,
  "consistencyRounds": $CONSISTENCY_ROUNDS,
  "nodeIndex": $NODE_INDEX,
  "faultSleepSeconds": $SLEEP_SECS,
  "rounds": $rounds_json
}
EOF

  cat >"$SUMMARY_PATH" <<EOF
# Chaos Testing Summary

- startedAt: $started_at
- endedAt: $ended_at
- scenario: $SCENARIO
- totalRounds: $total
- passRounds: $pass
- failRounds: $fail
- consistencyKeys: $CONSISTENCY_KEYS
- consistencyRounds: $CONSISTENCY_ROUNDS
- nodeIndex: $NODE_INDEX
- faultSleepSeconds: $SLEEP_SECS
- reportJson: $REPORT_PATH

## Round Results

| round | baseline_rc | fault_rc | post_rc | ok | reason |
|------:|------------:|---------:|--------:|:--:|:-------|
EOF

  for ((i = 1; i <= ROUNDS; ++i)); do
    round_item="$(echo "$rounds_json" | sed 's/^\[//; s/\]$//' | tr '}' '\n' | grep "\"round\":$i" | head -n1 || true)"
    if [[ -z "$round_item" ]]; then
      echo "| $i | -1 | -1 | -1 | FAIL | missing_round_data |" >>"$SUMMARY_PATH"
      continue
    fi
    baseline_rc="$(echo "$round_item" | sed -n 's/.*"baseline_rc":\(-\?[0-9]\+\).*/\1/p')"
    fault_rc="$(echo "$round_item" | sed -n 's/.*"fault_rc":\(-\?[0-9]\+\).*/\1/p')"
    post_rc="$(echo "$round_item" | sed -n 's/.*"post_rc":\(-\?[0-9]\+\).*/\1/p')"
    ok_flag="$(echo "$round_item" | sed -n 's/.*"ok":\([^,}]*\).*/\1/p')"
    reason="$(echo "$round_item" | sed -n 's/.*"reason":"\([^"]*\)".*/\1/p')"
    if [[ "$ok_flag" == "true" ]]; then
      ok_txt="OK"
      reason="${reason:-success}"
    else
      ok_txt="FAIL"
      reason="${reason:-unknown}"
    fi
    echo "| $i | ${baseline_rc:--1} | ${fault_rc:--1} | ${post_rc:--1} | $ok_txt | $reason |" >>"$SUMMARY_PATH"
  done

  if [[ "$fail" -gt 0 ]]; then
    echo "chaos_runner FAIL rounds=$total pass=$pass fail=$fail report=$REPORT_PATH summary=$SUMMARY_PATH" >&2
    "$(dirname "$0")/cluster_down.sh" || true
    exit 1
  fi
  echo "chaos_runner OK rounds=$total pass=$pass report=$REPORT_PATH summary=$SUMMARY_PATH"
  "$(dirname "$0")/cluster_down.sh"
}

main "$@"
