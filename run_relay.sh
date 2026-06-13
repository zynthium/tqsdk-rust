#!/bin/bash
set -e

echo "Starting tqsdk-relay..."
export TQSDK_RELAY_DOWNSTREAM_LISTEN="127.0.0.1:7788"
export TQSDK_RELAY_METRICS_LISTEN="127.0.0.1:7789"
export TQSDK_RELAY_FUTURES_UNIVERSE="symbol:KQ.m@SHFE.au"

cargo run -p tqsdk-relay --features server
