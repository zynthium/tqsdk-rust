#!/usr/bin/env bash
#
# 这是一个启动 tqsdk-relay 的参考脚本。
# Relay 需要通过设置环境变量来进行配置。

# 【必须】设置您的快期账户和密码，Relay 需要使用此账号连接上游服务器
export TQ_AUTH_USER="您的快期账号"
export TQ_AUTH_PASS="您的快期密码"

# 【可选】设置 Relay 监听的 WebSocket 端口（供下游客户端如 tqsdk 订阅，默认 127.0.0.1:7788）
export TQSDK_RELAY_DOWNSTREAM_LISTEN="127.0.0.1:7788"

# 【可选】设置 Relay 的 Metrics 监控面板端口（默认 127.0.0.1:7789）
export TQSDK_RELAY_METRICS_LISTEN="127.0.0.1:7789"

# 【可选】设置预启动订阅的品种列表（如果不填，Relay 会根据下游客户端请求按需订阅）
# export TQSDK_RELAY_FUTURES_SYMBOLS="SHFE.au2608,SHFE.ag2608,SHFE.rb2410"

# 【可选】仅在终端预演 Relay 参数是否正确配置，不会真实启动网络连接
# export TQSDK_RELAY_DRY_RUN="true"

echo "==========================================================="
echo "启动 tqsdk-relay..."
echo "下游 WebSocket 服务地址: ws://${TQSDK_RELAY_DOWNSTREAM_LISTEN}"
echo "监控面板 HTTP 服务地址: http://${TQSDK_RELAY_METRICS_LISTEN}"
echo "==========================================================="

# 使用 cargo run 启动 tqsdk-relay (在开发环境中)
# 在生产环境中，您可以直接执行编译好的二进制文件： `./tqsdk-relay`
TQSDK_RELAY_FUTURES_PRODUCTS="ALL" \
TQSDK_RELAY_FUTURES_MAIN_ONLY=true \
TQSDK_RELAY_UPSTREAM_TICK_VIEW_WIDTH=1 \
cargo run -p tqsdk-relay --bin tqsdk-relay
