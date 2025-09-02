#!/bin/bash
# 停止脚本，用于终止后台运行的程序

# 查找程序进程ID
PID=$(ps aux | grep 'airdrop_sender.py' | grep -v grep | awk '{print $2}')

if [ -z "$PID" ]; then
    echo "程序未在运行"
else
    echo "正在停止进程 $PID..."
    kill $PID
    # 等待进程终止
    sleep 2
    # 检查进程是否已终止
    if ps -p $PID > /dev/null; then
        echo "强制终止进程 $PID..."
        kill -9 $PID
    fi
    echo "程序已停止"
fi
    