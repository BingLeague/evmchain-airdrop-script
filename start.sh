#!/bin/bash
# 启动脚本，在后台运行空投发送程序

# 检查Python是否安装
if ! command -v python3 &> /dev/null
then
    echo "Python3 未安装，请先安装Python3"
    exit 1
fi

# 检查配置文件是否存在
if [ ! -f "config.yaml" ]; then
    if [ -f "config.yaml.example" ]; then
        cp config.yaml.example config.yaml
        echo "已创建默认配置文件 config.yaml，请编辑后重新运行"
        exit 1
    else
        echo "配置文件 config.yaml 不存在"
        exit 1
    fi
fi

# 检查依赖是否安装
if ! python3 -c "import web3" &> /dev/null; then
    echo "正在安装依赖包..."
    pip3 install -r requirements.txt
fi

# 启动程序并在后台运行
echo "启动空投发送程序..."
nohup python3 airdrop_sender.py > /dev/null 2>&1 &

# 显示进程ID
PID=$!
echo "程序已启动，进程ID: $PID"
echo "日志文件位于 logs/ 目录下"
    