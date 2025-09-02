# BNB链自动空投发送程序

这是一个在Ubuntu Server上运行的Python程序，用于定期扫描MariaDB数据库中的airdrop表，并自动在BNB链上发送代币空投。程序包含完善的错误处理和状态管理机制，确保空投过程可靠可控。

## 功能特点

- 定期扫描数据库中的待发送空投记录
- 自动在BNB链上发送代币到指定地址
- 实时更新空投状态（待发送、已发送、已确认）
- 余额检查机制，确保有足够的Gas和代币
- 多RPC节点自动切换，提高稳定性
- 动态调整扫描间隔，根据任务量优化性能
- 失败重试机制，支持最大重试次数限制
- 错误记录功能，方便管理员处理异常情况
- 系统服务集成，支持服务器重启后自动运行

## 工作流程

1. 程序启动后，加载配置文件并初始化日志系统
2. 定期扫描数据库中的airdrop表，查找待处理记录
3. 根据待处理记录数量动态调整下次扫描间隔
4. 检查发送账户的Gas和代币余额是否充足
5. 连接到BNB链节点（自动切换可用节点）
6. 对每条待处理记录执行以下操作：
   - 发送代币到指定地址
   - 记录交易哈希并更新状态为"已发送"
   - 监控交易确认情况，确认后更新状态为"已确认"
7. 处理发送失败的记录：
   - 自动重试发送
   - 达到最大重试次数后记录到错误表
8. 持续循环执行上述流程

## 数据库表结构

### airdrop表（需提前创建）
CREATE TABLE airdrop (
    id INT AUTO_INCREMENT PRIMARY KEY,
    userid INT NOT NULL,
    address VARCHAR(255) NOT NULL,
    amount DECIMAL(18, 8) NOT NULL,
    aflag TINYINT DEFAULT 0,  -- 0: 待发送, 1: 已发送, 2: 已确认, 3: 失败
    stime DATETIME NULL,
    retry INT DEFAULT 0,
    tx_hash VARCHAR(255) NULL
);
### airdrop_errors表（程序自动创建）

用于记录达到最大重试次数仍失败的空投记录，方便管理员查看和处理。

## 文件说明

1. **airdrop_sender.py**：主程序文件，实现核心功能
   - 数据库连接和操作
   - 区块链交互和交易处理
   - 状态管理和错误处理
   - 动态扫描间隔调整

2. **config.yaml.example**：配置文件示例
   - 数据库连接信息
   - 钱包私钥和地址
   - 代币合约信息
   - RPC节点列表
   - 程序运行参数

3. **start.sh**：启动脚本
   - 检查依赖和配置
   - 在后台启动程序
   - 显示进程ID

4. **stop.sh**：停止脚本
   - 查找程序进程
   - 终止程序运行

5. **requirements.txt**：项目依赖列表
   - 包含所有需要安装的Python包

6. **airdrop.service**：系统服务配置文件
   - 用于将程序注册为系统服务
   - 支持自动启动和故障重启

## 安装与使用

### 前置条件

- Ubuntu Server（推荐18.04或更高版本）
- Python 3.7或更高版本
- MariaDB数据库
- 已创建airdrop表

### 安装步骤

1. 克隆代码库到服务器：
   ```bash
   git clone <repository-url>
   cd bnb-airdrop-sender
   ```

2. 复制配置文件并编辑：
   ```bash
   cp config.yaml.example config.yaml
   nano config.yaml  # 或使用其他编辑器
   ```
   请确保正确配置以下信息：
   - 数据库连接信息
   - 钱包私钥和地址（确保该账户有足够的BNB和代币）
   - 代币合约地址和ABI

3. 安装依赖：
   ```bash
   pip3 install -r requirements.txt
   ```

4. 测试运行程序：
   ```bash
   python3 airdrop_sender.py
   ```
   确认程序能正常启动且没有错误后，按Ctrl+C停止。

5. 使用脚本在后台启动：
   ```bash
   chmod +x start.sh stop.sh
   ./start.sh
   ```

### 配置为系统服务（推荐）

1. 编辑服务文件，修改路径为实际程序目录：
   ```bash
   nano airdrop.service
   ```

2. 复制服务文件到系统服务目录：
   ```bash
   sudo cp airdrop.service /etc/systemd/system/
   ```

3. 启用并启动服务：
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable airdrop
   sudo systemctl start airdrop
   ```

4. 检查服务状态：
   ```bash
   sudo systemctl status airdrop
   ```

## 日志与监控

- 程序日志存储在`logs/`目录下，按日期命名
- 可通过以下命令查看系统服务日志：
  ```bash
  journalctl -u airdrop -f
  ```
- 错误记录会保存到数据库的`airdrop_errors`表中

## 注意事项

1. 私钥安全：配置文件中的私钥是敏感信息，请确保文件权限正确（仅所有者可读写）
2. 测试建议：首次使用时建议先在BSC测试网（chain id 97）进行测试
3. 余额监控：定期检查发送账户的余额，确保有足够的Gas和代币
4. 备份：定期备份数据库，防止重要数据丢失
5. 升级：更新程序前请先停止服务，更新后再启动

## 状态码说明

- `0`：待发送 - 记录已创建但尚未处理
- `1`：已发送 - 交易已发出但尚未确认
- `2`：已确认 - 交易已在链上确认
- `3`：失败 - 达到最大重试次数仍未发送成功
    