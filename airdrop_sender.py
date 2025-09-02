import os
import time
import logging
import yaml
import random
from datetime import datetime
from web3 import Web3, exceptions
from web3.middleware import geth_poa_middleware
import mysql.connector
from mysql.connector import Error
from decimal import Decimal

# 配置日志
def setup_logger():
    logger = logging.getLogger('airdrop_sender')
    logger.setLevel(logging.INFO)
    
    # 确保日志目录存在
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    # 创建文件处理器和控制台处理器
    file_handler = logging.FileHandler(f'logs/airdrop_{datetime.now().strftime("%Y%m%d")}.log')
    console_handler = logging.StreamHandler()
    
    # 定义日志格式
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # 添加处理器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

# 加载配置
def load_config():
    try:
        with open('config.yaml', 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"加载配置文件失败: {str(e)}")
        raise

# 数据库连接
def get_db_connection(config):
    try:
        connection = mysql.connector.connect(
            host=config['database']['host'],
            database=config['database']['name'],
            user=config['database']['user'],
            password=config['database']['password']
        )
        if connection.is_connected():
            return connection
    except Error as e:
        logger.error(f"数据库连接失败: {str(e)}")
        return None

# 初始化错误表
def init_error_table(connection):
    if not connection:
        return
    
    cursor = connection.cursor()
    try:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS airdrop_errors (
            id INT AUTO_INCREMENT PRIMARY KEY,
            airdrop_id INT NOT NULL,
            user_id INT,
            address VARCHAR(255),
            amount DECIMAL(18, 8),
            error_message TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (airdrop_id) REFERENCES airdrop(id)
        )
        """)
        connection.commit()
    except Error as e:
        logger.error(f"初始化错误表失败: {str(e)}")
    finally:
        cursor.close()

# 获取Web3连接
def get_web3_connection(rpc_nodes, current_rpc_index=None):
    if current_rpc_index is not None:
        # 尝试使用指定的RPC节点
        try:
            w3 = Web3(Web3.HTTPProvider(rpc_nodes[current_rpc_index]))
            if w3.is_connected():
                w3.middleware_onion.inject(geth_poa_middleware, layer=0)
                return w3, current_rpc_index
        except Exception as e:
            logger.warning(f"RPC节点 {rpc_nodes[current_rpc_index]} 连接失败: {str(e)}")
    
    # 尝试所有RPC节点
    for i, rpc in enumerate(rpc_nodes):
        try:
            w3 = Web3(Web3.HTTPProvider(rpc))
            if w3.is_connected():
                w3.middleware_onion.inject(geth_poa_middleware, layer=0)
                logger.info(f"成功连接到RPC节点: {rpc}")
                return w3, i
        except Exception as e:
            logger.warning(f"RPC节点 {rpc} 连接失败: {str(e)}")
    
    logger.error("所有RPC节点都无法连接")
    return None, -1

# 检查余额是否足够
def check_balances(w3, config, amount_needed):
    try:
        sender_address = Web3.to_checksum_address(config['wallet']['address'])
        
        # 检查BNB余额（用于Gas）
        bnb_balance = w3.eth.get_balance(sender_address)
        bnb_balance_eth = w3.from_wei(bnb_balance, 'ether')
        
        # 检查代币余额
        token_contract = w3.eth.contract(
            address=Web3.to_checksum_address(config['token']['contract_address']),
            abi=config['token']['abi']
        )
        token_balance = token_contract.functions.balanceOf(sender_address).call()
        token_balance_decimal = token_balance / (10 ** config['token']['decimals'])
        
        logger.info(f"当前BNB余额: {bnb_balance_eth} BNB")
        logger.info(f"当前代币余额: {token_balance_decimal} {config['token']['symbol']}")
        
        # 检查Gas是否足够（预估每笔交易需要0.001 BNB）
        gas_needed = config['batch_size'] * 0.001
        if bnb_balance_eth < gas_needed:
            logger.error(f"BNB余额不足，需要至少 {gas_needed} BNB 用于Gas")
            return False
        
        # 检查代币是否足够
        if token_balance_decimal < amount_needed:
            logger.error(f"代币余额不足，需要 {amount_needed} {config['token']['symbol']}，但只有 {token_balance_decimal}")
            return False
            
        return True
    except Exception as e:
        logger.error(f"余额检查失败: {str(e)}")
        return False

# 获取待处理的空投记录
def get_pending_airdrops(connection, batch_size):
    if not connection:
        return []
    
    cursor = connection.cursor(dictionary=True)
    try:
        # 查询状态为0（待发送）或状态为1（已发送但未确认）且需要重试的记录
        cursor.execute("""
        SELECT * FROM airdrop 
        WHERE (aflag = 0 OR (aflag = 1 AND retry < %s))
        ORDER BY aflag ASC, retry ASC, id ASC
        LIMIT %s
        """, (config['max_retries'], batch_size))
        return cursor.fetchall()
    except Error as e:
        logger.error(f"查询待处理空投记录失败: {str(e)}")
        return []
    finally:
        cursor.close()

# 发送代币
def send_token(w3, config, recipient, amount):
    try:
        sender_address = Web3.to_checksum_address(config['wallet']['address'])
        private_key = config['wallet']['private_key']
        token_contract_address = Web3.to_checksum_address(config['token']['contract_address'])
        
        # 创建合约实例
        token_contract = w3.eth.contract(
            address=token_contract_address,
            abi=config['token']['abi']
        )
        
        # 转换金额为最小单位
        amount_wei = int(amount * (10 ** config['token']['decimals']))
        
        # 构建交易
        nonce = w3.eth.get_transaction_count(sender_address)
        tx = token_contract.functions.transfer(
            Web3.to_checksum_address(recipient),
            amount_wei
        ).build_transaction({
            'from': sender_address,
            'nonce': nonce,
            'gas': config['gas']['limit'],
            'gasPrice': w3.to_wei(config['gas']['price'], 'gwei'),
            'chainId': config['chain']['id']
        })
        
        # 签名交易
        signed_tx = w3.eth.account.sign_transaction(tx, private_key)
        
        # 发送交易
        tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        
        # 返回交易哈希
        return Web3.to_hex(tx_hash)
        
    except exceptions.InsufficientFunds:
        logger.error("发送交易失败：余额不足")
        return None
    except exceptions.InvalidAddress:
        logger.error(f"发送交易失败：无效地址 {recipient}")
        return None
    except Exception as e:
        logger.error(f"发送交易失败: {str(e)}")
        return None

# 检查交易状态
def check_transaction_status(w3, tx_hash, confirmations_needed=1):
    try:
        tx_receipt = w3.eth.get_transaction_receipt(tx_hash)
        if tx_receipt.status == 1:
            # 交易成功，检查确认数
            current_block = w3.eth.block_number
            confirmations = current_block - tx_receipt.blockNumber
            if confirmations >= confirmations_needed:
                return "success", confirmations
            else:
                return "pending", confirmations
        else:
            return "failed", 0
    except exceptions.TransactionNotFound:
        return "not_found", 0
    except Exception as e:
        logger.error(f"检查交易状态失败: {str(e)}")
        return "error", 0

# 更新空投记录状态
def update_airdrop_status(connection, airdrop_id, status, tx_hash=None, retry_count=None):
    if not connection:
        return False
    
    cursor = connection.cursor()
    try:
        update_fields = ["aflag = %s", "stime = CURRENT_TIMESTAMP"]
        params = [status, airdrop_id]
        
        if tx_hash:
            update_fields.append("tx_hash = %s")
            params.insert(1, tx_hash)
            
        if retry_count is not None:
            update_fields.append("retry = %s")
            params.insert(1, retry_count)
        
        query = f"UPDATE airdrop SET {', '.join(update_fields)} WHERE id = %s"
        cursor.execute(query, params)
        connection.commit()
        return True
    except Error as e:
        logger.error(f"更新空投状态失败: {str(e)}")
        connection.rollback()
        return False
    finally:
        cursor.close()

# 记录错误
def log_error(connection, airdrop):
    if not connection:
        return False
    
    cursor = connection.cursor()
    try:
        cursor.execute("""
        INSERT INTO airdrop_errors 
        (airdrop_id, user_id, address, amount, error_message)
        VALUES (%s, %s, %s, %s, %s)
        """, (
            airdrop['id'],
            airdrop['userid'],
            airdrop['address'],
            airdrop['amount'],
            f"达到最大重试次数 {config['max_retries']}"
        ))
        connection.commit()
        return True
    except Error as e:
        logger.error(f"记录错误失败: {str(e)}")
        connection.rollback()
        return False
    finally:
        cursor.close()

# 处理已发送但未确认的交易
def process_pending_transactions(connection, w3):
    if not connection or not w3:
        return
    
    cursor = connection.cursor(dictionary=True)
    try:
        # 查询状态为1（已发送但未确认）的记录
        cursor.execute("""
        SELECT * FROM airdrop 
        WHERE aflag = 1 AND tx_hash IS NOT NULL
        """)
        pending_transactions = cursor.fetchall()
        
        for tx in pending_transactions:
            status, confirmations = check_transaction_status(
                w3, 
                tx['tx_hash'],
                config['required_confirmations']
            )
            
            if status == "success":
                logger.info(f"交易 {tx['tx_hash']} 已确认，确认数: {confirmations}")
                update_airdrop_status(connection, tx['id'], 2)
            elif status == "failed":
                logger.warning(f"交易 {tx['tx_hash']} 失败，将重试")
                # 增加重试次数但保持状态为1，等待下一轮处理
                update_airdrop_status(
                    connection, 
                    tx['id'], 
                    1, 
                    retry_count=tx['retry'] + 1
                )
            elif status == "not_found" and tx['retry'] < config['max_retries']:
                logger.warning(f"交易 {tx['tx_hash']} 未找到，将重试")
                update_airdrop_status(
                    connection, 
                    tx['id'], 
                    1, 
                    retry_count=tx['retry'] + 1
                )
                
    except Error as e:
        logger.error(f"处理未确认交易失败: {str(e)}")
    finally:
        cursor.close()

# 主处理函数
def process_airdrops():
    global config, logger
    
    # 初始化变量
    current_rpc_index = None
    scan_interval = config['initial_scan_interval']
    
    while True:
        try:
            # 连接数据库
            db_connection = get_db_connection(config)
            if not db_connection:
                time.sleep(scan_interval)
                continue
            
            # 初始化错误表（首次运行时）
            init_error_table(db_connection)
            
            # 获取待处理的空投记录
            pending_airdrops = get_pending_airdrops(db_connection, config['batch_size'])
            logger.info(f"发现 {len(pending_airdrops)} 条待处理的空投记录")
            
            # 根据待处理数量调整扫描间隔
            if len(pending_airdrops) == 0:
                # 没有待处理记录，延长扫描间隔
                scan_interval = min(
                    scan_interval + config['interval_adjustment'], 
                    config['max_scan_interval']
                )
                logger.info(f"没有待处理记录，下次扫描间隔调整为 {scan_interval} 秒")
            else:
                # 有待处理记录，缩短扫描间隔
                scan_interval = max(
                    scan_interval - config['interval_adjustment'], 
                    config['min_scan_interval']
                )
                logger.info(f"有待处理记录，下次扫描间隔调整为 {scan_interval} 秒")
            
            # 如果没有待处理记录，直接进入下一轮
            if not pending_airdrops:
                db_connection.close()
                time.sleep(scan_interval)
                continue
            
            # 计算所需总代币数量
            total_amount = sum(Decimal(str(airdrop['amount'])) for airdrop in pending_airdrops)
            
            # 连接到区块链节点
            w3, current_rpc_index = get_web3_connection(
                config['rpc_nodes'], 
                current_rpc_index
            )
            if not w3:
                logger.error("无法连接到任何区块链节点，将在下次尝试")
                db_connection.close()
                time.sleep(scan_interval)
                continue
            
            # 检查余额
            if not check_balances(w3, config, float(total_amount)):
                logger.error("余额不足，无法处理空投，将在下次尝试")
                db_connection.close()
                time.sleep(scan_interval)
                continue
            
            # 处理待发送的空投
            for airdrop in pending_airdrops:
                try:
                    logger.info(f"处理空投记录 ID: {airdrop['id']}, 地址: {airdrop['address']}, 数量: {airdrop['amount']}")
                    
                    # 发送代币
                    tx_hash = send_token(
                        w3, 
                        config, 
                        airdrop['address'], 
                        float(airdrop['amount'])
                    )
                    
                    if tx_hash:
                        logger.info(f"交易已发送，哈希: {tx_hash}")
                        # 更新状态为1（已发送）
                        update_airdrop_status(
                            db_connection, 
                            airdrop['id'], 
                            1, 
                            tx_hash,
                            airdrop['retry'] + 1
                        )
                    else:
                        logger.error(f"发送空投失败，记录 ID: {airdrop['id']}")
                        # 增加重试次数
                        new_retry_count = airdrop['retry'] + 1
                        if new_retry_count >= config['max_retries']:
                            # 达到最大重试次数，记录错误并标记为失败
                            logger.error(f"达到最大重试次数，记录错误，ID: {airdrop['id']}")
                            update_airdrop_status(db_connection, airdrop['id'], 3)  # 3表示失败
                            log_error(db_connection, airdrop)
                        else:
                            # 更新重试次数，保持状态为0（待发送）
                            update_airdrop_status(
                                db_connection, 
                                airdrop['id'], 
                                0, 
                                retry_count=new_retry_count
                            )
                            
                except Exception as e:
                    logger.error(f"处理空投记录 {airdrop['id']} 时出错: {str(e)}")
                    # 增加重试次数
                    new_retry_count = airdrop['retry'] + 1
                    update_airdrop_status(
                        db_connection, 
                        airdrop['id'], 
                        0, 
                        retry_count=new_retry_count
                    )
            
            # 处理已发送但未确认的交易
            process_pending_transactions(db_connection, w3)
            
            # 关闭数据库连接
            db_connection.close()
            
        except Exception as e:
            logger.error(f"主循环出错: {str(e)}")
        
        # 等待下一次扫描
        logger.info(f"等待 {scan_interval} 秒后进行下一次扫描")
        time.sleep(scan_interval)

if __name__ == "__main__":
    # 初始化日志
    logger = setup_logger()
    logger.info("===== 启动空投发送程序 =====")
    
    # 加载配置
    try:
        config = load_config()
        logger.info("配置文件加载成功")
    except Exception as e:
        logger.error("无法加载配置文件，程序退出")
        exit(1)
    
    # 启动主处理函数
    process_airdrops()
    