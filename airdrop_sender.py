#!/usr/bin/env python3
# BNB链自动空投发送程序
# Copyright (C) 2023  开发者名称
# 
# 本程序是自由软件：你可以根据自由软件基金会发布的GNU通用公共许可证
# （第三版或任何更新版本）重新分发和/或修改本程序。
# 
# 本程序的分发是希望它能有用，但没有任何保证；甚至没有隐含的适销性或特定用途的保证。
# 详见GNU通用公共许可证了解更多信息。
# 
# 你应该已经收到了GNU通用公共许可证的副本。如果没有，请参见<https://www.gnu.org/licenses/>。

import os
import time
import yaml
import logging
import datetime
import web3
import web3.exceptions
from web3 import Web3
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from logging.handlers import RotatingFileHandler

# 配置日志
def setup_logger():
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    logger = logging.getLogger('AirdropSender')
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # 文件日志
    file_handler = RotatingFileHandler(
        f'logs/airdrop_{datetime.date.today()}.log',
        maxBytes=1024*1024*5,  # 5MB
        backupCount=10
    )
    file_handler.setFormatter(formatter)
    
    # 控制台日志
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

# 加载配置
def load_config():
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)

# 初始化数据库连接
def init_db(config):
    db_config = config['database']
    db_uri = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    engine = create_engine(db_uri, pool_pre_ping=True)
    
    # 创建错误表（如果不存在）
    with engine.connect() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS airdrop_errors (
            id INT AUTO_INCREMENT PRIMARY KEY,
            airdrop_id INT NOT NULL,
            userid INT NOT NULL,
            address VARCHAR(255) NOT NULL,
            amount DECIMAL(18, 8) NOT NULL,
            error_message TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (airdrop_id) REFERENCES airdrop(id)
        )
        """))
        conn.commit()
    
    return engine

# 初始化Web3连接（带节点切换）
def init_web3(rpc_nodes, logger):
    for node in rpc_nodes:
        try:
            w3 = Web3(Web3.HTTPProvider(node))
            if w3.is_connected():
                logger.info(f"成功连接到RPC节点: {node}")
                return w3, node
            else:
                logger.warning(f"无法连接到RPC节点: {node}，尝试下一个节点")
        except Exception as e:
            logger.warning(f"连接RPC节点 {node} 时出错: {str(e)}，尝试下一个节点")
    
    logger.error("所有RPC节点都无法连接")
    return None, None

# 检查余额是否充足
def check_balances(w3, config, required_amount, logger):
    try:
        sender_address = Web3.to_checksum_address(config['wallet']['address'])
        token_contract = w3.eth.contract(
            address=Web3.to_checksum_address(config['token']['contract_address']),
            abi=config['token']['abi']
        )
        
        # 检查BNB余额（用于支付gas）
        bnb_balance = w3.eth.get_balance(sender_address)
        if bnb_balance < Web3.to_wei(config['wallet']['min_bnb_balance'], 'ether'):
            logger.error(f"BNB余额不足: {Web3.from_wei(bnb_balance, 'ether')} BNB")
            return False
        
        # 检查代币余额
        token_balance = token_contract.functions.balanceOf(sender_address).call()
        if token_balance < Web3.to_wei(required_amount, 'ether'):
            logger.error(f"代币余额不足: {Web3.from_wei(token_balance, 'ether')}，需要: {required_amount}")
            return False
            
        return True
    except Exception as e:
        logger.error(f"检查余额时出错: {str(e)}")
        return False

# 获取待处理的空投记录
def get_pending_airdrops(engine, batch_size, logger):
    try:
        with engine.connect() as conn:
            # 使用FOR UPDATE锁定记录，防止并发处理
            result = conn.execute(text("""
            SELECT id, userid, address, amount FROM airdrop 
            WHERE aflag = 0 AND retry < :max_retry 
            ORDER BY id ASC LIMIT :batch_size
            FOR UPDATE SKIP LOCKED
            """), {
                "max_retry": config['app']['max_retry'],
                "batch_size": batch_size
            })
            
            airdrops = result.fetchall()
            conn.commit()
            return airdrops
    except SQLAlchemyError as e:
        logger.error(f"获取待处理空投记录时出错: {str(e)}")
        return []

# 标记记录为处理中（关键的防重复发送步骤）
def mark_airdrop_as_processing(engine, airdrop_id, logger):
    try:
        with engine.connect() as conn:
            # 使用UPDATE ... WHERE确保原子性操作
            result = conn.execute(text("""
            UPDATE airdrop 
            SET aflag = 1, stime = NOW() 
            WHERE id = :id AND aflag = 0
            """), {"id": airdrop_id})
            
            conn.commit()
            
            # 检查是否有行被更新
            if result.rowcount == 1:
                logger.info(f"成功将空投记录 {airdrop_id} 标记为处理中")
                return True
            else:
                logger.error(f"更新空投记录 {airdrop_id} 状态失败，可能已被其他进程处理")
                return False
    except SQLAlchemyError as e:
        logger.error(f"更新空投记录 {airdrop_id} 状态时出错: {str(e)}")
        return False

# 发送代币
def send_token(w3, config, airdrop, logger):
    try:
        sender_address = Web3.to_checksum_address(config['wallet']['address'])
        private_key = config['wallet']['private_key']
        recipient_address = Web3.to_checksum_address(airdrop.address)
        
        # 确保地址有效
        if not Web3.is_address(recipient_address):
            raise ValueError(f"无效的接收地址: {airdrop.address}")
        
        token_contract = w3.eth.contract(
            address=Web3.to_checksum_address(config['token']['contract_address']),
            abi=config['token']['abi']
        )
        
        # 构建交易
        nonce = w3.eth.get_transaction_count(sender_address)
        amount_wei = Web3.to_wei(airdrop.amount, 'ether')
        
        tx = token_contract.functions.transfer(recipient_address, amount_wei).build_transaction({
            'from': sender_address,
            'nonce': nonce,
            'gas': config['transaction']['gas_limit'],
            'gasPrice': w3.eth.gas_price
        })
        
        # 签名交易
        signed_tx = w3.eth.account.sign_transaction(tx, private_key)
        
        # 发送交易
        tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        tx_hash_hex = w3.to_hex(tx_hash)
        
        logger.info(f"交易已发送，哈希: {tx_hash_hex}, 接收地址: {recipient_address}, 金额: {airdrop.amount}")
        return tx_hash_hex
        
    except Exception as e:
        logger.error(f"发送代币到 {airdrop.address} 时出错: {str(e)}")
        return None

# 更新交易哈希
def update_airdrop_tx_hash(engine, airdrop_id, tx_hash, logger):
    try:
        with engine.connect() as conn:
            conn.execute(text("""
            UPDATE airdrop 
            SET tx_hash = :tx_hash 
            WHERE id = :id
            """), {
                "tx_hash": tx_hash,
                "id": airdrop_id
            })
            conn.commit()
            logger.info(f"已更新空投记录 {airdrop_id} 的交易哈希")
            return True
    except SQLAlchemyError as e:
        logger.error(f"更新空投记录 {airdrop_id} 的交易哈希时出错: {str(e)}")
        return False

# 检查交易是否确认
def check_transaction_confirmation(w3, tx_hash, required_confirmations, logger):
    try:
        tx_receipt = w3.eth.get_transaction_receipt(tx_hash)
        if tx_receipt is None:
            return False, "交易尚未确认"
            
        current_block = w3.eth.block_number
        confirmations = current_block - tx_receipt['blockNumber']
        
        if confirmations >= required_confirmations:
            return True, "交易已确认"
        else:
            return False, f"等待更多确认 (当前: {confirmations}/{required_confirmations})"
    except web3.exceptions.TransactionNotFound:
        return False, "交易未找到"
    except Exception as e:
        logger.error(f"检查交易确认时出错: {str(e)}")
        return False, str(e)

# 更新空投状态
def update_airdrop_status(engine, airdrop_id, status, logger):
    try:
        with engine.connect() as conn:
            conn.execute(text("""
            UPDATE airdrop 
            SET aflag = :status 
            WHERE id = :id
            """), {
                "status": status,
                "id": airdrop_id
            })
            conn.commit()
            logger.info(f"已将空投记录 {airdrop_id} 的状态更新为 {status}")
            return True
    except SQLAlchemyError as e:
        logger.error(f"更新空投记录 {airdrop_id} 的状态时出错: {str(e)}")
        return False

# 处理发送失败
def handle_send_failure(engine, airdrop, error_msg, logger, max_retry):
    try:
        with engine.connect() as conn:
            # 增加重试次数
            result = conn.execute(text("""
            UPDATE airdrop 
            SET retry = retry + 1, aflag = 0 
            WHERE id = :id
            """), {"id": airdrop.id})
            
            conn.commit()
            
            # 检查是否达到最大重试次数
            if airdrop.retry + 1 >= max_retry:
                # 记录到错误表
                conn.execute(text("""
                INSERT INTO airdrop_errors (airdrop_id, userid, address, amount, error_message)
                VALUES (:airdrop_id, :userid, :address, :amount, :error_message)
                """), {
                    "airdrop_id": airdrop.id,
                    "userid": airdrop.userid,
                    "address": airdrop.address,
                    "amount": airdrop.amount,
                    "error_message": error_msg
                })
                
                # 标记为失败
                conn.execute(text("""
                UPDATE airdrop 
                SET aflag = 3 
                WHERE id = :id
                """), {"id": airdrop.id})
                
                conn.commit()
                logger.warning(f"空投记录 {airdrop.id} 达到最大重试次数，已记录到错误表")
                return False
            
            logger.info(f"空投记录 {airdrop.id} 发送失败，已重试 {airdrop.retry + 1} 次")
            return True
    except SQLAlchemyError as e:
        logger.error(f"处理空投记录 {airdrop.id} 发送失败时出错: {str(e)}")
        return False

# 主处理函数
def process_airdrops(engine, w3, config, logger):
    batch_size = config['app']['batch_size']
    max_retry = config['app']['max_retry']
    required_confirmations = config['transaction']['required_confirmations']
    
    # 获取待处理的空投记录
    airdrops = get_pending_airdrops(engine, batch_size, logger)
    logger.info(f"找到 {len(airdrops)} 条待处理的空投记录")
    
    if not airdrops:
        return 0
    
    # 计算所需总金额
    total_amount = sum(airdrop.amount for airdrop in airdrops)
    
    # 检查余额
    if not check_balances(w3, config, total_amount, logger):
        return 0
    
    # 处理每条记录
    success_count = 0
    for airdrop in airdrops:
        # 关键步骤：先标记记录为处理中，确保原子性
        if not mark_airdrop_as_processing(engine, airdrop.id, logger):
            # 如果标记失败，立即停止处理，防止重复发送
            logger.critical("标记记录为处理中失败，为防止重复发送，程序将停止")
            # 可以考虑发送告警通知
            return success_count
        
        # 发送代币
        tx_hash = send_token(w3, config, airdrop, logger)
        
        if tx_hash:
            # 更新交易哈希
            if update_airdrop_tx_hash(engine, airdrop.id, tx_hash, logger):
                # 等待交易确认
                confirmed = False
                for _ in range(config['transaction']['confirmation_check_attempts']):
                    confirmed, msg = check_transaction_confirmation(
                        w3, tx_hash, required_confirmations, logger
                    )
                    logger.info(f"交易 {tx_hash} 确认状态: {msg}")
                    
                    if confirmed:
                        # 更新状态为已确认
                        update_airdrop_status(engine, airdrop.id, 2, logger)
                        success_count += 1
                        break
                    
                    time.sleep(config['transaction']['confirmation_check_interval'])
                
                if not confirmed:
                    logger.warning(f"交易 {tx_hash} 在指定时间内未确认")
                    # 不更新状态为失败，留待下一轮检查
        else:
            # 发送失败，处理重试
            handle_send_failure(engine, airdrop, "发送交易失败", logger, max_retry)
    
    return success_count

# 主函数
def main():
    global config
    logger = setup_logger()
    logger.info("启动BNB链自动空投发送程序")
    
    try:
        # 加载配置
        config = load_config()
        
        # 初始化数据库
        engine = init_db(config)
        
        # 初始化Web3连接
        w3, current_node = init_web3(config['rpc_nodes'], logger)
        if not w3:
            logger.error("无法连接到任何RPC节点，程序退出")
            return
        
        # 初始扫描间隔
        scan_interval = config['app']['initial_scan_interval']
        min_interval = config['app']['min_scan_interval']
        max_interval = config['app']['max_scan_interval']
        interval_adjustment = config['app']['interval_adjustment']
        
        while True:
            try:
                # 检查Web3连接
                if not w3.is_connected():
                    logger.warning("Web3连接已断开，尝试重新连接")
                    w3, current_node = init_web3(config['rpc_nodes'], logger)
                    if not w3:
                        logger.error("无法重新连接到任何RPC节点，等待下一轮")
                        time.sleep(scan_interval)
                        continue
                
                # 处理空投
                processed_count = process_airdrops(engine, w3, config, logger)
                
                # 动态调整扫描间隔
                if processed_count == 0:
                    # 没有处理任何记录，延长间隔
                    scan_interval = min(scan_interval + interval_adjustment, max_interval)
                else:
                    # 处理了记录，缩短间隔
                    scan_interval = max(scan_interval - interval_adjustment, min_interval)
                
                logger.info(f"本轮处理完成，下次扫描间隔: {scan_interval}秒")
                time.sleep(scan_interval)
                
            except Exception as e:
                logger.error(f"主循环出错: {str(e)}", exc_info=True)
                time.sleep(scan_interval)
    
    except Exception as e:
        logger.critical(f"程序初始化失败: {str(e)}", exc_info=True)
        return

if __name__ == "__main__":
    main()
