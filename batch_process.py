import mysql.connector
import time
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_processing.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

# 数据库连接配置
db_config = {
    'host': 'localhost',
    'user': 'your_username',
    'password': 'your_password',
    'database': 'your_database'
}

def get_last_processed_id():
    """获取上次处理到的ID"""
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    
    cursor.execute("SELECT last_processed_id FROM process_status WHERE id = 1")
    result = cursor.fetchone()
    
    cursor.close()
    conn.close()
    
    return result[0] if result else 0

def update_process_status(last_id, processed_count):
    """更新处理进度"""
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    
    cursor.execute(
        "UPDATE process_status SET last_processed_id = %s, processed_count = processed_count + %s, last_update = NOW() WHERE id = 1",
        (last_id, processed_count)
    )
    
    conn.commit()
    cursor.close()
    conn.close()

def process_batch(batch_size=1000, sleep_seconds=0.5):
    """处理一个批次的数据"""
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)
    
    # 获取上次处理到的ID
    last_id = get_last_processed_id()
    logger.info(f"Starting batch processing from ID > {last_id}")
    
    # 查询下一批数据
    cursor.execute(
        "SELECT id, product_category, purchase_amount FROM source_table WHERE id > %s ORDER BY id LIMIT %s",
        (last_id, batch_size)
    )
    
    records = cursor.fetchall()
    if not records:
        logger.info("No more records to process")
        cursor.close()
        conn.close()
        return 0
    
    # 聚合当前批次数据
    category_stats = {}
    for record in records:
        category = record['product_category']
        amount = float(record['purchase_amount'])
        
        if category not in category_stats:
            category_stats[category] = {
                'count': 0,
                'total': 0.0
            }
        
        category_stats[category]['count'] += 1
        category_stats[category]['total'] += amount
    
    # 使用事务进行更新
    try:
        cursor.execute("START TRANSACTION")
        
        # 更新目标表
        for category, stats in category_stats.items():
            avg_amount = stats['total'] / stats['count']
            
            # 使用INSERT ... ON DUPLICATE KEY UPDATE进行更新
            cursor.execute("""
            INSERT INTO target_table (product_category, total_purchases, total_amount, avg_amount)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                total_purchases = total_purchases + VALUES(total_purchases),
                total_amount = total_amount + VALUES(total_amount),
                avg_amount = (total_amount + VALUES(total_amount)) / (total_purchases + VALUES(total_purchases))
            """, (category, stats['count'], stats['total'], avg_amount))
        
        # 记录最后处理的ID
        last_processed_id = records[-1]['id']
        
        # 提交事务
        conn.commit()
        
        # 更新处理状态
        update_process_status(last_processed_id, len(records))
        
        logger.info(f"Processed batch of {len(records)} records. Last ID: {last_processed_id}")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error processing batch: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()
    
    # 控制处理速度
    time.sleep(sleep_seconds)
    
    return len(records)

def main():
    """主处理函数"""
    start_time = datetime.now()
    logger.info(f"Starting data processing at {start_time}")
    
    total_processed = 0
    batch_size = 1000  # 每批处理的记录数
    log_interval = 100000  # 每处理10万条记录记录一次日志
    
    try:
        while True:
            processed = process_batch(batch_size)
            if processed == 0:
                break
                
            total_processed += processed
            
            # 每处理log_interval条记录记录一次详细日志
            if total_processed % log_interval == 0:
                elapsed = (datetime.now() - start_time).total_seconds()
                rate = total_processed / elapsed if elapsed > 0 else 0
                logger.info(f"Processed {total_processed} records so far. Rate: {rate:.2f} records/second")
    
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Error during processing: {str(e)}")
    
    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()
    logger.info(f"Completed processing. Total records: {total_processed}")
    logger.info(f"Total time: {elapsed:.2f} seconds. Average rate: {total_processed/elapsed:.2f} records/second")

if __name__ == "__main__":
    main()