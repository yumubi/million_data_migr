import mysql.connector
import time
from datetime import datetime
import os
import sys


# 数据库连接配置
db_config = {
    'host': 'localhost',
    'user': 'your_username',
    'password': 'your_password',
    'database': 'your_database'
}

def clear_screen():
    """清屏"""
    os.system('cls' if os.name == 'nt' else 'clear')

def get_status():
    """获取处理状态"""
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)
    
    # 获取处理状态
    cursor.execute("""
    SELECT last_processed_id, processed_count, last_update
    FROM process_status
    WHERE id = 1
    """)
    status = cursor.fetchone()
    
    # 获取源表总记录数
    cursor.execute("SELECT COUNT(*) as total FROM source_table")
    total = cursor.fetchone()['total']
    
    # 获取目标表记录数
    cursor.execute("SELECT COUNT(*) as total FROM target_table")
    target_count = cursor.fetchone()['total']
    
    cursor.close()
    conn.close()
    
    return {
        'status': status,
        'total_records': total,
        'target_records': target_count
    }

def format_seconds(seconds):
    """将秒数格式化为人类可读格式"""
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

def main():
    """监控主函数"""
    print("Starting monitoring...")
    print("Press Ctrl+C to exit")
    
    try:
        last_count = 0
        last_time = datetime.now()
        
        while True:
            clear_screen()
            data = get_status()
            
            if not data['status']:
                print("No processing data found")
                time.sleep(5)
                continue
            
            now = datetime.now()
            elapsed = (now - last_time).total_seconds()
            
            # 计算速率 (每秒处理的记录数)
            current_count = data['status']['processed_count']
            rate = (current_count - last_count) / elapsed if elapsed > 0 else 0
            
            # 估计剩余时间
            remaining_records = data['total_records'] - current_count
            eta_seconds = remaining_records / rate if rate > 0 else 0
            
            # 进度百分比
            progress = (current_count / data['total_records']) * 100 if data['total_records'] > 0 else 0
            
            # 更新上次统计的数据
            if elapsed >= 5:  # 每5秒更新一次速率计算
                last_count = current_count
                last_time = now
            
            # 打印状态信息
            print(f"Data Processing Monitor - {now.strftime('%Y-%m-%d %H:%M:%S')}")
            print("-" * 50)
            print(f"Total Records:        {data['total_records']:,}")
            print(f"Processed Records:    {current_count:,}")
            print(f"Progress:             {progress:.2f}%")
            print(f"Last Processed ID:    {data['status']['last_processed_id']:,}")
            print(f"Target Table Records: {data['target_records']:,}")
            print(f"Current Rate:         {rate:.2f} records/second")
            print(f"Estimated Time Left:  {format_seconds(int(eta_seconds))}")
            print(f"Last Update:          {data['status']['last_update']}")
            print("-" * 50)
            print("Press Ctrl+C to exit")
            
            time.sleep(2)
            
    except KeyboardInterrupt:
        print("\nExiting monitor...")
    except Exception as e:
        print(f"Error in monitor: {str(e)}")

if __name__ == "__main__":
    main()