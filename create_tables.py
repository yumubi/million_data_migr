import mysql.connector
import random
import string
from tqdm import tqdm

"""创建两个表：一个源表(包含亿级数据)和一个目标表(用于存放聚合结果)。"""

# 数据库连接配置
db_config = {
    'host': 'localhost',
    'user': 'your_username',
    'password': 'your_password',
    'database': 'your_database'
}

def create_tables():
    """创建源表和目标表"""
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    
    # 创建源表 - 假设我们要分析的是用户购买行为
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS source_table (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL,
        product_category VARCHAR(50) NOT NULL,
        purchase_amount DECIMAL(10, 2) NOT NULL,
        purchase_date DATETIME NOT NULL,
        INDEX idx_user_id (user_id),
        INDEX idx_product_category (product_category)
    ) ENGINE=InnoDB
    """)
    
    # 创建目标表 - 用于存储聚合结果
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS target_table (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        product_category VARCHAR(50) NOT NULL,
        total_purchases INT NOT NULL,
        total_amount DECIMAL(15, 2) NOT NULL,
        avg_amount DECIMAL(10, 2) NOT NULL,
        UNIQUE KEY idx_product_category (product_category)
    ) ENGINE=InnoDB
    """)
    
    # 创建进度记录表 - 用于存储处理进度
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS process_status (
        id INT PRIMARY KEY,
        last_processed_id BIGINT NOT NULL,
        processed_count BIGINT NOT NULL,
        last_update DATETIME NOT NULL
    ) ENGINE=InnoDB
    """)
    
    # 初始化进度记录
    cursor.execute("INSERT IGNORE INTO process_status VALUES (1, 0, 0, NOW())")
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print("Tables created successfully")

def generate_sample_data(batch_size=10000, total_records=100000):
    """生成模拟数据插入到源表中 - 用于测试"""
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    
    categories = ["Electronics", "Clothing", "Food", "Books", "Sports", "Home", "Beauty", "Toys"]
    
    # 使用tqdm显示进度条
    print(f"Generating {total_records} sample records...")
    for i in tqdm(range(0, total_records, batch_size)):
        values = []
        for _ in range(min(batch_size, total_records - i)):
            user_id = random.randint(1, 1000000)
            category = random.choice(categories)
            amount = round(random.uniform(10.0, 1000.0), 2)
            date = f"2024-{random.randint(1, 12):02d}-{random.randint(1, 28):02d} {random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}"
            
            values.append((user_id, category, amount, date))
        
        query = "INSERT INTO source_table (user_id, product_category, purchase_amount, purchase_date) VALUES (%s, %s, %s, %s)"
        cursor.executemany(query, values)
        conn.commit()
    
    cursor.close()
    conn.close()
    print(f"Generated {total_records} sample records successfully")

if __name__ == "__main__":
    create_tables()
    
    # 询问是否生成示例数据
    response = input("Do you want to generate sample data? (y/n): ")
    if response.lower() == 'y':
        try:
            record_count = int(input("How many records to generate? (default: 100000): ") or "100000")
            generate_sample_data(total_records=record_count)
        except ValueError:
            print("Invalid input. Using default value of 100000.")
            generate_sample_data()