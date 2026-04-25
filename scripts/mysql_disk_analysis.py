#!/usr/bin/env python3
"""
MySQL远程服务器磁盘分析
"""
import sys
sys.path.insert(0, '/Volumes/Xdata/workstation/xxxcnstock')

import pymysql
from core.config import get_settings

settings = get_settings()

print("=" * 70)
print("🗄️  MySQL远程服务器磁盘分析")
print("=" * 70)

try:
    conn = pymysql.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD or '',
        database=settings.DB_NAME,
        connect_timeout=10
    )
    
    with conn.cursor() as cursor:
        # 1. 检查数据目录磁盘空间
        print("\n📁 1. 数据目录磁盘空间")
        print("-" * 50)
        try:
            cursor.execute("SHOW VARIABLES LIKE 'datadir'")
            datadir = cursor.fetchone()
            print(f"   数据目录: {datadir[1] if datadir else 'Unknown'}")
            
            # 尝试获取磁盘空间（需要PROCESS权限）
            cursor.execute("""
                SELECT 
                    table_schema,
                    ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS total_mb
                FROM information_schema.tables
                WHERE table_schema = %s
                GROUP BY table_schema
            """, (settings.DB_NAME,))
            
            result = cursor.fetchone()
            if result:
                print(f"   数据库总大小: {result[1]} MB")
        except Exception as e:
            print(f"   无法获取: {e}")
        
        # 2. 检查所有表的大小
        print("\n📊 2. 所有表的大小")
        print("-" * 50)
        cursor.execute("""
            SELECT 
                table_name,
                ROUND(data_length / 1024 / 1024, 2) as data_mb,
                ROUND(index_length / 1024 / 1024, 2) as index_mb,
                ROUND((data_length + index_length) / 1024 / 1024, 2) as total_mb,
                table_rows
            FROM information_schema.tables
            WHERE table_schema = %s
            ORDER BY (data_length + index_length) DESC
        """, (settings.DB_NAME,))
        
        tables = cursor.fetchall()
        total_mb = 0
        for table in tables:
            print(f"   {table[0]:40s} {table[3]:8.2f} MB ({table[4]:,} 行)")
            total_mb += table[3]
        
        print(f"\n   总计: {total_mb:.2f} MB ({total_mb/1024:.2f} GB)")
        
        # 3. 检查二进制日志
        print("\n📝 3. 二进制日志 (Binlog)")
        print("-" * 50)
        try:
            cursor.execute("SHOW VARIABLES LIKE 'log_bin'")
            log_bin = cursor.fetchone()
            print(f"   Binlog启用: {log_bin[1] if log_bin else 'Unknown'}")
            
            cursor.execute("SHOW BINARY LOGS")
            binlogs = cursor.fetchall()
            if binlogs:
                total_binlog_size = sum(int(b[1]) for b in binlogs) / 1024 / 1024
                print(f"   Binlog文件数: {len(binlogs)}")
                print(f"   Binlog总大小: {total_binlog_size:.2f} MB")
                print(f"   最新Binlog: {binlogs[-1][0]} ({int(binlogs[-1][1])/1024/1024:.2f} MB)")
        except Exception as e:
            print(f"   无法获取Binlog信息: {e}")
        
        # 4. 检查临时表空间
        print("\n💾 4. 临时表空间")
        print("-" * 50)
        try:
            cursor.execute("SELECT @@innodb_temp_data_file_path")
            temp_path = cursor.fetchone()
            print(f"   临时表空间: {temp_path[0] if temp_path else 'Unknown'}")
        except Exception as e:
            print(f"   无法获取: {e}")
        
        # 5. 检查undo日志
        print("\n🔄 5. Undo日志")
        print("-" * 50)
        try:
            cursor.execute("SHOW VARIABLES LIKE 'innodb_undo%'")
            undo_vars = cursor.fetchall()
            for var in undo_vars:
                print(f"   {var[0]}: {var[1]}")
        except Exception as e:
            print(f"   无法获取: {e}")
        
        # 6. 检查可以清理的数据
        print("\n🧹 6. 可以清理的数据")
        print("-" * 50)
        
        # 检查旧的报告数据
        try:
            cursor.execute("""
                SELECT COUNT(*) FROM xcn_daily_report 
                WHERE report_date < DATE_SUB(NOW(), INTERVAL 30 DAY)
            """)
            old_reports = cursor.fetchone()[0]
            print(f"   30天前的日报: {old_reports} 条")
        except:
            pass
        
        # 检查旧的选股记录
        try:
            cursor.execute("""
                SELECT COUNT(*) FROM daily_stock_selections 
                WHERE report_date < DATE_SUB(NOW(), INTERVAL 90 DAY)
            """)
            old_selections = cursor.fetchone()[0]
            print(f"   90天前的选股记录: {old_selections} 条")
        except:
            pass
        
        # 检查CCTV新闻
        try:
            cursor.execute("""
                SELECT COUNT(*) FROM cctv_news_broadcast 
                WHERE broadcast_date < DATE_SUB(NOW(), INTERVAL 30 DAY)
            """)
            old_news = cursor.fetchone()[0]
            print(f"   30天前的CCTV新闻: {old_news} 条")
        except:
            pass
    
    conn.close()
    
    print("\n" + "=" * 70)
    print("💡 解决方案建议")
    print("=" * 70)
    print("""
方案1: 清理旧数据（推荐）
------------------------
- 删除90天前的选股记录
- 删除30天前的日报数据
- 删除30天前的CCTV新闻
- 清理旧的Binlog文件

方案2: 使用本地SQLite（无需MySQL）
----------------------------------
- 修改配置使用本地SQLite数据库
- 不需要远程MySQL服务器
- 数据保存在本地文件

方案3: 联系DBA扩容
------------------
- 联系服务器管理员清理磁盘
- 或增加MySQL服务器磁盘空间
""")
    
except Exception as e:
    print(f"❌ 连接失败: {e}")
