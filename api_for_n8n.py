from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import mysql.connector
from mysql.connector import Error
import matplotlib.pyplot as plt
import matplotlib.table as tbl
from telegram import Bot
import logging
import tracemalloc
import asyncio
from telegram.error import TimedOut, NetworkError
import os
from datetime import datetime
import uvicorn
from typing import Optional
import aiofiles
import shutil
import sys

# Thiết lập encoding cho stdout
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

# Kích hoạt tracemalloc để theo dõi phân bổ bộ nhớ
tracemalloc.start()

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

# Thông tin kết nối đến MySQL
mysql_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '123456',
    'database': 'ctv_database',
    'port': 3306,
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_unicode_ci'
}

# Thông tin kết nối đến Telegram bot
TELEGRAM_BOT_TOKEN = '8151971611:AAFwpuSJBfm3doTaNpuTquWOdMQOgFBMId0'
TELEGRAM_CHAT_ID = '-4701474696'
MAX_RETRIES = 3
TIMEOUT = 30

# Khởi tạo FastAPI
app = FastAPI(
    title="API for N8N Automation",
    description="API endpoints for automating Excel to Telegram report process",
    version="1.0.0"
)

# Cấu hình CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Tạo thư mục tạm để lưu file
TEMP_DIR = "temp_files"
os.makedirs(TEMP_DIR, exist_ok=True)

async def save_upload_file(upload_file: UploadFile) -> str:
    """Lưu file tải lên vào thư mục tạm"""
    file_path = os.path.join(TEMP_DIR, upload_file.filename)
    async with aiofiles.open(file_path, 'wb') as out_file:
        content = await upload_file.read()
        await out_file.write(content)
    return file_path

def read_excel_file(file_path: str) -> pd.DataFrame:
    """Đọc và xử lý file Excel"""
    try:
        logging.info(f"Đang đọc file Excel: {file_path}")
        df = pd.read_excel(file_path)
        
        # Xử lý tên cột
        if df.columns.str.contains('Unnamed:').any():
            unnamed_cols = df.columns.str.contains('Unnamed:')
            new_cols = df.columns.tolist()
            for i, is_unnamed in enumerate(unnamed_cols):
                if is_unnamed:
                    new_cols[i] = f'col_{i+1}'
            df.columns = new_cols
        
        # Làm sạch tên cột
        df.columns = (df.columns.str.strip()
                     .str.lower()
                     .str.replace(' ', '_')
                     .str.replace('[^a-z0-9_]', '', regex=True))
        
        # Xử lý giá trị NaN
        df = df.fillna('')
        
        logging.info("Đã đọc và xử lý file Excel thành công")
        return df
    except Exception as e:
        logging.error(f"Lỗi khi đọc file Excel: {e}")
        raise

def setup_mysql_database(connection, database_name: str, df: pd.DataFrame, table_name: str):
    """Thiết lập database và bảng trong MySQL"""
    try:
        cursor = connection.cursor()
        
        # Tạo database nếu chưa tồn tại
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        cursor.execute(f"USE {database_name}")
        logging.info(f"Đã tạo và chọn database: {database_name}")
        
        # Tạo bảng
        columns = []
        for column in df.columns:
            dtype = df[column].dtype
            if 'int' in str(dtype):
                sql_type = 'INT'
            elif 'float' in str(dtype):
                sql_type = 'FLOAT'
            elif 'datetime' in str(dtype):
                sql_type = 'DATETIME'
            else:
                sql_type = 'VARCHAR(255)'
            
            column = column.strip().lower().replace(' ', '_')
            column = ''.join(c for c in column if c.isalnum() or c == '_')
            if column[0].isdigit():
                column = 'col_' + column
                
            columns.append(f"`{column}` {sql_type}")
        
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            {', '.join(columns)}
        )
        """
        cursor.execute(create_table_query)
        logging.info(f"Đã tạo bảng: {table_name}")
        
    except Error as e:
        logging.error(f"Lỗi khi thiết lập database: {e}")
        raise

def import_data_to_mysql(connection, df: pd.DataFrame, table_name: str):
    """Import dữ liệu từ DataFrame vào MySQL"""
    try:
        cursor = connection.cursor()
        
        # Chuẩn bị câu lệnh INSERT
        columns = [f"`{col}`" for col in df.columns]
        columns_str = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        
        # Chuyển DataFrame thành list of tuples và xử lý giá trị null
        values = []
        for _, row in df.iterrows():
            row_values = []
            for val in row:
                if pd.isna(val) or val == 'nan':
                    row_values.append(None)
                elif isinstance(val, (int, float)):
                    row_values.append(val)
                else:
                    row_values.append(str(val))
            values.append(tuple(row_values))
        
        # Thực hiện insert
        cursor.executemany(insert_query, values)
        connection.commit()
        logging.info(f"Đã import thành công {len(df)} dòng dữ liệu")
        
    except Error as e:
        logging.error(f"Lỗi khi import dữ liệu: {e}")
        connection.rollback()
        raise

def delete_mysql_tables(tables: list):
    """Xóa các bảng MySQL sau khi hoàn thành"""
    try:
        logging.info("Đang kết nối đến MySQL để xóa bảng...")
        connection = mysql.connector.connect(**mysql_config)
        cursor = connection.cursor()
        
        for table in tables:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
                logging.info(f"Đã xóa thành công bảng: {table}")
            except Exception as e:
                logging.error(f"Lỗi khi xóa bảng {table}: {e}")
        
        connection.commit()
        cursor.close()
        connection.close()
        logging.info("Đã xóa xong các bảng")
    except Exception as e:
        logging.error(f"Lỗi khi thao tác với MySQL: {e}")
        raise

def cleanup_temp_files():
    """Dọn dẹp các file tạm"""
    try:
        shutil.rmtree(TEMP_DIR)
        os.makedirs(TEMP_DIR)
        logging.info("Đã dọn dẹp thư mục tạm")
    except Exception as e:
        logging.error(f"Lỗi khi dọn dẹp thư mục tạm: {e}")

@app.post("/upload/ctv")
async def upload_ctv_file(file: UploadFile = File(...)):
    """Upload file Excel danh sách CTV"""
    try:
        file_path = await save_upload_file(file)
        df = read_excel_file(file_path)
        
        connection = mysql.connector.connect(
            host=mysql_config['host'],
            user=mysql_config['user'],
            password=mysql_config['password']
        )
        
        setup_mysql_database(connection, mysql_config['database'], df, 'danh_sach_ctv_dl')
        import_data_to_mysql(connection, df, 'danh_sach_ctv_dl')
        
        return {"message": "Đã import thành công file danh sách CTV"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/upload/donhang")
async def upload_donhang_file(file: UploadFile = File(...)):
    """Upload file Excel danh sách đơn hàng"""
    try:
        file_path = await save_upload_file(file)
        df = read_excel_file(file_path)
        
        connection = mysql.connector.connect(
            host=mysql_config['host'],
            user=mysql_config['user'],
            password=mysql_config['password']
        )
        
        setup_mysql_database(connection, mysql_config['database'], df, 'danhsachdonhang')
        import_data_to_mysql(connection, df, 'danhsachdonhang')
        
        return {"message": "Đã import thành công file danh sách đơn hàng"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/generate-report")
async def generate_and_send_report():
    """Tạo báo cáo và gửi lên Telegram"""
    try:
        from excel_to_telegram_report import (
            query_mysql,
            create_image_from_dataframe,
            send_image_to_telegram,
            get_report_query
        )
        
        # Tạo báo cáo
        report_query = get_report_query()
        report_df = query_mysql(report_query)
        
        # Tạo và gửi hình ảnh
        image_path = os.path.join(TEMP_DIR, "report.jpg")
        create_image_from_dataframe(report_df, image_path)
        await send_image_to_telegram(image_path)
        
        return {"message": "Đã tạo và gửi báo cáo thành công"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/cleanup")
async def cleanup():
    """Dọn dẹp dữ liệu và file tạm"""
    try:
        # Xóa các bảng MySQL
        tables_to_delete = ['danh_sach_ctv_dl', 'danhsachdonhang']
        delete_mysql_tables(tables_to_delete)
        
        # Dọn dẹp file tạm
        cleanup_temp_files()
        
        return {"message": "Đã dọn dẹp thành công"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000) 