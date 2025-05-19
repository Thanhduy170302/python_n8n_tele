import pandas as pd
import mysql.connector
from mysql.connector import Error
import sys
import os
from tkinter import filedialog
import tkinter as tk

def select_excel_file():
    root = tk.Tk()
    root.withdraw()  # Ẩn cửa sổ Tkinter chính
    file_path = filedialog.askopenfilename(
        title="Chọn file Excel",
        filetypes=[("Excel files", "*.xlsx *.xls")]
    )
    if not file_path:
        print("Không có file nào được chọn!")
        sys.exit(1)
    return file_path

def create_connection():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user=input("Nhập MySQL username: "),
            password=input("Nhập MySQL password: "),
        )
        return connection
    except Error as e:
        print(f"Lỗi kết nối MySQL: {e}")
        sys.exit(1)

def create_database(connection, database_name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        print(f"Database {database_name} đã được tạo thành công!")
    except Error as e:
        print(f"Lỗi khi tạo database: {e}")
        sys.exit(1)

def read_excel_file(file_path):
    try:
        # Đọc file Excel
        df = pd.read_excel(file_path)
        
        # Xử lý tên cột
        # 1. Thay thế các cột không tên bằng col_1, col_2,...
        if df.columns.str.contains('Unnamed:').any():
            unnamed_cols = df.columns.str.contains('Unnamed:')
            new_cols = df.columns.tolist()
            for i, is_unnamed in enumerate(unnamed_cols):
                if is_unnamed:
                    new_cols[i] = f'col_{i+1}'
            df.columns = new_cols
        
        # 2. Làm sạch tên cột
        df.columns = (df.columns.str.strip()
                     .str.lower()
                     .str.replace(' ', '_')
                     .str.replace('[^a-z0-9_]', '', regex=True))
        
        # 3. Xử lý giá trị NaN
        df = df.fillna('')  # Thay thế NaN bằng chuỗi rỗng
        
        return df
    except Exception as e:
        print(f"Lỗi khi đọc file Excel: {e}")
        sys.exit(1)

def create_table(connection, database_name, df, table_name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"USE {database_name}")
        
        # Tạo câu lệnh CREATE TABLE dựa trên cấu trúc DataFrame
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
            
            # Đảm bảo tên cột hợp lệ cho MySQL
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
        print(f"Bảng {table_name} đã được tạo thành công!")
        
    except Error as e:
        print(f"Lỗi khi tạo bảng: {e}")
        sys.exit(1)

def insert_data(connection, database_name, df, table_name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"USE {database_name}")
        
        # Chuẩn bị câu lệnh INSERT
        columns = [f"`{col}`" for col in df.columns]  # Thêm backticks cho tên cột
        columns_str = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        
        # Chuyển DataFrame thành list of tuples và xử lý giá trị null
        values = []
        for _, row in df.iterrows():
            # Chuyển đổi các giá trị trong row
            row_values = []
            for val in row:
                if pd.isna(val) or val == 'nan':  # Kiểm tra giá trị NaN
                    row_values.append(None)
                elif isinstance(val, (int, float)):
                    row_values.append(val)
                else:
                    row_values.append(str(val))
            values.append(tuple(row_values))
        
        # Thực hiện insert
        cursor.executemany(insert_query, values)
        connection.commit()
        
        print(f"Đã import thành công {len(df)} dòng dữ liệu!")
        
    except Error as e:
        print(f"Lỗi khi import dữ liệu: {e}")
        connection.rollback()
        sys.exit(1)

def main():
    # Chọn file Excel
    print("Vui lòng chọn file Excel để import...")
    excel_file = select_excel_file()
    
    # Lấy tên file (không có đuôi) làm tên bảng
    table_name = os.path.splitext(os.path.basename(excel_file))[0].lower()
    table_name = ''.join(c for c in table_name if c.isalnum() or c == '_')
    
    # Nhập tên database
    database_name = input("\nNhập tên database (Enter để dùng 'excel_import'): ").strip()
    if not database_name:
        database_name = 'excel_import'
    
    # Đọc file Excel
    print(f"\nĐang đọc file Excel {excel_file}...")
    df = read_excel_file(excel_file)
    
    # Kết nối MySQL
    print("\nKết nối đến MySQL...")
    connection = create_connection()
    
    # Tạo database
    print("\nĐang tạo database...")
    create_database(connection, database_name)
    
    # Tạo bảng
    print("\nĐang tạo bảng...")
    create_table(connection, database_name, df, table_name)
    
    # Import dữ liệu
    print("\nĐang import dữ liệu...")
    insert_data(connection, database_name, df, table_name)
    
    # Đóng kết nối
    if connection.is_connected():
        connection.close()
        print("\nĐã đóng kết nối MySQL")
        
    print(f"\nHoàn thành! Dữ liệu đã được import vào bảng '{table_name}' trong database '{database_name}'")

if __name__ == "__main__":
    main() 