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
import sys
import os
from tkinter import filedialog
import tkinter as tk
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

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
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD', '123456'),
    'database': os.getenv('MYSQL_DATABASE', 'ctv_database'),
    'port': int(os.getenv('MYSQL_PORT', 3306)),
    'charset': 'utf8mb4'
}

# Thông tin kết nối đến Telegram bot
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '8151971611:AAFwpuSJBfm3doTaNpuTquWOdMQOgFBMId0')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '-4701474696')
MAX_RETRIES = 3
TIMEOUT = 30

# Thiết lập encoding cho stdout
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

# Cấu hình matplotlib để hiển thị tiếng Việt
plt.rcParams['font.family'] = 'Arial Unicode MS'
plt.rcParams['axes.unicode_minus'] = False

def select_excel_file():
    """Hiển thị hộp thoại chọn file Excel"""
    root = tk.Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename(
        title="Chọn file Excel",
        filetypes=[("Excel files", "*.xlsx *.xls")]
    )
    if not file_path:
        logging.error("Không có file nào được chọn!")
        sys.exit(1)
    return file_path

def read_excel_file(file_path):
    """Đọc và xử lý file Excel với hỗ trợ tiếng Việt"""
    try:
        logging.info(f"Đang đọc file Excel: {file_path}")
        df = pd.read_excel(file_path, engine='openpyxl')
        
        # Xử lý tên cột
        if df.columns.str.contains('Unnamed:').any():
            unnamed_cols = df.columns.str.contains('Unnamed:')
            new_cols = df.columns.tolist()
            for i, is_unnamed in enumerate(unnamed_cols):
                if is_unnamed:
                    new_cols[i] = f'col_{i+1}'
            df.columns = new_cols
        
        # Làm sạch tên cột nhưng giữ nguyên dấu tiếng Việt
        df.columns = (df.columns.str.strip()
                     .str.lower()
                     .str.replace(' ', '_')
                     .str.replace('[^\w\s_àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]', '', regex=True))
        
        # Bỏ hoàn toàn cột STT nếu có
        if 'STT' in df.columns:
            df = df.drop(columns=['STT'])

        # Sau đó mới fillna cho các cột khác (trừ STT)
        for col in df.columns:
            if col != 'STT':
                df[col] = df[col].fillna('')
        
        # Các cột số khác: nếu là 0 hoặc NaN thì để trống, nếu là số nguyên thì hiển thị số nguyên, nếu là số thực thì hiển thị số thực
        for col in df.columns:
            if col != 'STT' and pd.api.types.is_numeric_dtype(df[col]):
                def format_number(x):
                    if pd.isna(x) or x == 0:
                        return ''
                    try:
                        x_float = float(x)
                        if x_float.is_integer():
                            return int(x_float)
                        return x_float
                    except:
                        return x
                df[col] = df[col].apply(format_number)
        
        logging.info("Đã đọc và xử lý file Excel thành công")
        return df
    except Exception as e:
        logging.error(f"Lỗi khi đọc file Excel: {e}")
        raise

def setup_mysql_database(connection, database_name, df, table_name):
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

def import_data_to_mysql(connection, df, table_name):
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

def query_mysql(query):
    """
    Thực hiện truy vấn MySQL và trả về kết quả dưới dạng DataFrame
    """
    try:
        logging.info("Kết nối đến MySQL")
        connection = mysql.connector.connect(**mysql_config)
        cursor = connection.cursor()
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame.from_records(rows, columns=columns)
        cursor.close()
        connection.close()
        logging.info("Truy vấn MySQL thành công")
        return df
    except Exception as e:
        logging.error(f"Lỗi khi truy vấn MySQL: {e}")
        raise

def create_image_from_dataframe(df, output_path):
    """Tạo hình ảnh từ DataFrame với style giống ảnh mẫu báo cáo CTV LK"""
    try:
        if df.empty:
            logging.error("DataFrame trống, không thể tạo hình ảnh")
            raise ValueError("Không có dữ liệu để tạo hình ảnh")

        # Đổi tên cột ngắn gọn, có thể xuống dòng
        column_names = {
            'PBH': 'PBH',
            'PTM2025': 'PTM25',
            'PTM2024': 'PTM24',
            'TỔNG SL CTVLK Hiện hữu': 'TỔNG SL\nCTVLK HH',
            'SL CTVLK PTM 2025 PSSL 2025': 'CTVLK\nPTM25 PS',
            'SL CTVLK PTM 2024 PSSL 2025': 'CTVLK\nPTM24 PS',
            'TỔNG SL CTVLK PSSL 2025': 'TỔNG SL\nCTVLK PS',
            'Tỷ lệ CTVLK PTM 2025 PSSL 2025': 'TL CTVLK\nPTM25',
            'Tỷ lệ CTVLK PTM 2024 PSSL 2025': 'TL CTVLK\nPTM24',
            'Tỷ lệ CTVLK PSSL 2025': 'TL\nCTVLK PS',
            'SL ĐH Phát sinh': 'SL ĐH\nPhát sinh',
            'SL ĐH Thành công (Xác thực)': 'SL ĐH\nThành công (Xác thực)',
            'SL ĐH PS Thành công': 'SL ĐH\nPS Thành công',
            'Tỉ lệ ĐH Thành công': 'Tỉ lệ\nĐH Thành công'
        }
        df = df.rename(columns=column_names)

        row_count = len(df)
        col_labels = df.columns.tolist()

        # Tạo màu nền cho từng dòng
        cell_colors = []
        for i in range(row_count):
            if i == row_count - 1:
                cell_colors.append(['#0074bc'] * len(col_labels))  # Dòng tổng cộng
            else:
                base_color = '#ffffff' if i % 2 == 0 else '#eaf6ea'
                cell_colors.append([base_color] * len(col_labels))

        # Tô màu theo quy định cho tất cả các ô có %
        for i in range(row_count - 1):
            for j, col in enumerate(df.columns):
                val = df.iloc[i, j]
                if isinstance(val, str) and '%' in val:
                    try:
                        num = float(val.replace('%', '').replace(',', '.'))
                        if num >= 80:
                            cell_colors[i][j] = '#4CAF50'   # Xanh lá đậm
                        elif num >= 60:
                            cell_colors[i][j] = '#A5D6A7'   # Xanh lá nhạt
                        elif num >= 40:
                            cell_colors[i][j] = '#FFF59D'   # Vàng nhạt
                        elif num >= 20:
                            cell_colors[i][j] = '#FFCC80'   # Cam nhạt
                        else:
                            cell_colors[i][j] = '#EF9A9A'   # Đỏ nhạt
                    except:
                        pass

        # Vẽ bảng
        fig, ax = plt.subplots(figsize=(15, row_count * 0.45))
        ax.axis('off')

        # Tiêu đề lớn màu đỏ phía trên bảng
        plt.text(0.5, 1.08, 'Báo cáo kết quả triển khai Mô hình CTV LK Năm 2025',
                 fontsize=18, color='red', weight='bold', ha='center', va='bottom', transform=ax.transAxes)
        plt.text(0.5, 1.03, '(01.01.2025 - 19.05.2025)',
                 fontsize=14, color='red', weight='bold', ha='center', va='bottom', transform=ax.transAxes)

        col_widths = [0.12] + [0.08] * (len(col_labels) - 1)
        table = ax.table(
            cellText=df.values,
            colLabels=col_labels,
            cellLoc='center',
            loc='center',
            cellColours=cell_colors,
            colWidths=col_widths
        )
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1.1, 1.8)

        # Định dạng header và dòng tổng cộng
        for (row, col), cell in table.get_celld().items():
            if row == 0:
                cell.set_text_props(weight='bold', color='#000000')
                cell.set_fontsize(9)
                cell.set_height(0.18)
                cell.set_edgecolor('#000000')
                cell.set_linewidth(1)
                cell.set_facecolor('#ffffff')
            elif row == row_count:
                cell.set_text_props(weight='bold', color='white')
                cell.set_facecolor('#0074bc')
                cell.set_fontsize(9)
                cell.set_edgecolor('#0074bc')
                cell.set_linewidth(1)
            else:
                cell.set_fontsize(9)
                cell.set_edgecolor('#b0b0b0')
                cell.set_linewidth(0.7)
                cell.set_text_props(va='center', ha='center')

        plt.savefig(output_path, bbox_inches='tight', pad_inches=0.05, format="jpg", dpi=300)
        plt.close(fig)
        logging.info(f"Đã lưu hình ảnh thành công tại: {output_path}")
    except Exception as e:
        logging.error(f"Lỗi khi tạo hình ảnh: {str(e)}")
        raise

async def send_image_to_telegram(image_path, caption=None):
    """
    Gửi ảnh lên Telegram với caption và cơ chế thử lại nếu thất bại
    """
    retry_count = 0
    while retry_count < MAX_RETRIES:
        try:
            logging.info(f"Đang gửi ảnh lên Telegram (lần thử {retry_count + 1}/{MAX_RETRIES})")
            bot = Bot(token=TELEGRAM_BOT_TOKEN)
            with open(image_path, 'rb') as photo:
                await bot.send_photo(
                    chat_id=TELEGRAM_CHAT_ID,
                    photo=photo,
                    caption=caption,
                    read_timeout=TIMEOUT,
                    write_timeout=TIMEOUT,
                    connect_timeout=TIMEOUT
                )
            logging.info("Đã gửi ảnh lên Telegram thành công")
            return
        except (TimedOut, NetworkError) as e:
            retry_count += 1
            if retry_count < MAX_RETRIES:
                wait_time = 2 ** retry_count
                logging.warning(f"Lỗi kết nối, sẽ thử lại sau {wait_time} giây: {e}")
                await asyncio.sleep(wait_time)
            else:
                logging.error(f"Đã thử gửi {MAX_RETRIES} lần không thành công: {e}")
                raise
        except Exception as e:
            logging.error(f"Lỗi không xác định khi gửi ảnh: {e}")
            raise

def delete_mysql_tables(tables):
    pass  # Đã bỏ chức năng xóa bảng

async def process_excel_to_telegram():
    """Quy trình chính: Excel -> MySQL -> Telegram -> Cleanup"""
    try:
        # Bước 1: Chọn và đọc các file Excel
        logging.info("=== Bắt đầu quy trình xử lý ===")
        
        # File danh sách CTV
        logging.info("Vui lòng chọn file Excel chứa danh sách CTV...")
        ctv_file = select_excel_file()
        ctv_df = read_excel_file(ctv_file)
        
        # File danh sách đơn hàng
        logging.info("Vui lòng chọn file Excel chứa danh sách đơn hàng...")
        dh_file = select_excel_file()
        dh_df = read_excel_file(dh_file)
        
        # Bước 2: Kết nối MySQL
        connection = mysql.connector.connect(
            host=mysql_config['host'],
            user=mysql_config['user'],
            password=mysql_config['password']
        )
        
        # Bước 3: Import dữ liệu vào MySQL
        # Import danh sách CTV
        setup_mysql_database(connection, mysql_config['database'], ctv_df, 'danh_sach_ctv_dl')
        import_data_to_mysql(connection, ctv_df, 'danh_sach_ctv_dl')
        
        # Import danh sách đơn hàng
        setup_mysql_database(connection, mysql_config['database'], dh_df, 'danhsachdonhang')
        import_data_to_mysql(connection, dh_df, 'danhsachdonhang')
        
        # Bước 4: Truy vấn dữ liệu và tạo báo cáo
        report_query = get_report_query()
        report_df = query_mysql(report_query)
        
        # Bước 5: Tạo và gửi hình ảnh
        image_path = "report.jpg"
        create_image_from_dataframe(report_df, image_path)
        await send_image_to_telegram(image_path)
        
        # Bước 6: Dọn dẹp
        tables_to_delete = ['danh_sach_ctv_dl', 'danhsachdonhang']
        delete_mysql_tables(tables_to_delete)
        
        logging.info("=== Hoàn thành quy trình xử lý ===")
        
    except Exception as e:
        logging.error(f"Lỗi trong quá trình xử lý: {e}")
        raise

def get_report_query():
    """
    Trả về câu truy vấn để tạo báo cáo
    """
    return """
    SELECT 
        CASE ctv.col_14
            WHEN 'Phòng bán hàng Bến Lức' THEN 'BLC'
            WHEN 'Phòng bán hàng Cần Đước' THEN 'CĐC'
            WHEN 'Phòng bán hàng Cần Giuộc' THEN 'CGC'
            WHEN 'Phòng bán hàng Châu Thành' THEN 'CTH'
            WHEN 'Phòng bán hàng Đức Hòa' THEN 'ĐHA'
            WHEN 'Phòng bán hàng Đức Huệ' THEN 'ĐHE'
            WHEN 'Phòng BHKV Kiến Tường - Mộc Hóa' THEN 'KTMH'
            WHEN 'Phòng BHKV Tân An' THEN 'TAN'
            WHEN 'Phòng bán hàng Thạnh Hóa' THEN 'THA'
            WHEN 'Phòng bán hàng Tân Trụ' THEN 'TTU'
            WHEN 'Phòng bán hàng Tân Thạnh' THEN 'TTH'
            WHEN 'Phòng bán hàng Thủ Thừa' THEN 'TTA'
            WHEN 'Phòng BHKV Vĩnh Hưng - Tân Hưng' THEN 'VHTH'
        END as 'PBH',
        COUNT(DISTINCT CASE 
            WHEN ctv.col_22 = 'Đã xác thực'
            AND ctv.col_17 LIKE '%2025%'
            THEN ctv.col_3 
        END) as 'PTM2025',
        COUNT(DISTINCT CASE 
            WHEN ctv.col_22 = 'Đã xác thực' 
            AND ctv.col_17 LIKE '%2024%'
            THEN ctv.col_3 
        END) as 'PTM2024',
        COUNT(DISTINCT CASE 
            WHEN ctv.col_22 = 'Đã xác thực' 
            AND ctv.col_17 LIKE '%2025%'
            THEN ctv.col_3 
        END) as 'TỔNG SL CTVLK Hiện hữu',
        COUNT(DISTINCT CASE 
            WHEN ctv.col_22 = 'Đã xác thực' 
            AND ctv.col_17 LIKE '%2025%'
            AND dh.col_27 = 'Thành công'
            AND dh.col_25 LIKE '%2025%'
            THEN dh.col_12
        END) as 'SL CTVLK PTM 2025 PSSL 2025',
        COUNT(DISTINCT CASE 
            WHEN ctv.col_22 = 'Đã xác thực' 
            AND ctv.col_17 LIKE '%2024%'
            AND dh.col_27 = 'Thành công'
            AND dh.col_25 LIKE '%2025%'
            THEN dh.col_12
        END) as 'SL CTVLK PTM 2024 PSSL 2025',
        (COUNT(DISTINCT CASE 
            WHEN ctv.col_22 = 'Đã xác thực' 
            AND ctv.col_17 LIKE '%2025%'
            AND dh.col_27 = 'Thành công'
            AND dh.col_25 LIKE '%2025%'
            THEN dh.col_12
        END) +
        COUNT(DISTINCT CASE 
            WHEN ctv.col_22 = 'Đã xác thực' 
            AND ctv.col_17 LIKE '%2024%'
            AND dh.col_27 = 'Thành công'
            AND dh.col_25 LIKE '%2025%'
            THEN dh.col_12
        END)) as 'TỔNG SL CTVLK PSSL 2025',
        CONCAT(
            ROUND(
                COALESCE(
                    COUNT(DISTINCT CASE 
                        WHEN ctv.col_22 = 'Đã xác thực' 
                        AND ctv.col_17 LIKE '%2025%'
                        AND dh.col_27 = 'Thành công'
                        AND dh.col_25 LIKE '%2025%'
                        THEN dh.col_12
                    END) * 100.0 /
                    NULLIF(COUNT(DISTINCT CASE 
                        WHEN ctv.col_22 = 'Đã xác thực' 
                        AND ctv.col_17 LIKE '%2025%'
                        THEN ctv.col_3 
                    END), 0),
                0
            ), 1
        ), '%') as 'Tỷ lệ CTVLK PTM 2025 PSSL 2025',
        CONCAT(
            ROUND(
                COALESCE(
                    COUNT(DISTINCT CASE 
                        WHEN ctv.col_22 = 'Đã xác thực' 
                        AND ctv.col_17 LIKE '%2024%'
                        AND dh.col_27 = 'Thành công'
                        AND dh.col_25 LIKE '%2025%'
                        THEN dh.col_12
                    END) * 100.0 /
                    NULLIF(COUNT(DISTINCT CASE 
                        WHEN ctv.col_22 = 'Đã xác thực' 
                        AND ctv.col_17 LIKE '%2024%'
                        THEN ctv.col_3 
                    END), 0),
                0
            ), 1
        ), '%') as 'Tỷ lệ CTVLK PTM 2024 PSSL 2025',
        CONCAT(
            ROUND(
                COALESCE(
                    (COUNT(DISTINCT CASE 
                        WHEN ctv.col_22 = 'Đã xác thực' 
                        AND ctv.col_17 LIKE '%2025%'
                        AND dh.col_27 = 'Thành công'
                        AND dh.col_25 LIKE '%2025%'
                        THEN dh.col_12
                    END) +
                    COUNT(DISTINCT CASE 
                        WHEN ctv.col_22 = 'Đã xác thực' 
                        AND ctv.col_17 LIKE '%2024%'
                        AND dh.col_27 = 'Thành công'
                        AND dh.col_25 LIKE '%2025%'
                        THEN dh.col_12
                    END)) * 100.0 /
                    NULLIF(COUNT(DISTINCT CASE 
                        WHEN ctv.col_22 = 'Đã xác thực' 
                        AND (ctv.col_17 LIKE '%2025%' OR ctv.col_17 LIKE '%2024%')
                        THEN ctv.col_3 
                    END), 0),
                0
            ), 1
        ), '%') as 'Tỷ lệ CTVLK PSSL 2025',
        COUNT(DISTINCT CASE 
            WHEN ctv.col_22 = 'Đã xác thực'
            AND dh2.col_25 LIKE '%2025%'
            THEN dh2.col_3
        END) as 'SL ĐH Phát sinh',
        COUNT(DISTINCT CASE 
            WHEN ctv.col_22 = 'Đã xác thực'
            AND dh2.col_25 LIKE '%2025%'
            AND dh2.col_27 = 'Thành công'
            THEN dh2.col_3
        END) as 'SL ĐH PS Thành công',
        CONCAT(
            ROUND(
                COALESCE(
                    COUNT(DISTINCT CASE 
                        WHEN ctv.col_22 = 'Đã xác thực'
                        AND dh2.col_25 LIKE '%2025%'
                        AND dh2.col_27 = 'Thành công'
                        THEN dh2.col_3
                    END) * 100.0 /
                    NULLIF(COUNT(DISTINCT CASE 
                        WHEN ctv.col_22 = 'Đã xác thực'
                        AND dh2.col_25 LIKE '%2025%'
                        THEN dh2.col_3
                    END), 0),
                0
            ), 1
        ), '%') as 'Tỉ lệ ĐH Thành công'
    FROM danh_sach_ctv_dl ctv
    LEFT JOIN (
        SELECT col_12, col_27, col_25
        FROM danhsachdonhang 
        WHERE col_27 = 'Thành công' 
        AND col_25 LIKE '%2025%'
    ) dh ON ctv.col_3 = dh.col_12
    LEFT JOIN (
        SELECT col_12, col_2, col_3, col_25, col_19, col_27
        FROM danhsachdonhang 
        WHERE col_25 LIKE '%2025%'
    ) dh2 ON dh2.col_12 = ctv.col_3
    WHERE ctv.col_14 IN (
        'Phòng bán hàng Bến Lức',
        'Phòng bán hàng Cần Đước',
        'Phòng bán hàng Cần Giuộc',
        'Phòng bán hàng Châu Thành',
        'Phòng bán hàng Đức Hòa',
        'Phòng bán hàng Đức Huệ',
        'Phòng BHKV Kiến Tường - Mộc Hóa',
        'Phòng BHKV Tân An',
        'Phòng bán hàng Thạnh Hóa',
        'Phòng bán hàng Tân Trụ',
        'Phòng bán hàng Tân Thạnh',
        'Phòng bán hàng Thủ Thừa',
        'Phòng BHKV Vĩnh Hưng - Tân Hưng'
    )
    GROUP BY ctv.col_14

    UNION ALL

    SELECT 
        'TỔNG CỘNG' as 'PBH',
        COUNT(DISTINCT CASE 
            WHEN ctv.col_22 = 'Đã xác thực' 
            AND ctv.col_17 LIKE '%2025%'
            THEN ctv.col_3 
        END) as 'PTM2025',
        COUNT(DISTINCT CASE 
            WHEN ctv.col_22 = 'Đã xác thực' 
            AND ctv.col_17 LIKE '%2024%'
            THEN ctv.col_3 
        END) as 'PTM2024',
        COUNT(DISTINCT CASE 
            WHEN ctv.col_22 = 'Đã xác thực' 
            AND (ctv.col_17 LIKE '%2025%' OR ctv.col_17 LIKE '%2024%')
            THEN ctv.col_3 
        END) as 'TỔNG SL CTVLK Hiện hữu',
        COUNT(DISTINCT CASE 
            WHEN ctv.col_22 = 'Đã xác thực' 
            AND ctv.col_17 LIKE '%2025%'
            AND dh.col_27 = 'Thành công'
            AND dh.col_25 LIKE '%2025%'
            THEN dh.col_12
        END) as 'SL CTVLK PTM 2025 PSSL 2025',
        COUNT(DISTINCT CASE 
            WHEN ctv.col_22 = 'Đã xác thực' 
            AND ctv.col_17 LIKE '%2024%'
            AND dh.col_27 = 'Thành công'
            AND dh.col_25 LIKE '%2025%'
            THEN dh.col_12
        END) as 'SL CTVLK PTM 2024 PSSL 2025',
        (COUNT(DISTINCT CASE 
            WHEN ctv.col_22 = 'Đã xác thực' 
            AND ctv.col_17 LIKE '%2025%'
            AND dh.col_27 = 'Thành công'
            AND dh.col_25 LIKE '%2025%'
            THEN dh.col_12
        END) +
        COUNT(DISTINCT CASE 
            WHEN ctv.col_22 = 'Đã xác thực' 
            AND ctv.col_17 LIKE '%2024%'
            AND dh.col_27 = 'Thành công'
            AND dh.col_25 LIKE '%2025%'
            THEN dh.col_12
        END)) as 'TỔNG SL CTVLK PSSL 2025',
        CONCAT(
            ROUND(
                COALESCE(
                    COUNT(DISTINCT CASE 
                        WHEN ctv.col_22 = 'Đã xác thực' 
                        AND ctv.col_17 LIKE '%2025%'
                        AND dh.col_27 = 'Thành công'
                        AND dh.col_25 LIKE '%2025%'
                        THEN dh.col_12
                    END) * 100.0 /
                    NULLIF(COUNT(DISTINCT CASE 
                        WHEN ctv.col_22 = 'Đã xác thực' 
                        AND ctv.col_17 LIKE '%2025%'
                        THEN ctv.col_3 
                    END), 0),
                0
            ), 1
        ), '%') as 'Tỷ lệ CTVLK PTM 2025 PSSL 2025',
        CONCAT(
            ROUND(
                COALESCE(
                    COUNT(DISTINCT CASE 
                        WHEN ctv.col_22 = 'Đã xác thực' 
                        AND ctv.col_17 LIKE '%2024%'
                        AND dh.col_27 = 'Thành công'
                        AND dh.col_25 LIKE '%2025%'
                        THEN dh.col_12
                    END) * 100.0 /
                    NULLIF(COUNT(DISTINCT CASE 
                        WHEN ctv.col_22 = 'Đã xác thực' 
                        AND ctv.col_17 LIKE '%2024%'
                        THEN ctv.col_3 
                    END), 0),
                0
            ), 1
        ), '%') as 'Tỷ lệ CTVLK PTM 2024 PSSL 2025',
        CONCAT(
            ROUND(
                COALESCE(
                    (COUNT(DISTINCT CASE 
                        WHEN ctv.col_22 = 'Đã xác thực' 
                        AND ctv.col_17 LIKE '%2025%'
                        AND dh.col_27 = 'Thành công'
                        AND dh.col_25 LIKE '%2025%'
                        THEN dh.col_12
                    END) +
                    COUNT(DISTINCT CASE 
                        WHEN ctv.col_22 = 'Đã xác thực' 
                        AND ctv.col_17 LIKE '%2024%'
                        AND dh.col_27 = 'Thành công'
                        AND dh.col_25 LIKE '%2025%'
                        THEN dh.col_12
                    END)) * 100.0 /
                    NULLIF(COUNT(DISTINCT CASE 
                        WHEN ctv.col_22 = 'Đã xác thực' 
                        AND (ctv.col_17 LIKE '%2025%' OR ctv.col_17 LIKE '%2024%')
                        THEN ctv.col_3 
                    END), 0),
                0
            ), 1
        ), '%') as 'Tỷ lệ CTVLK PSSL 2025',
        COUNT(DISTINCT CASE 
            WHEN ctv.col_22 = 'Đã xác thực'
            AND dh2.col_25 LIKE '%2025%'
            THEN dh2.col_3
        END) as 'SL ĐH Phát sinh',
        COUNT(DISTINCT CASE 
            WHEN ctv.col_22 = 'Đã xác thực'
            AND dh2.col_25 LIKE '%2025%'
            AND dh2.col_27 = 'Thành công'
            THEN dh2.col_3
        END) as 'SL ĐH PS Thành công',
        CONCAT(
            ROUND(
                COALESCE(
                    COUNT(DISTINCT CASE 
                        WHEN ctv.col_22 = 'Đã xác thực'
                        AND dh2.col_25 LIKE '%2025%'
                        AND dh2.col_27 = 'Thành công'
                        THEN dh2.col_3
                    END) * 100.0 /
                    NULLIF(COUNT(DISTINCT CASE 
                        WHEN ctv.col_22 = 'Đã xác thực'
                        AND dh2.col_25 LIKE '%2025%'
                        THEN dh2.col_3
                    END), 0),
                0
            ), 1
        ), '%') as 'Tỉ lệ ĐH Thành công'
    FROM danh_sach_ctv_dl ctv
    LEFT JOIN (
        SELECT col_12, col_27, col_25
        FROM danhsachdonhang 
        WHERE col_27 = 'Thành công' 
        AND col_25 LIKE '%2025%'
    ) dh ON ctv.col_3 = dh.col_12
    LEFT JOIN (
        SELECT col_12, col_2, col_3, col_25, col_19, col_27
        FROM danhsachdonhang 
        WHERE col_25 LIKE '%2025%'
    ) dh2 ON dh2.col_12 = ctv.col_3
    WHERE ctv.col_14 IN (
        'Phòng bán hàng Bến Lức',
        'Phòng bán hàng Cần Đước',
        'Phòng bán hàng Cần Giuộc',
        'Phòng bán hàng Châu Thành',
        'Phòng bán hàng Đức Hòa',
        'Phòng bán hàng Đức Huệ',
        'Phòng BHKV Kiến Tường - Mộc Hóa',
        'Phòng BHKV Tân An',
        'Phòng bán hàng Thạnh Hóa',
        'Phòng bán hàng Tân Trụ',
        'Phòng bán hàng Tân Thạnh',
        'Phòng bán hàng Thủ Thừa',
        'Phòng BHKV Vĩnh Hưng - Tân Hưng'
    )
    """

def generate_report_summary(df):
    # Tổng số lượng CTV Liên kết phát triển mới (PTM2025)
    total_ctv = df.loc[df['PBH'] != 'TỔNG CỘNG', 'PTM2025'].replace('', 0).astype(int).sum()
    # Top 3 PBH có số lượng CTV cao nhất
    top3_ctv = df.loc[df['PBH'] != 'TỔNG CỘNG', ['PBH', 'PTM2025']].replace('', 0)
    top3_ctv = top3_ctv.sort_values('PTM2025', ascending=False).head(3)
    top3_ctv_str = ', '.join([f"{row['PBH']} ({row['PTM2025']})" for _, row in top3_ctv.iterrows()])

    # Tỷ lệ chung CTV LK PSSL năm 2025 (lấy dòng tổng cộng, cột Tỷ lệ CTVLK PSSL 2025)
    ty_le_chung = df.loc[df['PBH'] == 'TỔNG CỘNG', 'Tỷ lệ CTVLK PSSL 2025'].values[0]

    # Top 3 PBH tỷ lệ cao nhất
    df_no_total = df[df['PBH'] != 'TỔNG CỘNG'].copy()
    df_no_total['tyle_num'] = df_no_total['Tỷ lệ CTVLK PSSL 2025'].str.replace('%','').replace('', 0).astype(float)
    top3_ty_le = df_no_total.sort_values('tyle_num', ascending=False).head(3)
    top3_ty_le_str = ', '.join([f"{row['PBH']} ({row['Tỷ lệ CTVLK PSSL 2025']})" for _, row in top3_ty_le.iterrows()])

    # Top 3 PBH tỷ lệ thấp nhất
    bottom3_ty_le = df_no_total.sort_values('tyle_num', ascending=True).head(3)
    bottom3_ty_le_str = ', '.join([f"{row['PBH']} ({row['Tỷ lệ CTVLK PSSL 2025']})" for _, row in bottom3_ty_le.iterrows()])

    # Ngày hiện tại
    today = datetime.now().strftime('%d/%m/%Y')

    # Tạo đoạn điểm tin
    msg = (
        f"ĐIỂM TIN  KẾT QUẢ TRIỂN KHAI MÔ HÌNH CTV LIÊN KẾT NĂM 2025 (tính đến ngày {today})\n"
        f"- Số lượng CTV Liên kết phát triển mới: Tổng {total_ctv} CTV Liên kết. Trong đó:\n"
        f"+ PBH đạt số lượng CTV cao nhất là: {top3_ctv_str}.\n"
        f"- Tỷ lệ CTV LK PSSL năm 2025: Tỷ lệ chung đạt {ty_le_chung}. Trong đó:\n"
        f"+ PBH đạt tỷ lệ cao nhất: {top3_ty_le_str}.\n"
        f"+ PBH đạt tỷ lệ thấp nhất: {bottom3_ty_le_str}.\n"
    )
    return msg

async def main():
    """
    Hàm chính thực hiện toàn bộ quy trình
    """
    try:
        logging.info("Bắt đầu quy trình tạo báo cáo...")
        
        # Lấy dữ liệu báo cáo
        report_query = get_report_query()
        df = query_mysql(report_query)
        
        # Nếu vẫn bị sort lại, bạn có thể tách dòng tổng cộng ra và nối lại cuối cùng:
        if 'PBH' in df.columns:
            df_total = df[df['PBH'] == 'TỔNG CỘNG']
            df_main = df[df['PBH'] != 'TỔNG CỘNG']
            df = pd.concat([df_main, df_total], ignore_index=True)
        
        # Tạo hình ảnh
        image_path = "result.jpg"
        create_image_from_dataframe(df, image_path)
        
        # Tạo đoạn điểm tin
        report_text = generate_report_summary(df)
        
        # Gửi cả hình ảnh và điểm tin lên Telegram
        await send_image_to_telegram(image_path, caption=report_text)
        
        logging.info("Hoàn thành quy trình tạo báo cáo!")
        
    except Exception as e:
        logging.error(f"Lỗi trong quá trình thực hiện: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main()) 