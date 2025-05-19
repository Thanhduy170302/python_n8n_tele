import pypyodbc
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.table as tbl
from telegram import Bot
import logging
import tracemalloc
import asyncio

# Kích hoạt tracemalloc để theo dõi phân bổ bộ nhớ
tracemalloc.start()

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Thông tin kết nối đến SQL Server
connection_string = (
    'DRIVER={ODBC Driver 17 for SQL Server};'  # Đảm bảo driver ODBC chính xác
    'SERVER=10.81.31.78;'                      # Tên máy chủ SQL Server của bạn
    'DATABASE=telegram;'                       # Tên cơ sở dữ liệu của bạn
    'UID=sa;'                                  # Tên đăng nhập của bạn
    'PWD=T@nkhanh123!@#;'                      # Mật khẩu của bạn
    'TrustServerCertificate=yes;'              # Thêm tùy chọn này để bỏ qua kiểm tra chứng chỉ SSL
    'Encrypt=yes;'                             # Bật mã hóa SSL
    'Connection Timeout=30;')

# Thông tin kết nối đến Telegram bot
TELEGRAM_BOT_TOKEN = '6496568829:AAG38pueSVhN_Zw9rMQ-aKRBmOVxKdlHR9I'
TELEGRAM_CHAT_ID = '6653901323'

# Truy vấn SQL Server và lấy kết quả
def query_sql_server():
    try:
        logging.info("Kết nối đến SQL Server")
        connection = pypyodbc.connect(connection_string)
        cursor = connection.cursor()
        query = "SELECT [DONVI_ID], [TEN_NGAN], [TEN], [MA_DV] FROM phongbanhang"
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame.from_records(rows, columns=columns)
        cursor.close()
        connection.close()
        logging.info("Truy vấn SQL Server thành công")
        return df
    except Exception as e:
        logging.error(f"Lỗi khi truy vấn SQL Server: {e}")
        raise

# Tạo hình ảnh từ DataFrame
def create_image_from_dataframe(df, output_path):
    try:
        logging.info("Tạo hình ảnh từ DataFrame")
        fig, ax = plt.subplots(figsize=(10, len(df) * 0.4))  # Điều chỉnh kích thước hình ảnh dựa trên số lượng hàng
        ax.axis('tight')
        ax.axis('off')
        table = tbl.table(ax, df, loc='center', cellLoc='center', colWidths=[0.1] * len(df.columns))
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1.2, 1.2)
        plt.savefig(output_path, format="jpg")
        plt.close(fig)
        logging.info(f"Hình ảnh đã được lưu tại {output_path}")
    except Exception as e:
        logging.error(f"Lỗi khi tạo hình ảnh: {e}")
        raise

# Gửi ảnh lên Telegram
async def send_image_to_telegram(image_path):
    try:
        logging.info("Gửi hình ảnh lên Telegram")
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        with open(image_path, 'rb') as photo:
            await bot.send_photo(chat_id=TELEGRAM_CHAT_ID, photo=photo)
        logging.info("Hình ảnh đã được gửi lên Telegram")
    except Exception as e:
        logging.error(f"Lỗi khi gửi hình ảnh lên Telegram: {e}")
        raise

# Thực hiện các bước
async def main():
    try:
        df = query_sql_server()
        image_path = "result.jpg"
        create_image_from_dataframe(df, image_path)
        await send_image_to_telegram(image_path)
    except Exception as e:
        logging.error(f"Lỗi trong quá trình thực hiện: {e}")

if __name__ == "__main__":
    asyncio.run(main())