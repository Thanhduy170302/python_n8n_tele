import cx_Oracle
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

# Thông tin kết nối đến Oracle Database
oracle_connection_string = ('BAOCAO/baocao1#1@10.81.30.77:1521/ALINETEST')

# Thông tin kết nối đến Telegram bot
TELEGRAM_BOT_TOKEN = '6496568829:AAG38pueSVhN_Zw9rMQ-aKRBmOVxKdlHR9I'
TELEGRAM_CHAT_ID = '6653901323'

# Truy vấn Oracle Database và lấy kết quả
def query_oracle_database(query):
    try:
        logging.info("Kết nối đến Oracle Database")
        connection = cx_Oracle.connect(oracle_connection_string)
        cursor = connection.cursor()
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame.from_records(rows, columns=columns)
        cursor.close()
        connection.close()
        logging.info("Truy vấn Oracle Database thành công")
        return df
    except Exception as e:
        logging.error(f"Lỗi khi truy vấn Oracle Database: {e}")
        raise

# Tạo hình ảnh từ DataFrame mà không bao gồm cột đầu tiên và chỉ số hàng
def create_image_from_dataframe(df, output_path):
    try:
        logging.info("Tạo hình ảnh từ DataFrame")
        # Bỏ cột đầu tiên và chỉ số hàng
        df = df.iloc[:, 1:]
        fig, ax = plt.subplots(figsize=(12, len(df) * 0.8))  # Điều chỉnh kích thước hình ảnh dựa trên số lượng hàng
        ax.axis('tight')
        ax.axis('off')
        table = tbl.table(ax, df.values, colLabels=df.columns, loc='center', cellLoc='left', colWidths=[0.2] * len(df.columns))
        table.auto_set_font_size(False)
        table.set_fontsize(16)
        table.scale(2.2, 3.2)
         # Làm đậm các chữ trong bảng
        for key, cell in table.get_celld().items():
            cell.set_text_props(weight='bold')

        plt.savefig(output_path, bbox_inches='tight', pad_inches=0.5, format="jpg")
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
async def main(query):
    try:
        df = query_oracle_database(query)
        image_path = "result.jpg"
        create_image_from_dataframe(df, image_path)
        await send_image_to_telegram(image_path)
    except Exception as e:
        logging.error(f"Lỗi trong quá trình thực hiện: {e}")

if __name__ == "__main__":
    import sys
    query = sys.argv[1] if len(sys.argv) > 1 else "SELECT * FROM BAOCAO.DANHMUC_DONVI_PBH_KETOAN"
    asyncio.run(main(query))