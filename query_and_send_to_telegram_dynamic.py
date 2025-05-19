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
TELEGRAM_CHAT_ID = '-4700723582'

# Truy vấn SQL Server và lấy kết quả
def query_sql_server(query):
    try:
        logging.info("Kết nối đến SQL Server")
        connection = pypyodbc.connect(connection_string)
        cursor = connection.cursor()
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


# Tạo hình ảnh từ DataFrame mà không bao gồm cột đầu tiên và chỉ số hàng
def create_image_from_dataframe(df, output_path, col_widths=None):
    try:
        logging.info("Tạo hình ảnh từ DataFrame")
        # Bỏ chỉ số hàng
        df = df.iloc[:, 0:]
        fig, ax = plt.subplots(figsize=(12, len(df) * 0.8))  # Điều chỉnh kích thước hình ảnh dựa trên số lượng hàng
        ax.axis('tight')
        ax.axis('off')
        
               
         # Nếu không có col_widths, đặt mặc định với cột đầu tiên có độ rộng là 0.5
        if col_widths is None:
            col_widths = [0.4] + [0.2] * (len(df.columns) - 1)

        table = tbl.table(ax, df.values, colLabels=df.columns, loc='center', cellLoc='center', colWidths=col_widths)
        table.auto_set_font_size(False)
        table.set_fontsize(15)
        table.scale(0.8, 3.2)

   # Làm đậm các chữ trong bảng và canh lề trái cho cột đầu tiên
        for (i, j), cell in table.get_celld().items():
            cell.set_text_props(weight='bold')
            if i == 0:  # Header row
                cell.get_text().set_fontweight('bold')
                cell.get_text().set_text(cell.get_text().get_text().upper())
            if j == 0:  # Cột đầu tiên
                cell.set_text_props(ha='left')
 

        plt.savefig(output_path, bbox_inches='tight', pad_inches=0.5, format="jpg")
        plt.close(fig)
        logging.info(f"Hình ảnh đã được lưu tại {output_path}")
    except Exception as e:
        logging.error(f"Lỗi khi tạo hình ảnh: {e}")
        raise

# Gửi ảnh lên Telegram
async def send_image_to_telegram(image_path, caption):
    try:
        logging.info("Gửi hình ảnh lên Telegram")
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        async with bot:
            with open(image_path, 'rb') as photo:
                await bot.send_photo(chat_id=TELEGRAM_CHAT_ID, photo=photo, caption=caption)
        logging.info("Hình ảnh đã được gửi lên Telegram")
    except Exception as e:
        logging.error(f"Lỗi khi gửi hình ảnh lên Telegram: {e}")
        raise

# Thực hiện các bước
async def main(query, caption):
    try:
        df = query_sql_server(query)
        image_path = "result.jpg"
        create_image_from_dataframe(df, image_path)
        await send_image_to_telegram(image_path, caption)
    except Exception as e:
        logging.error(f"Lỗi trong quá trình thực hiện: {e}")


if __name__ == "__main__":
    import sys
    query = sys.argv[1] if len(sys.argv) > 1 else "SELECT [PBH],[thucuoc],[cskh_ob],[cskh_ob_nc] as ob_nc,[cskh_sla_lonhon_72] as [sla > 72],[baohong],[tuvan] ,[td_tocdo],[tong_ton] FROM [dbo].[AutocallGiaHanTraTruoc]"
    caption = sys.argv[2] if len(sys.argv) > 2 else "AUTOCALL GIA HẠN TRẢ TRƯỚC ĐẾN HIỆN TẠI"
    asyncio.run(main(query, caption))