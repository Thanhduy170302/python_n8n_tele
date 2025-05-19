import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.table as tbl
from telegram import Bot
import logging
import tracemalloc
import asyncio
from telegram.error import TimedOut, NetworkError
from datetime import datetime

# Kích hoạt tracemalloc để theo dõi phân bổ bộ nhớ
tracemalloc.start()

# Cấu hình logging để ghi nhật ký hoạt động
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'  # Thêm hỗ trợ tiếng Việt cho logging
)

# Thông tin kết nối đến cơ sở dữ liệu MySQL
mysql_config = {
    'host': 'localhost',      # Địa chỉ máy chủ MySQL
    'user': 'root',          # Tên người dùng MySQL
    'password': '123456',    # Mật khẩu MySQL
    'database': 'ctv_database', # Tên cơ sở dữ liệu
    'port': 3306,            # Cổng kết nối MySQL (mặc định là 3306)
    'charset': 'utf8mb4'     # Hỗ trợ tiếng Việt và emoji
}

# Thông tin kết nối đến bot Telegram
TELEGRAM_BOT_TOKEN = '8151971611:AAFwpuSJBfm3doTaNpuTquWOdMQOgFBMId0'  # Token của bot
TELEGRAM_CHAT_ID = '-4701474696'  # ID của nhóm/kênh Telegram
MAX_RETRIES = 3  # Số lần thử lại tối đa khi gửi ảnh
TIMEOUT = 30     # Thời gian chờ tối đa (giây)

# Hàm truy vấn dữ liệu từ MySQL và chuyển thành DataFrame
def query_mysql(query):
    """
    Thực hiện truy vấn MySQL và trả về kết quả dưới dạng DataFrame
    
    Args:
        query (str): Câu truy vấn SQL
        
    Returns:
        DataFrame: Kết quả truy vấn
    """
    try:
        logging.info("Đang kết nối đến cơ sở dữ liệu MySQL...")
        connection = mysql.connector.connect(**mysql_config)
        cursor = connection.cursor()
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame.from_records(rows, columns=columns)
        cursor.close()
        connection.close()
        logging.info(f"Đã truy vấn thành công, nhận được {len(df)} dòng dữ liệu")
        return df
    except Exception as e:
        logging.error(f"Lỗi khi truy vấn MySQL: {e}")
        raise

# Hàm tạo hình ảnh từ DataFrame
def create_image_from_dataframe(df, output_path):
    """
    Tạo hình ảnh từ DataFrame và lưu vào file
    
    Args:
        df (DataFrame): DataFrame chứa dữ liệu
        output_path (str): Đường dẫn file ảnh đầu ra
    """
    try:
        if df.empty:
            logging.error("DataFrame trống, không thể tạo hình ảnh")
            raise ValueError("Không có dữ liệu để tạo hình ảnh")
            
        logging.info("Đang tạo hình ảnh từ dữ liệu...")
        
        # Tính toán kích thước hình ảnh
        rows = len(df)
        max_height = min(rows * 0.4 + 1.2, 15.2)  # Giới hạn chiều cao tối đa
        max_width = 14  # Chiều rộng cố định
        
        # Tạo figure với 2 phần: tiêu đề và bảng dữ liệu
        fig, (ax_title, ax) = plt.subplots(2, 1, figsize=(max_width, max_height), 
                                         gridspec_kw={'height_ratios': [0.3, 0.7]})
        
        # Thêm tiêu đề
        ax_title.axis('off')
        current_date = datetime.now().strftime("%d.%m.%Y")
        title_text = f"Báo cáo kết quả triển khai Mô hình CTV LK Năm 2025\n(01.01.2025 - {current_date})"
        ax_title.text(0.5, 0.5, title_text, 
                     fontsize=12, 
                     fontweight='bold',
                     ha='center',
                     va='center',
                     linespacing=1.5,
                     color='red')

        ax.axis('tight')
        ax.axis('off')

        # Định nghĩa độ rộng cho từng cột
        colWidths = [0.04, 0.06, 0.07, 0.07, 0.09, 0.12, 0.11, 0.11, 0.11, 0.11, 0.11, 0.11]
        
        # Chuẩn bị dữ liệu cho bảng
        cell_data = df.values.tolist()
        header = df.columns.tolist()

        if not cell_data:
            logging.error("Không có dữ liệu để tạo bảng")
            raise ValueError("DataFrame không có nội dung")

        # Tạo và định dạng bảng
        table = ax.table(cellText=cell_data,
                        colLabels=header,
                        cellLoc='center',
                        loc='center',
                        colWidths=colWidths)
        
        table.auto_set_font_size(False)
        table.set_fontsize(8)
        table.scale(1.2, 1.8)

        # Lưu hình ảnh
        plt.savefig(output_path, 
                   bbox_inches='tight', 
                   pad_inches=0.3,
                   format="jpg", 
                   dpi=200)
        plt.close(fig)
        logging.info(f"Đã lưu hình ảnh thành công tại: {output_path}")
    except Exception as e:
        logging.error(f"Lỗi khi tạo hình ảnh: {str(e)}")
        raise

# Hàm gửi ảnh lên Telegram
async def send_image_to_telegram(image_path):
    """
    Gửi ảnh lên Telegram với cơ chế thử lại nếu thất bại
    
    Args:
        image_path (str): Đường dẫn đến file ảnh cần gửi
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
                    read_timeout=TIMEOUT,
                    write_timeout=TIMEOUT,
                    connect_timeout=TIMEOUT
                )
            logging.info("Đã gửi ảnh lên Telegram thành công")
            return
        except (TimedOut, NetworkError) as e:
            retry_count += 1
            if retry_count < MAX_RETRIES:
                wait_time = 2 ** retry_count  # Tăng thời gian chờ theo cấp số nhân
                logging.warning(f"Lỗi kết nối, sẽ thử lại sau {wait_time} giây: {e}")
                await asyncio.sleep(wait_time)
            else:
                logging.error(f"Đã thử gửi {MAX_RETRIES} lần không thành công: {e}")
                raise
        except Exception as e:
            logging.error(f"Lỗi không xác định khi gửi ảnh: {e}")
            raise

# Hàm xóa các bảng MySQL
def delete_mysql_tables(tables):
    """
    Xóa các bảng được chỉ định khỏi cơ sở dữ liệu MySQL
    
    Args:
        tables (list): Danh sách tên các bảng cần xóa
    """
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
        logging.info(f"Đã xóa thành công {len(tables)} bảng")
    except Exception as e:
        logging.error(f"Lỗi khi thao tác với MySQL: {e}")
        raise

# Hàm chính thực hiện toàn bộ quy trình
async def main(query, tables_to_delete):
    """
    Thực hiện quy trình: truy vấn dữ liệu -> tạo ảnh -> gửi lên Telegram -> xóa bảng
    
    Args:
        query (str): Câu truy vấn SQL
        tables_to_delete (list): Danh sách các bảng cần xóa
    """
    try:
        # Bước 1: Truy vấn dữ liệu
        logging.info("Bắt đầu quy trình xử lý...")
        df = query_mysql(query)
        
        # Bước 2: Tạo và gửi hình ảnh
        image_path = "result.jpg"
        create_image_from_dataframe(df, image_path)
        await send_image_to_telegram(image_path)
        
        # Bước 3: Xóa các bảng
        logging.info("Bắt đầu xóa các bảng sau khi đã gửi ảnh thành công...")
        delete_mysql_tables(tables_to_delete)
        
        logging.info("Hoàn thành toàn bộ quy trình!")
        
    except Exception as e:
        logging.error(f"Lỗi trong quá trình thực hiện: {e}")
        raise

if __name__ == "__main__":
    # Câu truy vấn mặc định
    default_query = """
    SELECT * FROM danh_sach_ctv_dl
    """
    
    # Danh sách các bảng cần xóa sau khi hoàn thành
    tables_to_delete = ['danh_sach_ctv_dl', 'danhsachdonhang']
    
    # Chạy chương trình
    logging.info("Khởi động chương trình...")
    asyncio.run(main(default_query, tables_to_delete)) 