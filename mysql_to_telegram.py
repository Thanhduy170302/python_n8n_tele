import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.table as tbl
from telegram import Bot
import logging
import tracemalloc
import asyncio
from telegram.error import TimedOut, NetworkError
import aiohttp
from datetime import datetime

# Kích hoạt tracemalloc để theo dõi phân bổ bộ nhớ
tracemalloc.start()

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Thông tin kết nối đến MySQL
mysql_config = {
    'host': 'localhost',      # Địa chỉ host MySQL của bạn
    'user': 'root',          # Tên đăng nhập MySQL của bạn
    'password': '123456', # Mật khẩu MySQL của bạn
    'database': 'ctv_database', # Tên database của bạn
    'port': 3306             # Port MySQL (mặc định là 3306)
}

# Thông tin kết nối đến Telegram bot
TELEGRAM_BOT_TOKEN = '8151971611:AAFwpuSJBfm3doTaNpuTquWOdMQOgFBMId0'
TELEGRAM_CHAT_ID = '-4701474696'
MAX_RETRIES = 3  # Số lần thử lại tối đa
TIMEOUT = 30     # Thời gian chờ tối đa (giây)

# Truy vấn MySQL và lấy kết quả
def query_mysql(query):
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

# Tạo hình ảnh từ DataFrame
def create_image_from_dataframe(df, output_path):
    try:
        if df.empty:
            logging.error("Không có dữ liệu để tạo hình ảnh")
            raise ValueError("DataFrame trống")
            
        logging.info("Đang tạo hình ảnh từ dữ liệu...")
        
        # Điều chỉnh kích thước của figure dựa trên số dòng
        rows = len(df)
        # Giới hạn kích thước tối đa để phù hợp với điện thoại
        max_height = min(rows * 0.4 + 1.2, 15.2)  # Tăng chiều cao tổng thể
        max_width = 14  # Giữ nguyên chiều rộng
        
        fig, (ax_title, ax) = plt.subplots(2, 1, figsize=(max_width, max_height), 
                                         gridspec_kw={'height_ratios': [0.3, 0.7]})  # Tăng tỷ lệ cho phần title
        
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
                     color='red')  # Thêm màu đỏ cho tiêu đề

        ax.axis('tight')
        ax.axis('off')

        # Tạo bảng với các cột cách đều nhau - điều chỉnh độ rộng theo nội dung
        colWidths = [0.04, 0.06, 0.07, 0.07, 0.09, 0.12, 0.11, 0.11, 0.11, 0.11, 0.11, 0.11]  # Điều chỉnh độ rộng cho cột tiếng Việt
        
        # Chuyển các giá trị số thành số nguyên
        for col in df.columns:
            if col == 'STT':
                # Sửa escape sequence warning
                df[col] = df[col].fillna('').astype(str).replace(r'\.0$', '', regex=True)
            elif col == 'PBH':
                df[col] = df[col].replace('TỔNG CỘNG', 'TỔNG\nCỘNG')
                continue
            elif col in ['Tỷ lệ CTVLK PTM 2025 PSSL 2025', 'Tỷ lệ CTVLK PTM 2024 PSSL 2025', 'Tỷ lệ CTVLK PSSL 2025']:
                # Giữ nguyên định dạng phần trăm
                continue
            else:
                # Chuyển đổi các cột số khác
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                # Replace 0 with "-"
                df[col] = df[col].replace(0, '-')

        # Chuyển DataFrame thành list để kiểm tra dữ liệu
        cell_data = df.values.tolist()
        header = df.columns.tolist()

        if not cell_data:
            logging.error("Không có dữ liệu trong DataFrame")
            raise ValueError("Không có dữ liệu để tạo bảng")

        # Tạo bảng với dữ liệu đã kiểm tra
        table = ax.table(cellText=cell_data,
                        colLabels=header,
                        cellLoc='center',
                        loc='center',
                        colWidths=colWidths)
        
        # Định dạng bảng
        table.auto_set_font_size(False)
        table.set_fontsize(8)  # Giảm kích thước font
        table.scale(1.2, 1.8)

        # Định dạng header
        header_height = 0.15  # Giảm chiều cao header
        for j, cell in enumerate(table._cells[(0, j)] for j in range(len(header))):
            cell.set_text_props(weight='bold')
            cell.set_facecolor('#f0f0f0')
            cell.set_text_props(color='black')
            cell.set_height(header_height)
            
            # Xử lý text trong header
            text = cell.get_text().get_text()
            # Rút gọn text
            if text == 'TỔNG SL CTVLK Hiện hữu':
                text = 'TỔNG SL CTVLK HH'
            elif 'SL CTVLK PTM' in text and 'PSSL' in text:
                year = '24' if '2024' in text else '25'
                text = f'CTVLK PTM{year} PS'
            elif 'TỔNG SL CTVLK PSSL' in text:
                text = 'TỔNG SL CTVLK PS'
            elif 'Tỷ lệ CTVLK PTM' in text and 'PSSL' in text:
                year = '24' if '2024' in text else '25'
                text = f'TL CTVLK PTM{year}'
            elif 'Tỷ lệ CTVLK PSSL' in text:
                text = 'TL CTVLK PS'
            elif 'PTM2025' in text:
                text = 'PTM25'
            elif 'PTM2024' in text:
                text = 'PTM24'
            
            cell.get_text().set_text(text)
            cell._text.set_wrap(True)
            cell.set_text_props(va='center', ha='center')

        # Định dạng các dòng với màu xen kẽ và căn giữa
        row_height = 0.1  # Chiều cao cho các dòng dữ liệu
        for i in range(len(df)):
            for j in range(len(header)):
                cell = table._cells[(i + 1, j)]
                cell.set_height(row_height)
                cell.set_text_props(fontsize=8)  # Đặt kích thước font cho dữ liệu
                
                # Màu nền cho các dòng
                if i == len(df) - 1:  # Dòng tổng cộng
                    cell.set_facecolor('#0072BC')  # Màu xanh dương đậm
                    cell.set_text_props(weight='bold', color='white')
                elif i % 2 == 1:  # Dòng lẻ
                    cell.set_facecolor('#E8F4E8')  # Màu xanh lá nhạt
                    cell.set_text_props(color='black')
                else:  # Dòng chẵn
                    cell.set_facecolor('white')
                    cell.set_text_props(color='black')

                # Định dạng đặc biệt cho cột tỷ lệ
                if header[j].startswith('Tỷ lệ') or header[j].startswith('TL'):
                    if cell.get_text().get_text() != '-':
                        val = float(cell.get_text().get_text().strip('%'))
                        if val > 50:  # Nếu tỷ lệ > 50%, tô màu xanh lá
                            cell.set_facecolor('#90EE90')  # Màu xanh lá nhạt

        plt.subplots_adjust(top=0.95, bottom=0.05, hspace=0.2)  # Điều chỉnh khoảng cách giữa các subplot
        plt.savefig(output_path, 
                   bbox_inches='tight', 
                   pad_inches=0.3,
                   format="jpg", 
                   dpi=200)
        plt.close(fig)
        logging.info(f"Đã lưu hình ảnh thành công tại {output_path}")
    except Exception as e:
        logging.error(f"Lỗi khi tạo hình ảnh: {str(e)}")
        raise

# Gửi ảnh lên Telegram với retry
async def send_image_to_telegram(image_path):
    retry_count = 0
    while retry_count < MAX_RETRIES:
        try:
            logging.info(f"Gửi hình ảnh lên Telegram (lần thử {retry_count + 1})")
            bot = Bot(token=TELEGRAM_BOT_TOKEN)
            with open(image_path, 'rb') as photo:
                await bot.send_photo(
                    chat_id=TELEGRAM_CHAT_ID,
                    photo=photo,
                    read_timeout=TIMEOUT,
                    write_timeout=TIMEOUT,
                    connect_timeout=TIMEOUT
                )
            logging.info("Hình ảnh đã được gửi lên Telegram thành công")
            return
        except (TimedOut, NetworkError) as e:
            retry_count += 1
            if retry_count < MAX_RETRIES:
                wait_time = 2 ** retry_count  # Tăng thời gian chờ theo cấp số nhân
                logging.warning(f"Lỗi kết nối, thử lại sau {wait_time} giây: {e}")
                await asyncio.sleep(wait_time)
            else:
                logging.error(f"Đã thử {MAX_RETRIES} lần không thành công: {e}")
                raise
        except Exception as e:
            logging.error(f"Lỗi không mong đợi khi gửi hình ảnh: {e}")
            raise

# Thực hiện các bước
async def main(query):
    try:
        df = query_mysql(query)
        image_path = "result.jpg"
        create_image_from_dataframe(df, image_path)
        await send_image_to_telegram(image_path)
    except Exception as e:
        logging.error(f"Lỗi trong quá trình thực hiện: {e}")

if __name__ == "__main__":
    import sys
    # Nếu không có tham số truyền vào, sử dụng câu truy vấn mặc định
    default_query = """
        SELECT 
            CAST(ROW_NUMBER() OVER (ORDER BY 
                CASE ctv.col_14
                    WHEN 'Phòng bán hàng Bến Lức' THEN 1
                    WHEN 'Phòng bán hàng Cần Đước' THEN 2
                    WHEN 'Phòng bán hàng Cần Giuộc' THEN 3
                    WHEN 'Phòng bán hàng Châu Thành' THEN 4
                    WHEN 'Phòng bán hàng Đức Hòa' THEN 5
                    WHEN 'Phòng bán hàng Đức Huệ' THEN 6
                    WHEN 'Phòng BHKV Kiến Tường - Mộc Hóa' THEN 7
                    WHEN 'Phòng BHKV Tân An' THEN 8
                    WHEN 'Phòng bán hàng Thạnh Hóa' THEN 9
                    WHEN 'Phòng bán hàng Tân Trụ' THEN 10
                    WHEN 'Phòng bán hàng Tân Thạnh' THEN 11
                    WHEN 'Phòng bán hàng Thủ Thừa' THEN 12
                    WHEN 'Phòng BHKV Vĩnh Hưng - Tân Hưng' THEN 13
                END
            ) AS UNSIGNED) as 'STT',
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
                END) as 'SL ĐH Phát sinh'
        FROM danh_sach_ctv_dl ctv
        LEFT JOIN (
            SELECT col_12, col_27, col_25
            FROM danhsachdonhang 
            WHERE col_27 = 'Thành công' 
            AND col_25 LIKE '%2025%'
        ) dh ON ctv.col_3 = dh.col_12
        LEFT JOIN (
            SELECT col_12, col_2, col_3, col_25, col_19
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
            NULL as 'STT',
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
                END) as 'SL ĐH Phát sinh'
        FROM danh_sach_ctv_dl ctv
        LEFT JOIN (
            SELECT col_12, col_27, col_25
            FROM danhsachdonhang 
            WHERE col_27 = 'Thành công' 
            AND col_25 LIKE '%2025%'
        ) dh ON ctv.col_3 = dh.col_12
        LEFT JOIN (
            SELECT col_12, col_2, col_3, col_25, col_19
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
        ORDER BY CASE WHEN STT IS NULL THEN 999999 ELSE STT END;
    """
    query = sys.argv[1] if len(sys.argv) > 1 else default_query
    asyncio.run(main(query)) 