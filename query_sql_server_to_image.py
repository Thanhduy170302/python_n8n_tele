
import pypyodbc
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.table as tbl

# Thiết lập kết nối đến SQL Server bằng OLE DB
connection_string = (
    'DRIVER={ODBC Driver 17 for SQL Server};'  # Đảm bảo driver ODBC chính xác
    'SERVER=10.81.31.78;'                 # Tên máy chủ SQL Server của bạn
    'DATABASE=telegram;'             # Tên cơ sở dữ liệu của bạn
    'UID=sa;'                       # Tên đăng nhập của bạn
    'PWD=T@nkhanh123!@#;'                       # Mật khẩu của bạn
    'TrustServerCertificate=yes;'              # Thêm tùy chọn này để bỏ qua kiểm tra chứng chỉ SSL
    'Encrypt=yes;'                             # Bật mã hóa SSL
    'Connection Timeout=30;'
)

connection = pypyodbc.connect(connection_string)

# Tạo con trỏ để thực hiện truy vấn
cursor = connection.cursor()

# Viết câu truy vấn SQL
query = "SELECT  [DONVI_ID] ,[TEN_NGAN] ,[TEN] ,[MA_DV] FROM phongbanhang"

# Thực hiện truy vấn và lấy kết quả
cursor.execute(query)

# Chuyển kết quả truy vấn thành DataFrame
columns = [column[0] for column in cursor.description]
rows = cursor.fetchall()
df = pd.DataFrame.from_records(rows, columns=columns)

# Đóng kết nối
cursor.close()
connection.close()

# Tạo hình ảnh từ DataFrame
fig, ax = plt.subplots(figsize=(10, len(df) * 0.4))  # Điều chỉnh kích thước hình ảnh dựa trên số lượng hàng
ax.axis('tight')
ax.axis('off')

# Tạo bảng từ DataFrame
table = tbl.table(ax, df, loc='center', cellLoc='center', colWidths=[0.1] * len(df.columns))

# Định dạng bảng
table.auto_set_font_size(False)
table.set_fontsize(10)
table.scale(1.2, 1.2)

# Lưu hình ảnh dưới dạng tệp .jpg
plt.savefig("result.jpg", format="jpg")

# Hiển thị hình ảnh
plt.show()