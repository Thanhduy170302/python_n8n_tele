import pyodbc

# Thiết lập kết nối đến SQL Server
connection = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'  # Đảm bảo driver ODBC chính xác
    'SERVER=10.81.31.78;'                 # Tên máy chủ SQL Server của bạn
    'DATABASE=telegram;'             # Tên cơ sở dữ liệu của bạn
    'UID=sa;'                       # Tên đăng nhập của bạn
    'PWD=T@nkhanh123!@#;'                       # Mật khẩu của bạn
    'TrustServerCertificate=yes;'              # Thêm tùy chọn này để bỏ qua kiểm tra chứng chỉ SSL
    'Encrypt=yes;'                             # Bật mã hóa SSL
    'Connection Timeout=30;'                   # Thiết lập thời gian chờ kết nối
)

# Tạo con trỏ để thực hiện truy vấn
cursor = connection.cursor()

# Viết câu truy vấn SQL
query = "SELECT * FROM phongbanhang"

# Thực hiện truy vấn và lấy kết quả
cursor.execute(query)

# Lấy tất cả các hàng từ kết quả truy vấn
rows = cursor.fetchall()

# Hiển thị kết quả
for row in rows:
    print(row)

# Đóng kết nối
cursor.close()
connection.close()