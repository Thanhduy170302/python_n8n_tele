import requests
import pandas as pd
import os

# URL cơ sở của API
BASE_URL = "http://localhost:8000"

def create_test_excel():
    """Tạo file Excel mẫu với dữ liệu tiếng Việt"""
    # Tạo dữ liệu mẫu cho danh sách CTV
    ctv_data = {
        'col_3': ['CTV001', 'CTV002', 'CTV003'],
        'col_14': ['Phòng bán hàng Bến Lức', 'Phòng bán hàng Cần Đước', 'Phòng bán hàng Cần Giuộc'],
        'col_17': ['2025', '2024', '2025'],
        'col_22': ['Đã xác thực', 'Đã xác thực', 'Đã xác thực']
    }
    
    # Tạo dữ liệu mẫu cho danh sách đơn hàng
    dh_data = {
        'col_12': ['CTV001', 'CTV002', 'CTV003'],
        'col_25': ['2025', '2025', '2025'],
        'col_27': ['Thành công', 'Thành công', 'Đang xử lý'],
        'col_2': ['DH001', 'DH002', 'DH003'],
        'col_3': ['SP001', 'SP002', 'SP003'],
        'col_19': ['Ghi chú 1', 'Ghi chú 2', 'Ghi chú có dấu tiếng Việt']
    }
    
    # Tạo DataFrame
    ctv_df = pd.DataFrame(ctv_data)
    dh_df = pd.DataFrame(dh_data)
    
    # Lưu file Excel
    ctv_df.to_excel('test_ctv.xlsx', index=False)
    dh_df.to_excel('test_dh.xlsx', index=False)
    
    return 'test_ctv.xlsx', 'test_dh.xlsx'

def test_upload_files():
    """Test upload file với dữ liệu tiếng Việt"""
    # Tạo file test
    ctv_file, dh_file = create_test_excel()
    
    try:
        # Test upload file CTV
        with open(ctv_file, 'rb') as f:
            response = requests.post(
                f"{BASE_URL}/upload/ctv",
                files={'file': ('test_ctv.xlsx', f, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')}
            )
        print(f"Upload CTV response: {response.status_code}")
        print(response.json())
        
        # Test upload file đơn hàng
        with open(dh_file, 'rb') as f:
            response = requests.post(
                f"{BASE_URL}/upload/donhang",
                files={'file': ('test_dh.xlsx', f, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')}
            )
        print(f"Upload đơn hàng response: {response.status_code}")
        print(response.json())
        
        # Test tạo báo cáo
        response = requests.post(f"{BASE_URL}/generate-report")
        print(f"Generate report response: {response.status_code}")
        print(response.json())
        
    finally:
        # Dọn dẹp file test
        for file in [ctv_file, dh_file]:
            if os.path.exists(file):
                os.remove(file)
        
        # Gọi endpoint cleanup
        response = requests.post(f"{BASE_URL}/cleanup")
        print(f"Cleanup response: {response.status_code}")
        print(response.json())

if __name__ == "__main__":
    test_upload_files() 