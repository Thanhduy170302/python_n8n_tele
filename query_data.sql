-- -- Chọn database
-- USE ctv_database;

-- -- Xem cấu trúc bảng
-- DESCRIBE ctv_dl;

-- -- Xem 10 dòng đầu tiên
-- SELECT * FROM ctv_dl LIMIT 10;

-- -- Đếm tổng số dòng
-- SELECT COUNT(*) as total_rows FROM ctv_dl;

-- -- Xem tên các cột
-- SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
-- FROM INFORMATION_SCHEMA.COLUMNS
-- WHERE TABLE_SCHEMA = 'ctv_database' 
-- AND TABLE_NAME = 'ctv_dl';

-- -- Kiểm tra dữ liệu null
-- SELECT COUNT(*) as null_count,
--        COLUMN_NAME
-- FROM ctv_dl
-- CROSS JOIN INFORMATION_SCHEMA.COLUMNS
-- WHERE TABLE_SCHEMA = 'ctv_database'
-- AND TABLE_NAME = 'ctv_dl'
-- AND COLUMN_NAME != 'id'
-- GROUP BY COLUMN_NAME
-- HAVING COUNT(*) > 0; 