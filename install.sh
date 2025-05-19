#!/bin/bash

# Cập nhật hệ thống
sudo apt update
sudo apt upgrade -y

# Cài đặt Python và pip
sudo apt install -y python3 python3-pip

# Cài đặt MySQL nếu chưa có
sudo apt install -y mysql-server

# Cài đặt các thư viện Python cần thiết
pip3 install -r requirements.txt

# Tạo thư mục logs
sudo mkdir -p /var/log/excel-import
sudo chown $USER:$USER /var/log/excel-import

# Copy service file
sudo cp /etc/systemd/system/excel-import.service /etc/systemd/system/

# Reload systemd
sudo systemctl daemon-reload

# Enable và start service
sudo systemctl enable excel-import
sudo systemctl start excel-import

# Kiểm tra trạng thái
sudo systemctl status excel-import 