from telegram import Bot
import logging

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Thông tin kết nối đến Telegram bot
TELEGRAM_BOT_TOKEN = '6496568829:AAG38pueSVhN_Zw9rMQ-aKRBmOVxKdlHR9I'
TELEGRAM_CHAT_ID = '6653901323'

# Gửi tin nhắn thử nghiệm lên Telegram
def send_test_message():
    logging.info("Gửi tin nhắn thử nghiệm lên Telegram")
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    bot.send_message(chat_id=TELEGRAM_CHAT_ID, text="Đây là tin nhắn thử nghiệm từ bot Telegram")
    logging.info("Tin nhắn thử nghiệm đã được gửi lên Telegram")

# Thực hiện gửi tin nhắn
if __name__ == "__main__":
    send_test_message()