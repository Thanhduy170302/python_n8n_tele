import logging
import asyncio
from telegram import Bot

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Thông tin kết nối đến Telegram bot
TELEGRAM_BOT_TOKEN = '6496568829:AAG38pueSVhN_Zw9rMQ-aKRBmOVxKdlHR9I'

async def get_group_id():
    try:
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        updates = await bot.get_updates()
        for update in updates:
            if update.message and update.message.chat.type in ['group', 'supergroup']:
                logging.info(f"Group Name: {update.message.chat.title}")
                logging.info(f"Group ID: {update.message.chat.id}")
                return update.message.chat.id
        logging.info("Không tìm thấy nhóm nào.")
        return None
    except Exception as e:
        logging.error(f"Lỗi khi lấy ID nhóm: {e}")
        raise

if __name__ == "__main__":
    group_id = asyncio.run(get_group_id())
    if group_id:
        print(f"Group ID: {group_id}")
    else:
        print("Không tìm thấy ID của nhóm.")