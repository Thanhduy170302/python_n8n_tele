from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# Thông tin kết nối đến Telegram bot
TELEGRAM_BOT_TOKEN = '6496568829:AAG38pueSVhN_Zw9rMQ-aKRBmOVxKdlHR9I'

# Hàm xử lý lệnh /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.message.chat_id
    await update.message.reply_text(f"Your chat ID is: {chat_id}")
    print(f"Chat ID: {chat_id}")

# Khởi tạo bot và thêm handler
def main() -> None:
    application = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))

    # Bắt đầu polling để nhận tin nhắn
    application.run_polling()

if __name__ == "__main__":
    main()