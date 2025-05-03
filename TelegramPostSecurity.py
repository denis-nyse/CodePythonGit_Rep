import asyncio
from telethon import TelegramClient
from datetime import datetime, timedelta
import logging
import re
from telegram import Bot, Update
from telegram.ext import Application, CommandHandler, ContextTypes

# Настройки
TELEGRAM_API_ID = '###'
TELEGRAM_API_HASH = '###'
BOT_TOKEN = '###' # Бот для постов по телеграм каналам Кибербезопасность
CHANNELS_FILE = './channelSecurity.txt'
SUMMARY_FILE = 'sum_posts.txt'

# Настройка логгирования
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Глобальная переменная для хранения chat_id пользователя
user_chat_id = None

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    global user_chat_id
    user_chat_id = update.effective_chat.id
    await update.message.reply_text(
        "🤖 Бот активирован!\n"
        "Я буду присылать вам саммари новых постов из отслеживаемых каналов."
    )
    logger.info(f"Бот запущен пользователем: {user_chat_id}")

def simple_summarize(text, chars=300):
    """Упрощенная генерация саммари"""
    text = re.sub(r'http\S+|@\w+|#\w+', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text[:chars] + ('...' if len(text) > chars else '')

async def send_to_user():
    """Отправка саммари пользователю"""
    if not user_chat_id:
        logger.warning("Пользователь не запустил бота командой /start")
        return

    try:
        bot = Bot(token=BOT_TOKEN)

        with open(SUMMARY_FILE, 'r', encoding='utf-8') as f:
            content = f.read()

        if not content.strip():
            await bot.send_message(chat_id=user_chat_id, text="ℹ️ Новых постов не найдено")
            return

        messages = content.split('\n\n')
        for msg in messages:
            if msg.strip():
                await bot.send_message(chat_id=user_chat_id, text=msg)
                await asyncio.sleep(1)  # Задержка между сообщениями

        logger.info(f"Саммари отправлены пользователю {user_chat_id}")
    except Exception as e:
        logger.error(f"Ошибка отправки: {e}")

async def monitor_channels():
    """Мониторинг каналов пакетами по 10 и отправка уведомлений"""
    client = TelegramClient('session_name', TELEGRAM_API_ID, TELEGRAM_API_HASH)

    try:
        await client.start()

        # Очистка итогового файла
        open(SUMMARY_FILE, 'w').close()

        with open(CHANNELS_FILE, 'r', encoding='utf-8') as f:
            all_channels = [line.strip() for line in f if line.strip()]

        logger.info(f"Всего каналов к обработке: {len(all_channels)}")

        for i in range(0, len(all_channels), 10):
            chunk = all_channels[i:i + 10]
            logger.info(f"Обработка каналов {i + 1} - {i + len(chunk)}")

            summaries = []

            for channel in chunk:
                try:
                    now = datetime.now()
                    time_threshold = now - timedelta(hours=24)

                    channel_entity = await client.get_entity(channel)

                    async for message in client.iter_messages(channel_entity):
                        if message.date.replace(tzinfo=None) < time_threshold:
                            break

                        if message.text:
                            post_link = f"https://t.me/{channel}/{message.id}"
                            summary = simple_summarize(message.text)
                            summaries.append(
                                f"📌 Канал: {channel}\n"
                                f"🕒 {message.date.strftime('%Y-%m-%d %H:%M')}\n"
                                f"🔗 {post_link}\n"
                                f"📝 {summary}\n\n"
                            )

                except Exception as e:
                    logger.error(f"Ошибка в канале {channel}: {e}")

                await asyncio.sleep(1)  # Задержка между каналами

            # Записываем саммари чанка
            if summaries:
                with open(SUMMARY_FILE, 'a', encoding='utf-8') as f:
                    f.writelines(summaries)

        # Отправляем все собранные саммари пользователю
        await send_to_user()

    except Exception as e:
        logger.error(f"Ошибка мониторинга: {e}")
    finally:
        await client.disconnect()

async def main():
    """Запуск бота и однократного мониторинга"""
    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))

    await application.initialize()
    await application.start()
    await application.updater.start_polling()

    await monitor_channels()

if __name__ == '__main__':
    asyncio.run(main())