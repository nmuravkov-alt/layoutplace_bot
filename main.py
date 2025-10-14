import asyncio
import logging
from datetime import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message

from config import TOKEN, ADMINS
from storage.db import init_db, add_post, get_all, get_oldest, delete_post

# Настройка логов
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Инициализация бота и диспетчера
bot = Bot(token=TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()

# ================= Команда /start =================
@dp.message(Command("start"))
async def cmd_start(m: Message):
    help_text = (
        "👋 Привет! Это бот для управления очередью постов.\n\n"
        "Команды:\n"
        "/add — добавить новое объявление\n"
        "/queue — посмотреть очередь постов\n"
        "/test_preview — проверить уведомления админу\n\n"
        "Постинг выполняется автоматически по расписанию."
    )
    await m.answer(help_text)

# ================= Добавление объявления =================
@dp.message(Command("add"))
async def cmd_add(m: Message):
    if not m.reply_to_message or not m.reply_to_message.caption:
        await m.answer("❌ Ответьте этой командой на сообщение с фотографией и подписью.")
        return

    try:
        photo = m.reply_to_message.photo[-1].file_id
        caption = m.reply_to_message.caption
        await add_post(photo, caption)
        await m.answer("✅ Объявление добавлено в очередь!")
    except Exception as e:
        log.exception(e)
        await m.answer(f"⚠️ Ошибка при добавлении объявления: {e}")

# ================= Очередь постов =================
@dp.message(Command("queue"))
async def cmd_queue(m: Message):
    posts = get_all()
    if not posts:
        await m.answer("📭 Очередь пуста.")
        return

    text = "<b>📋 Очередь постов:</b>\n\n"
    for i, post in enumerate(posts, start=1):
        text += f"{i}. {post['caption'][:60]}...\n"

    await m.answer(text)

# ================= Тест уведомления админу =================
@dp.message(Command("test_preview"))
async def cmd_test_preview(message: types.Message):
    """Проверка отправки уведомления админу"""
    sent = 0
    for admin_id in ADMINS:
        try:
            await bot.send_message(
                admin_id,
                f"✅ Тестовое уведомление админу работает!\n🕒 {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"
            )
            sent += 1
        except Exception as e:
            log.exception(f"Не удалось отправить сообщение админу {admin_id}: {e}")
            await message.answer(f"⚠️ Ошибка при отправке админу {admin_id}: {e}")
    if sent > 0:
        await message.answer("🔔 Уведомление успешно отправлено всем админам.")
    else:
        await message.answer("❌ Не удалось отправить уведомление ни одному админу.")

# ================= Запуск =================
async def main():
    await init_db()
    log.info(f"✅ Бот запущен для @{(await bot.me()).username} (TZ=Europe/Moscow)")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
