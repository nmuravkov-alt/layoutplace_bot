# main.py
import asyncio
import logging
import os
from datetime import datetime
import re

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message
from aiogram.exceptions import TelegramBadRequest

from storage.db import (
    init_db,
    enqueue as db_enqueue,
    get_oldest,
    delete_by_id,
    find_similar_ids,
    bulk_delete,
)

# ==================== Конфиг из переменных окружения ====================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()
ADMINS_RAW = os.getenv("ADMINS", "").strip()  # id через запятую
TZ = os.getenv("TZ", "Europe/Moscow")

# ==================== Логирование ====================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==================== Инициализация ====================

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())

# ==================== Безопасная отправка ====================

ALLOWED_TAGS = {"b", "strong", "i", "em", "u", "s", "strike", "del", "tg-spoiler", "a", "code", "pre"}

def clean_html_tags(text: str) -> str:
    """Удаляет теги, не разрешённые Telegram."""
    def repl(m):
        tag = m.group(1).lower()
        if tag in ALLOWED_TAGS or tag.startswith("a "):
            return m.group(0)
        return ""
    return re.sub(r"</?([^ >/]+)[^>]*>", repl, text)

async def safe_send(bot, chat_id, text, **kwargs):
    """Безопасно отправляет сообщение — чистит HTML и ловит TelegramBadRequest."""
    text = clean_html_tags(text)
    try:
        await bot.send_message(chat_id, text, **kwargs)
    except TelegramBadRequest:
        await bot.send_message(chat_id, text, parse_mode=None)

# ==================== Команды ====================

@dp.message(Command("start"))
async def cmd_start(m: Message):
    help_text = (
        "Готов к работе.\n\n"
        "<b>Команды:</b>\n"
        "/myid — показать твой Telegram ID\n"
        "/enqueue &lt;текст&gt; — положить объявление в очередь\n"
        "/queue — показать размер очереди\n"
        "/post_oldest — опубликовать самое старое и удалить похожие\n"
        "/now — текущее время сервера\n"
    )
    await m.answer(help_text)

@dp.message(Command("myid"))
async def cmd_myid(m: Message):
    await m.answer(f"Твой Telegram ID: <code>{m.from_user.id}</code>")

@dp.message(Command("now"))
async def cmd_now(m: Message):
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    await m.answer(f"<b>Серверное время:</b> {now_str}")

@dp.message(Command("enqueue"))
async def cmd_enqueue(m: Message, command: CommandObject):
    if not command.args:
        await m.answer("❗ Введи текст объявления: /enqueue <текст>")
        return
    text = command.args.strip()
    db_enqueue(text)
    await m.answer("✅ Объявление добавлено в очередь")

@dp.message(Command("queue"))
async def cmd_queue(m: Message):
    count = get_oldest(count_only=True)
    await m.answer(f"📊 В очереди {count} объявлений")

@dp.message(Command("post_oldest"))
async def cmd_post_oldest(m: Message):
    ad = get_oldest()
    if not ad:
        await m.answer("❗ Очередь пуста")
        return
    text, ad_id = ad["text"], ad["id"]
    await safe_send(bot, CHANNEL_ID, text)
    delete_by_id(ad_id)
    await m.answer("✅ Опубликовано и удалено самое старое объявление")

# ==================== Запуск ====================

async def main():
    await init_db()
    logger.info("✅ Бот запущен для @layoutplace (TZ=%s)", TZ)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
