# main.py
import asyncio
import logging
import os
from datetime import datetime
from html import escape as html_escape

from aiogram import Bot, Dispatcher
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

# ---------------- Конфиг ----------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()
ADMINS_RAW = os.getenv("ADMINS", "").strip()
TZ = os.getenv("TZ", "Europe/Moscow")

ADMINS: set[int] = set()
for part in ADMINS_RAW.replace(";", ",").split(","):
    p = part.strip()
    if not p:
        continue
    try:
        ADMINS.add(int(p))
    except ValueError:
        pass

# ---------------- Логирование ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("layoutplace_bot")

# ---------------- Бот ----------------
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())

# ---------------- Утилиты ----------------
def _is_admin(m: Message) -> bool:
    return bool(m.from_user and m.from_user.id in ADMINS)

async def safe_send_channel(text: str):
    """
    Шлём в канал безопасно: сначала как HTML, при ошибке — экранируем.
    """
    try:
        await bot.send_message(CHANNEL_ID, text, disable_web_page_preview=False)
    except TelegramBadRequest:
        await bot.send_message(CHANNEL_ID, html_escape(text), parse_mode=None, disable_web_page_preview=False)

# ---------------- Команды ----------------
@dp.message(Command("start"))
async def cmd_start(m: Message):
    help_text = (
        "Готов к работе.\n\n"
        "<b>Команды:</b>\n"
        "/myid — показать твой Telegram ID\n"
        "/enqueue &lt;текст&gt; — положить объявление в очередь (админы)\n"
        "/queue — показать размер очереди (админы)\n"
        "/post_oldest — опубликовать самое старое и удалить похожие (админы)\n"
        "/now — текущее время сервера\n"
    )
    await m.answer(help_text)

@dp.message(Command("myid"))
async def cmd_myid(m: Message):
    await m.answer(f"Твой Telegram ID: <code>{m.from_user.id}</code>")

@dp.message(Command("now"))
async def cmd_now(m: Message):
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    await m.answer(f"<b>Серверное время:</b> {now_str} ({TZ})")

@dp.message(Command("enqueue"))
async def cmd_enqueue(m: Message, command: CommandObject):
    if not _is_admin(m):
        return await m.answer("Нет прав.")
    text = (command.args or "").strip()
    if not text:
        return await m.answer("Использование: /enqueue &lt;текст&gt;")
    ad_id = db_enqueue(text)
    await m.answer(f"✅ Добавлено в очередь. ID: <code>{ad_id}</code>")

@dp.message(Command("queue"))
async def cmd_queue(m: Message):
    if not _is_admin(m):
        return await m.answer("Нет прав.")
    count = get_oldest(count_only=True)  # реализовано в storage/db.py
    await m.answer(f"📦 В очереди: <b>{count}</b>")

@dp.message(Command("post_oldest"))
async def cmd_post_oldest(m: Message):
    if not _is_admin(m):
        return await m.answer("Нет прав.")
    row = get_oldest()
    if not row:
        return await m.answer("Очередь пуста.")
    ad_id, text = row
    # пост в канал с защитой от HTML
    await safe_send_channel(text)
    # удалить похожие + сам пост
    similar = find_similar_ids(ad_id, threshold=0.88)
    removed = bulk_delete([ad_id] + similar)
    await m.answer(
        "✅ Опубликовано в канал.\n"
        f"🗑 Удалено из очереди (включая похожие): <b>{removed}</b>"
    )

# ---------------- Точка входа ----------------
async def main():
    # init_db у нас синхронная — без await
    init_db()
    log.info("✅ Бот запущен для %s (TZ=%s)", CHANNEL_ID, TZ)
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
