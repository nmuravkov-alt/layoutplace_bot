# main.py
import asyncio
import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ChatType
from aiogram.filters import Command, CommandObject
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message

from storage.db import (
    init_db,
    enqueue as db_enqueue,
    get_oldest,
    delete_by_id,
    find_similar_ids,
    bulk_delete,
)

# -------------------- Конфиг из окружения --------------------

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()   # @username канала или -100...
ADMINS_RAW = os.getenv("ADMINS", "").strip()       # список id через запятую
TZ = os.getenv("TZ", "Europe/Moscow")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")

try:
    tz = ZoneInfo(TZ)
except Exception:
    tz = ZoneInfo("UTC")

def _parse_admins(raw: str) -> set[int]:
    ids: set[int] = set()
    for part in raw.replace(";", ",").split(","):
        p = part.strip()
        if not p:
            continue
        try:
            ids.add(int(p))
        except ValueError:
            pass
    return ids

ADMINS: set[int] = _parse_admins(ADMINS_RAW)

def now_str() -> str:
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

# -------------------- Инициализация бота ---------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s:%(name)s:%(message)s",
)
log = logging.getLogger("layoutplace_bot")

bot = Bot(
    BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher(storage=MemoryStorage())

# -------------------- Хелперы -------------------------------

def is_admin(message: Message) -> bool:
    uid = message.from_user.id if message.from_user else 0
    return uid in ADMINS

async def send_to_channel(text: str) -> None:
    """Пост в канал."""
    await bot.send_message(CHANNEL_ID, text)

# -------------------- Команды -------------------------------

@dp.message(Command("start"))
async def cmd_start(m: Message):
    help_text = (
        "Готов к работе.\n\n"
        "<b>Команды</b>:\n"
        "/myid — показать твой Telegram ID\n"
        "/enqueue текст — положить объявление в очередь\n"
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
    await m.answer(f"<b>Серверное время:</b> {now_str()} ({TZ})")

@dp.message(Command("enqueue"))
async def cmd_enqueue(m: Message, command: CommandObject):
    if not is_admin(m):
        await m.answer("Нет прав.")
        return
    # Берём текст после команды
    # примеры: "/enqueue Текст..." или "/enqueue\nТекст..."
    text_after = m.text.split(maxsplit=1)
    text = text_after[1].strip() if len(text_after) > 1 else ""
    if not text:
        await m.answer("Нужно передать текст объявления. Пример:\n/enqueue Джинсы L, 3500 ₽ #штаны")
        return

    ad_id = db_enqueue(text)
    await m.answer(f"OK, объявление добавлено в очередь. ID: <code>{ad_id}</code>")

@dp.message(Command("queue"))
async def cmd_queue(m: Message):
    if not is_admin(m):
        await m.answer("Нет прав.")
        return
    count = get_oldest(count_only=True)  # благодаря обновлённому db.py
    await m.answer(f"В очереди: <b>{count}</b>.")

@dp.message(Command("post_oldest"))
async def cmd_post_oldest(m: Message):
    if not is_admin(m):
        await m.answer("Нет прав.")
        return

    oldest = get_oldest()
    if not oldest:
        await m.answer("Очередь пуста.")
        return

    ad_id, text = oldest
    # Постим в канал
    await send_to_channel(text)

    # Удаляем похожие + сам пост
    similar = find_similar_ids(ad_id, threshold=0.88)
    removed = bulk_delete([ad_id] + similar)

    await m.answer(
        f"Опубликовано объявление <code>{ad_id}</code> в канал.\n"
        f"Удалено из очереди объявлений: <b>{removed}</b> (включая похожие)."
    )

# -------------------- Точка входа ----------------------------

async def main():
    init_db()
    log.info("✅ Бот запущен для @%s (TZ=%s)", CHANNEL_ID.strip("@"), TZ)
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
