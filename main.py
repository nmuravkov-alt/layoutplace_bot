# main.py
import asyncio
import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from html import escape as _escape

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
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

# -------------------- Настройки окружения --------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()
ADMINS_RAW = os.getenv("ADMINS", "").strip()
TZ = os.getenv("TZ", "Europe/Moscow")

if not BOT_TOKEN:
    raise RuntimeError("❌ BOT_TOKEN не задан!")
if not CHANNEL_ID:
    raise RuntimeError("❌ CHANNEL_ID не задан!")

ADMINS: set[int] = set()
for piece in (ADMINS_RAW or "").replace(" ", "").split(","):
    if piece:
        try:
            ADMINS.add(int(piece))
        except ValueError:
            pass

# -------------------- Настройка логов --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("layoutplace_bot")

# -------------------- Вспомогательные функции --------------------
def safe_html(text: str) -> str:
    """Экранирует HTML, чтобы Telegram не ругался на < >"""
    return _escape(text, quote=False)

def now_str() -> str:
    tz = ZoneInfo(TZ)
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

def is_admin(m: Message) -> bool:
    return m.from_user and m.from_user.id in ADMINS

async def send_to_channel(bot: Bot, text: str):
    await bot.send_message(
        chat_id=CHANNEL_ID,
        text=safe_html(text),
        disable_web_page_preview=False,
    )

def _ad_fields(ad) -> tuple[int, str]:
    if ad is None:
        return (0, "")
    if isinstance(ad, dict):
        return int(ad.get("id", 0)), str(ad.get("text", ""))
    try:
        return int(ad[0]), str(ad[1])
    except Exception:
        return (0, "")

async def post_oldest_and_cleanup(bot: Bot, reply_to: Message | None = None):
    ad = get_oldest()
    ad_id, ad_text = _ad_fields(ad)
    if not ad_id or not ad_text.strip():
        msg = "Очередь пуста — нечего постить."
        if reply_to:
            await reply_to.answer(msg)
        else:
            log.info(msg)
        return

    await send_to_channel(bot, ad_text)
    similar_ids = set(find_similar_ids(ad_text) or [])
    similar_ids.add(ad_id)
    bulk_delete(list(similar_ids))

    msg = f"✅ Опубликовано и удалено {len(similar_ids)} объявлений."
    if reply_to:
        await reply_to.answer(msg)
    else:
        log.info(msg)

# -------------------- Основной запуск --------------------
async def main():
    init_db()

    bot = Bot(
        BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher(storage=MemoryStorage())

    @dp.message(Command("start"))
    async def cmd_start(m: Message):
        text = (
            "✅ Готов к работе.\n\n"
            "<b>Команды:</b>\n"
            "/myid — показать твой Telegram ID\n"
            "/enqueue <текст> — добавить объявление в очередь\n"
            "/post_oldest — опубликовать самое старое объявление\n"
            "/queue — показать количество объявлений в базе\n"
            "/now — текущее время сервера\n"
        )
        await m.answer(text)

    @dp.message(Command("myid"))
    async def cmd_myid(m: Message):
        await m.answer(f"Твой Telegram ID: <code>{m.from_user.id}</code>")

    @dp.message(Command("now"))
    async def cmd_now(m: Message):
        await m.answer(f"🕒 Серверное время: <b>{now_str()}</b>")

    @dp.message(Command("enqueue"))
    async def cmd_enqueue(m: Message, command: CommandObject):
        if not is_admin(m):
            return await m.answer("⛔ Нет прав.")

        text = (command.args or "").strip()
        if not text:
            return await m.answer("Используй: /enqueue ТЕКСТ_ОБЪЯВЛЕНИЯ")

        db_enqueue(text)
        await m.answer("✅ Добавлено в очередь.")

    @dp.message(Command("post_oldest"))
    async def cmd_post_oldest(m: Message):
        if not is_admin(m):
            return await m.answer("⛔ Нет прав.")
        await post_oldest_and_cleanup(bot, reply_to=m)

    @dp.message(Command("queue"))
    async def cmd_queue(m: Message):
        if not is_admin(m):
            return await m.answer("⛔ Нет прав.")
        from storage.db import _cx
        with _cx() as cx:
            count = cx.execute("SELECT COUNT(*) FROM ads").fetchone()[0]
        await m.answer(f"📦 Сейчас в очереди: <b>{count}</b> объявлений.")

    log.info(f"✅ Бот запущен для {CHANNEL_ID} (TZ={TZ})")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
