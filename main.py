# main.py
import asyncio
import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo
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
    list_queue as db_list_queue,
    delete_by_id,
    find_similar_ids,
    bulk_delete,
    is_duplicate,
)

# ---------------- Конфиг ----------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()  # @username или -100...
ADMINS_RAW = os.getenv("ADMINS", "").strip()
TZ = os.getenv("TZ", "Europe/Moscow")
POST_REPORT_TO_CHANNEL = os.getenv("POST_REPORT_TO_CHANNEL", "0").strip() == "1"

tz = ZoneInfo(TZ)
ADMINS: set[int] = set(int(x.strip()) for x in ADMINS_RAW.replace(";", ",").split(",") if x.strip().lstrip("-").isdigit())

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("layoutplace_bot")

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())

# ---------------- Утилиты ----------------
def _is_admin(m: Message) -> bool:
    return bool(m.from_user and m.from_user.id in ADMINS)

def _now_str() -> str:
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

async def safe_send_channel(text: str):
    """Сначала HTML; при ошибке — экранированный plain."""
    try:
        await bot.send_message(CHANNEL_ID, text, disable_web_page_preview=False)
    except TelegramBadRequest:
        await bot.send_message(CHANNEL_ID, html_escape(text), parse_mode=None, disable_web_page_preview=False)

async def _notify_admins(text: str):
    for uid in ADMINS:
        try:
            await bot.send_message(uid, text, disable_web_page_preview=True)
        except Exception:
            pass

# ---------------- Команды ----------------
@dp.message(Command("start"))
async def cmd_start(m: Message):
    help_text = (
        "Готов к работе.\n\n"
        "<b>Команды (только админы):</b>\n"
        "/enqueue &lt;текст&gt; — добавить объявление в очередь (с авто-очисткой и анти-дублем)\n"
        "/queue — показать размер очереди\n"
        "/queue_list [N] — показать N ближайших к публикации (по умолчанию 10)\n"
        "/delete &lt;id&gt; — удалить объявление по ID\n"
        "/post_oldest — опубликовать самое старое и удалить похожие\n"
        "/now — текущее время сервера\n"
    )
    await m.answer(help_text)

@dp.message(Command("myid"))
async def cmd_myid(m: Message):
    await m.answer(f"Твой Telegram ID: <code>{m.from_user.id}</code>")

@dp.message(Command("now"))
async def cmd_now(m: Message):
    await m.answer(f"<b>Серверное время:</b> {_now_str()} ({TZ})")

@dp.message(Command("enqueue"))
async def cmd_enqueue(m: Message, command: CommandObject):
    if not _is_admin(m):
        return await m.answer("Нет прав.")
    raw = (command.args or "").strip()
    if not raw:
        return await m.answer("Использование: /enqueue &lt;текст&gt;")

    # авто-очистка пробелов
    text = " ".join(raw.split())

    # анти-дубль по нормализованной форме
    dup_id = is_duplicate(text)
    if dup_id:
        return await m.answer(f"⚠️ Такой текст уже есть в очереди (ID: <code>{dup_id}</code>).")

    ad_id = db_enqueue(text)
    await m.answer(f"✅ Добавлено в очередь. ID: <code>{ad_id}</code>")

@dp.message(Command("queue"))
async def cmd_queue(m: Message):
    if not _is_admin(m):
        return await m.answer("Нет прав.")
    count = get_oldest(count_only=True)
    await m.answer(f"📦 В очереди: <b>{count}</b>")

@dp.message(Command("queue_list"))
async def cmd_queue_list(m: Message, command: CommandObject):
    if not _is_admin(m):
        return await m.answer("Нет прав.")
    # парсим N
    try:
        n = int((command.args or "").strip() or "10")
        n = max(1, min(50, n))
    except ValueError:
        n = 10
    items = db_list_queue(n)
    if not items:
        return await m.answer("Очередь пуста.")
    # отрисуем компактно
    lines = []
    for ad_id, text, created_at in items:
        when = datetime.fromtimestamp(created_at, tz).strftime("%d.%m %H:%M")
        preview = (text[:80] + "…") if len(text) > 80 else text
        lines.append(f"<code>{ad_id}</code> • {when} • {html_escape(preview)}")
    await m.answer("Первые в очереди:\n" + "\n".join(lines))

@dp.message(Command("delete"))
async def cmd_delete(m: Message, command: CommandObject):
    if not _is_admin(m):
        return await m.answer("Нет прав.")
    arg = (command.args or "").strip()
    if not arg or not arg.isdigit():
        return await m.answer("Использование: /delete &lt;id&gt;")
    ad_id = int(arg)
    removed = delete_by_id(ad_id)
    if removed:
        await m.answer(f"🗑 Удалено объявление <code>{ad_id}</code> из очереди.")
    else:
        await m.answer("Ничего не удалено (возможно, ID не существует).")

@dp.message(Command("post_oldest"))
async def cmd_post_oldest(m: Message):
    if not _is_admin(m):
        return await m.answer("Нет прав.")
    row = get_oldest()
    if not row:
        return await m.answer("Очередь пуста.")
    ad_id, text = row

    # пост в канал
    await safe_send_channel(text)

    # чистка похожих (включая исходный)
    similar = find_similar_ids(ad_id, threshold=0.88)
    removed = bulk_delete([ad_id] + similar)

    # отчёт админам
    now_h = _now_str()
    await _notify_admins(
        f"✅ Опубликовано ({now_h}). ID: <code>{ad_id}</code>. "
        f"Удалено похожих (включая исходный): <b>{removed}</b>."
    )

    # опционально — служебный лог в канал
    if POST_REPORT_TO_CHANNEL:
        await safe_send_channel(f"ℹ️ Пост опубликован. ID: {ad_id}. Удалено похожих: {removed}.")

    await m.answer(
        "✅ Опубликовано в канал.\n"
        f"🗑 Удалено (вместе с похожими): <b>{removed}</b>"
    )

# ---------------- Точка входа ----------------
async def main():
    init_db()
    log.info("✅ Бот запущен для %s (TZ=%s)", CHANNEL_ID, TZ)
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
