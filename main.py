# main.py
import os
import asyncio
import logging
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any, List

from aiogram import Bot, Dispatcher, F, types
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message
from aiogram.filters import Command, CommandObject

# ---- конфиг ----
from config import TOKEN as BOT_TOKEN, ADMINS as _ADMINS_LIST, CHANNEL_ID as _CHANNEL_ID, TZ as _TZ

# ---- работа с базой ----
from storage.db import (
    init_db,
    db_enqueue,
    get_oldest,
    find_similar_ids,
    bulk_delete,
    queue_add,
    queue_count_pending,
)

# ---------------- ЛОГИ ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("layoutplace_bot")

# ---------------- CONFIG ----------------
ADMINS: List[int] = [469734432, 6773668793]  # твои ID
CHANNEL_ID: str | int = _CHANNEL_ID
TZ: str = _TZ
tz = ZoneInfo(TZ)

def now_str() -> str:
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

def is_admin(uid: Optional[int]) -> bool:
    return uid is not None and int(uid) in ADMINS

# ---------------- Унификация подписи ----------------
def unify_caption(text: str | None) -> str:
    text = (text or "").strip()
    text = text.replace("Цена -", "Цена —")
    while "  " in text:
        text = text.replace("  ", " ")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    text = "\n".join(lines)
    if "layoutplacebuy" not in text.lower():
        text += "\n\n@layoutplacebuy"
    return text

# ---------------- Инициализация бота ----------------
props = DefaultBotProperties(parse_mode=ParseMode.HTML)
bot = Bot(BOT_TOKEN, default=props)
dp = Dispatcher(storage=MemoryStorage())

# ---------------- АВТО-СБОР АЛЬБОМОВ ----------------
DEBOUNCE_SEC = 2.0
ALBUM_CACHE: Dict[int, Dict[str, Dict[str, Any]]] = {}
ALBUM_TIMERS: Dict[tuple, asyncio.Task] = {}

def _album_cache_touch(user_id: int, mgid: str, source_chat_id: int, fwd_mid: int, caption: Optional[str]):
    u = ALBUM_CACHE.setdefault(user_id, {})
    rec = u.get(mgid)
    if not rec:
        rec = {"source_chat_id": int(source_chat_id), "ids": set(), "caption": "", "last_ts": 0}
        u[mgid] = rec
    rec["ids"].add(int(fwd_mid))
    if caption and not rec["caption"]:
        rec["caption"] = caption
    rec["last_ts"] = int(time.time())

def _album_cache_pop(user_id: int, mgid: str) -> Optional[Dict[str, Any]]:
    u = ALBUM_CACHE.get(user_id)
    if not u:
        return None
    return u.pop(mgid, None)

def _cancel_timer(user_id: int, mgid: str):
    key = (user_id, mgid)
    t = ALBUM_TIMERS.pop(key, None)
    if t and not t.done():
        t.cancel()

async def _finalize_album(user_id: int, mgid: str):
    rec = _album_cache_pop(user_id, mgid)
    if not rec:
        return
    ids_sorted: List[int] = sorted(rec["ids"])
    qid = queue_add(
        source_chat_id=int(rec["source_chat_id"]),
        message_ids=ids_sorted,
        caption_override=unify_caption(rec.get("caption") or "")
    )
    try:
        await bot.send_message(
            chat_id=user_id,
            text=(f"✅ Альбом автоматически добавлен в очередь копирования: <code>{qid}</code>\n"
                  f"Элементов: <b>{len(ids_sorted)}</b>"),
            disable_web_page_preview=True
        )
    except Exception:
        pass

def _debounce_album_finalize(user_id: int, mgid: str):
    _cancel_timer(user_id, mgid)
    async def _task():
        await asyncio.sleep(DEBOUNCE_SEC)
        await _finalize_album(user_id, mgid)
    t = asyncio.create_task(_task())
    ALBUM_TIMERS[(user_id, mgid)] = t

@dp.message(F.forward_from_chat & F.media_group_id)
async def on_forwarded_album_piece(m: Message):
    if not is_admin(m.from_user.id):
        return
    try:
        source_chat_id = m.forward_from_chat.id
        fwd_mid = m.forward_from_message_id
        mgid = str(m.media_group_id)
        caption = m.caption or m.text or ""
        if source_chat_id and fwd_mid and mgid:
            _album_cache_touch(m.from_user.id, mgid, int(source_chat_id), int(fwd_mid), caption)
            _debounce_album_finalize(m.from_user.id, mgid)
    except Exception as e:
        logging.exception("Ошибка сбора альбома: %s", e)

# ---------------- БАЗОВЫЕ КОМАНДЫ ----------------
@dp.message(Command("start"))
async def cmd_start(m: Message):
    help_text = (
        "Бот готов к работе.\n\n"
        "<b>Команды:</b>\n"
        "/myid — показать твой Telegram ID\n"
        "/enqueue <текст> — добавить текст в очередь\n"
        "/queue — показать состояние очередей\n"
        "/post_oldest — опубликовать старое объявление\n"
        "/add_post — добавить одиночный пост из канала\n"
        "/clear_albums_cache — очистить буфер альбомов\n"
        "/test_preview — проверить уведомления админу\n"
        "/now — текущее время"
    )
    await m.answer(help_text, disable_web_page_preview=True)

@dp.message(Command("myid"))
async def cmd_myid(m: Message):
    await m.answer(f"Твой Telegram ID: <code>{m.from_user.id}</code>")

@dp.message(Command("now"))
async def cmd_now(m: Message):
    await m.answer(f"Серверное время: <b>{now_str()}</b> ({TZ})")

@dp.message(Command("clear_albums_cache"))
async def cmd_clear_cache(m: Message):
    if not is_admin(m.from_user.id):
        return await m.answer("Нет прав.")
    ALBUM_CACHE.pop(m.from_user.id, None)
    to_cancel = [k for k in list(ALBUM_TIMERS.keys()) if k[0] == m.from_user.id]
    for key in to_cancel:
        t = ALBUM_TIMERS.pop(key, None)
        if t and not t.done():
            t.cancel()
    await m.answer("Буфер альбомов очищен.")

@dp.message(Command("enqueue"))
async def cmd_enqueue(m: Message, command: CommandObject):
    if not is_admin(m.from_user.id):
        return await m.answer("Нет прав.")
    text = (command.args or "").strip()
    if not text:
        return await m.answer("Использование: /enqueue <текст>")
    text = unify_caption(text)
    ad_id = db_enqueue(text)
    await m.answer(f"Добавлено в очередь: <code>{ad_id}</code>")

@dp.message(Command("queue"))
async def cmd_queue(m: Message):
    count_text = 1 if get_oldest() else 0
    count_copy = queue_count_pending()
    await m.answer(f"Очередь текста: {count_text}\nОчередь копий: {count_copy}")

@dp.message(Command("post_oldest"))
async def cmd_post_oldest(m: Message):
    if not is_admin(m.from_user.id):
        return await m.answer("Нет прав.")
    oldest = get_oldest()
    if not oldest:
        return await m.answer("Очередь пуста.")
    ad_id, text = oldest["id"], oldest["text"]
    await bot.send_message(CHANNEL_ID, text)
    similar = find_similar_ids(ad_id, threshold=0.88)
    removed = bulk_delete([ad_id] + similar)
    await m.answer(f"Опубликовано: {ad_id}\nУдалено из очереди: {removed}")

# ---------------- Тест уведомления админу ----------------
@dp.message(Command("test_preview"))
async def cmd_test_preview(message: types.Message):
    sent = 0
    for admin_id in ADMINS:
        try:
            await bot.send_message(
                admin_id,
                f"✅ Тестовое уведомление админу работает!\n🕒 {now_str()}",
                disable_web_page_preview=True
            )
            sent += 1
        except Exception as e:
            log.exception(f"Ошибка при отправке админу {admin_id}: {e}")
            await message.answer(f"⚠️ Ошибка при отправке админу {admin_id}: {e}")
    if sent > 0:
        await message.answer("🔔 Уведомление успешно отправлено всем админам.")
    else:
        await message.answer("❌ Не удалось отправить уведомления.")

# ---------------- Точка входа ----------------
async def main():
    init_db()
    me = await bot.me()
    log.info("✅ Бот запущен: @%s (TZ=%s)", me.username, TZ)
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
