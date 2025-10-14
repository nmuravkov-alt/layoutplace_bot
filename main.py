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

# ---- наша конфигурация из ENV через config.py ----
from config import TOKEN as BOT_TOKEN, ADMINS as _ADMINS_LIST, CHANNEL_ID as _CHANNEL_ID, TZ as _TZ

# ---- функции из нашей текущей БД ----
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

# ---------------- ENV/CONFIG (экспорт для runner.py) ----------------
ADMINS: List[int] = [int(x) for x in _ADMINS_LIST] if isinstance(_ADMINS_LIST, list) else []
CHANNEL_ID: str | int = _CHANNEL_ID
TZ: str = _TZ
tz = ZoneInfo(TZ)

def now_str() -> str:
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

def is_admin(uid: Optional[int]) -> bool:
    return uid is not None and (int(uid) in ADMINS)

# ---------------- Унификация подписи ----------------
def unify_caption(text: str | None) -> str:
    """
    Приводим текст к общему формату: тире в цене, чистим пробелы/пустые строки,
    добавляем @layoutplacebuy если нет.
    """
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
# { admin_id: { mgid: {"source_chat_id": int, "ids": set[int], "caption": str, "last_ts": int} } }
ALBUM_CACHE: Dict[int, Dict[str, Dict[str, Any]]] = {}
# таймеры завершения: { (admin_id, mgid): asyncio.Task }
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
    """
    Авто-добавление альбома в очередь копирования после паузы DEBOUNCE_SEC.
    """
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

# Ловим форвард части альбома из канала
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
        "Готов к работе.\n\n"
        "<b>Команды</b>:\n"
        "/myid — показать твой Telegram ID\n"
        "/enqueue &lt;текст&gt; — положить <i>текстовое</i> объявление в очередь (старый режим)\n"
        "/queue — показать размер очередей\n"
        "/post_oldest — опубликовать самое старое (старый режим)\n"
        "/add_post &lt;ссылка-на-сообщение&gt; — добавить <i>одиночный</i> пост из канала в очередь копий\n"
        "/clear_albums_cache — очистить буфер альбомов\n"
        "/test_preview — проверить отправку уведомления админу\n"
        "/now — текущее время (TZ)\n\n"
        "💡 Альбомы добавляются <b>автоматически</b>: просто перешли боту <u>все фото альбома</u> из канала подряд."
    )
    await m.answer(help_text, disable_web_page_preview=True)

@dp.message(Command("myid"))
async def cmd_myid(m: Message):
    await m.answer(f"Твой Telegram ID: <code>{m.from_user.id}</code>", disable_web_page_preview=True)

@dp.message(Command("now"))
async def cmd_now(m: Message):
    await m.answer(f"Серверное время: <b>{now_str()}</b> ({TZ})", disable_web_page_preview=True)

@dp.message(Command("clear_albums_cache"))
async def cmd_clear_cache(m: Message):
    if not is_admin(m.from_user.id):
        return await m.answer("Нет прав.", disable_web_page_preview=True)
    ALBUM_CACHE.pop(m.from_user.id, None)
    # отменим таймеры конкретного админа
    to_cancel = [k for k in list(ALBUM_TIMERS.keys()) if k[0] == m.from_user.id]
    for key in to_cancel:
        t = ALBUM_TIMERS.pop(key, None)
        if t and not t.done():
            t.cancel()
    await m.answer("Буфер альбомов для тебя очищен.", disable_web_page_preview=True)

# --------- Старый текстовый режим (совместимость) ---------
@dp.message(Command("enqueue"))
async def cmd_enqueue(m: Message, command: CommandObject):
    if not is_admin(m.from_user.id):
        return await m.answer("Нет прав.", disable_web_page_preview=True)
    text = (command.args or "").strip()
    if not text:
        return await m.answer("Использование: /enqueue <текст объявления>", disable_web_page_preview=True)
    text = unify_caption(text)
    ad_id = db_enqueue(text)
    await m.answer(f"Добавлено в очередь: <code>{ad_id}</code>", disable_web_page_preview=True)

@dp.message(Command("queue"))
async def cmd_queue(m: Message):
    count_text = 1 if get_oldest() else 0  # индикатор наличия, детальный список не нужен
    count_copy = queue_count_pending()
    await m.answer(
        f"Очередь ТЕКСТ: <b>{count_text}</b>\n"
        f"Очередь КОПИЙ: <b>{count_copy}</b>",
        disable_web_page_preview=True
    )

@dp.message(Command("post_oldest"))
async def cmd_post_oldest(m: Message):
    if not is_admin(m.from_user.id):
        return await m.answer("Нет прав.", disable_web_page_preview=True)
    oldest = get_oldest()
    if not oldest:
        return await m.answer("Очередь пуста.", disable_web_page_preview=True)
    ad_id, text = oldest["id"], oldest["text"]
    await bot.send_message(CHANNEL_ID, text)  # простой текстовый пост в канал
    similar = find_similar_ids(ad_id, threshold=0.88)
    removed = bulk_delete([ad_id] + similar)
    await m.answer(
        f"Опубликовано объявление <code>{ad_id}</code> в канал.\n"
        f"Удалено из очереди объявлений: <b>{removed}</b> (включая похожие).",
        disable_web_page_preview=True
    )

# ---------------- Добавление одиночного поста по ссылке или реплаю ----------------
def _parse_link(arg: str):
    """
    Поддерживает:
      - https://t.me/username/123
      - https://t.me/c/CHATID/123  (CHATID без -100, восстановим)
    Возвращает (source_chat_id:int|str, [message_ids:list[int]])
    """
    arg = (arg or "").strip()
    if not arg.startswith("http"):
        return None
    try:
        parts = arg.split("/")
        if "/c/" in arg:
            idx = parts.index("c")
            raw = parts[idx+1]
            msg = int(parts[idx+2])
            chat_id = int("-100" + raw)
            return chat_id, [msg]
        else:
            username = parts[3]
            msg = int(parts[4])
            return username, [msg]
    except Exception:
        return None

@dp.message(Command("add_post"))
async def cmd_add_post(m: Message, command: CommandObject):
    if not is_admin(m.from_user.id):
        return await m.answer("Нет прав.", disable_web_page_preview=True)
    args = (command.args or "").strip()

    # Вариант 1: ответ на пересланное одиночное сообщение из канала
    if m.reply_to_message and m.reply_to_message.forward_from_chat:
        rm = m.reply_to_message
        if rm.media_group_id:
            return await m.answer("Это часть альбома. Просто перешли ВСЕ фото — бот добавит сам.", disable_web_page_preview=True)
        source_chat_id = rm.forward_from_chat.id
        fwd_mid = rm.forward_from_message_id
        if not fwd_mid:
            return await m.answer("Не вижу forward_from_message_id. Перешли сообщение из канала ещё раз.", disable_web_page_preview=True)
        qid = queue_add(
            source_chat_id=source_chat_id,
            message_ids=[int(fwd_mid)],
            caption_override=unify_caption(rm.caption or rm.text or "")
        )
        return await m.answer(f"Добавлено в очередь копирования: <code>{qid}</code>", disable_web_page_preview=True)

    # Вариант 2: по ссылке
    parsed = _parse_link(args) if args else None
    if not parsed:
        return await m.answer(
            "Использование:\n"
            "• Ответом на пересланное сообщение из канала: <code>/add_post</code>\n"
            "• Или: <code>/add_post https://t.me/username/123</code>\n"
            "     или <code>/add_post https://t.me/c/123456789/123</code>\n\n"
            "💡 Для альбомов — просто перешли все фото, бот добавит сам.",
            disable_web_page_preview=True
        )
    source_chat, mids = parsed
    qid = queue_add(
        source_chat_id=source_chat if isinstance(source_chat, int) else source_chat,
        message_ids=[int(x) for x in mids],
        caption_override=None
    )
    await m.answer(f"Добавлено в очередь копирования: <code>{qid}</code>", disable_web_page_preview=True)

# ---------------- Тест уведомления админу ----------------
@dp.message(Command("test_preview")))
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
            log.exception(f"Не удалось отправить сообщение админу {admin_id}: {e}")
            await message.answer(f"⚠️ Ошибка при отправке админу {admin_id}: {e}")
    if sent > 0:
        await message.answer("🔔 Уведомление успешно отправлено всем админам.", disable_web_page_preview=True)
    else:
        await message.answer("❌ Не удалось отправить уведомление ни одному админу.", disable_web_page_preview=True)

# ---------------- Точка входа ----------------
async def main():
    init_db()
    me = await bot.me()
    log.info("✅ Бот запущен: @%s (TZ=%s)", me.username, TZ)
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
