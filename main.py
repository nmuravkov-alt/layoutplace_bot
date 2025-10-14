# main.py
import os
import asyncio
import logging
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message
from aiogram.filters import Command, CommandObject

from storage.db import (
    init_db,
    db_enqueue,
    get_oldest,
    delete_by_id,
    find_similar_ids,
    bulk_delete,
    queue_add,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("layoutplace_bot")

# ---------------- ENV ----------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()  # @username или -100...
ADMINS_RAW = os.getenv("ADMINS", "").strip()
ADMINS = [a.strip() for a in ADMINS_RAW.split(",") if a.strip()]
TZ_NAME = os.getenv("TZ", "Europe/Moscow")
tz = ZoneInfo(TZ_NAME)

def now_str() -> str:
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

def is_admin(uid: Optional[int]) -> bool:
    return uid is not None and (str(uid) in ADMINS)

def unify_caption(text: str | None) -> str:
    """
    Приводим подпись к единому формату.
    """
    text = (text or "").strip()
    # косметика тире/пробелов
    text = text.replace("Цена -", "Цена —")
    text = text.replace("  ", " ")
    # убрать пустые строки
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    text = "\n".join(lines)
    # добавить контакт, если нет
    if "layoutplacebuy" not in text:
        text += "\n\n@layoutplacebuy"
    return text

props = DefaultBotProperties(parse_mode=ParseMode.HTML, disable_web_page_preview=True)
bot = Bot(BOT_TOKEN, default=props)
dp = Dispatcher(storage=MemoryStorage())

# ---------------- КЭШ АЛЬБОМОВ (последний альбом, который админ переслал боту) ----------------
# Храним ПОСЛЕДНИЙ альбом от каждого админа:
# { admin_id: {"media_group_id": int, "source_chat_id": int, "message_ids": [int,...], "caption": str, "ts": int } }
ALBUM_CACHE: Dict[int, Dict[str, Any]] = {}

def _touch_album(user_id: int, media_group_id: str, source_chat_id: int, forward_msg_id: int, caption: Optional[str]):
    rec = ALBUM_CACHE.get(user_id)
    if rec and str(rec.get("media_group_id")) == str(media_group_id):
        # тот же альбом — дополняем
        if forward_msg_id not in rec["message_ids"]:
            rec["message_ids"].append(forward_msg_id)
        if (not rec.get("caption")) and caption:
            rec["caption"] = caption
        rec["ts"] = int(time.time())
    else:
        # новый альбом
        ALBUM_CACHE[user_id] = {
            "media_group_id": str(media_group_id),
            "source_chat_id": int(source_chat_id),
            "message_ids": [int(forward_msg_id)],
            "caption": caption or "",
            "ts": int(time.time()),
        }

def _get_latest_album(user_id: int) -> Optional[Dict[str, Any]]:
    return ALBUM_CACHE.get(user_id)

def _clear_album(user_id: int):
    ALBUM_CACHE.pop(user_id, None)

# ---------------- Хэндлер: ловим пересланные сообщения с фото/медиагруппой ----------------
@dp.message(F.forward_from_chat & F.media_group_id)
async def on_forwarded_album_piece(m: Message):
    """
    Админ пересылает элементы альбома из канала боту.
    Мы собираем forward_from_message_id по media_group_id.
    """
    if not is_admin(m.from_user.id):
        return
    try:
        source_chat_id = m.forward_from_chat.id  # исходный канал
        fwd_mid = m.forward_from_message_id      # message_id в исходном канале
        mgid = m.media_group_id                  # общий media_group_id (сохраняется при forward)
        caption = m.caption or m.text or ""
        if source_chat_id and fwd_mid and mgid:
            _touch_album(m.from_user.id, str(mgid), int(source_chat_id), int(fwd_mid), caption)
    except Exception as e:
        log.exception("Ошибка сбора альбома: %s", e)

# На случай одиночной пересылки фото без media_group_id — пусть добавляют через /add_post (уже было)

# ---------------- Базовые команды ----------------
@dp.message(Command("start"))
async def cmd_start(m: Message):
    help_text = (
        "Готов к работе.\n\n"
        "<b>Команды</b>:\n"
        "/myid — показать твой Telegram ID\n"
        "/enqueue &lt;текст&gt; — положить текстовое объявление в очередь (старый режим)\n"
        "/queue — показать размер очередей\n"
        "/post_oldest — опубликовать самое старое (старый режим)\n"
        "/add_post &lt;ссылка-на-сообщение&gt; — добавить одиночный пост из канала в очередь копий\n"
        "/add_album — добавить ПОСЛЕДНИЙ пересланный альбом (forward из канала) в очередь копий\n"
        "/clear_album — очистить сохранённый альбом\n"
        "/now — текущее время (TZ)\n"
    )
    await m.answer(help_text)

@dp.message(Command("myid"))
async def cmd_myid(m: Message):
    await m.answer(f"Твой Telegram ID: <code>{m.from_user.id}</code>")

@dp.message(Command("now"))
async def cmd_now(m: Message):
    await m.answer(f"Серверное время: <b>{now_str()}</b> ({TZ_NAME})")

# --------- Старый текстовый режим (оставляем для совместимости) ---------
@dp.message(Command("enqueue"))
async def cmd_enqueue(m: Message, command: CommandObject):
    if not is_admin(m.from_user.id):
        return await m.answer("Нет прав.")
    text = (command.args or "").strip()
    if not text:
        return await m.answer("Использование: /enqueue <текст объявления>")
    text = unify_caption(text)
    ad_id = db_enqueue(text)
    await m.answer(f"Добавлено в очередь: <code>{ad_id}</code>")

@dp.message(Command("queue"))
async def cmd_queue(m: Message):
    from storage.db import queue_count_pending
    count_text = 1 if get_oldest() else 0
    count_copy = queue_count_pending()
    await m.answer(
        f"Очередь ТЕКСТ: <b>{count_text}</b>\n"
        f"Очередь КОПИЙ: <b>{count_copy}</b>"
    )

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
    await m.answer(
        f"Опубликовано объявление <code>{ad_id}</code> в канал.\n"
        f"Удалено из очереди объявлений: <b>{removed}</b> (включая похожие)."
    )

# ---------------- Новые команды: добавление одиночного поста и альбома ----------------
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
            # t.me/c/123456789/555
            idx = parts.index("c")
            raw = parts[idx+1]
            msg = int(parts[idx+2])
            chat_id = int("-100" + raw)
            return chat_id, [msg]
        else:
            # t.me/username/555
            username = parts[3]
            msg = int(parts[4])
            return username, [msg]
    except Exception:
        return None

@dp.message(Command("add_post"))
async def cmd_add_post(m: Message, command: CommandObject):
    if not is_admin(m.from_user.id):
        return await m.answer("Нет прав.")
    args = (command.args or "").strip()

    # Вариант 1: ответ на пересланное ОДИНОЧНОЕ сообщение
    if m.reply_to_message and m.reply_to_message.forward_from_chat:
        rm = m.reply_to_message
        if rm.media_group_id:
            return await m.answer("Это часть альбома. Для альбомов используй: /add_album")
        source_chat_id = rm.forward_from_chat.id
        fwd_mid = rm.forward_from_message_id
        if not fwd_mid:
            return await m.answer("Не вижу forward_from_message_id. Перешли сообщение из канала ещё раз.")
        qid = queue_add(source_chat_id=source_chat_id, message_ids=[fwd_mid], caption_override=unify_caption(rm.caption or rm.text or ""))
        return await m.answer(f"Добавлено в очередь копирования: <code>{qid}</code>")

    # Вариант 2: по ссылке
    parsed = _parse_link(args) if args else None
    if not parsed:
        return await m.answer(
            "Использование:\n"
            "• Ответом на пересланное сообщение из канала: <code>/add_post</code>\n"
            "• Или: <code>/add_post https://t.me/username/123</code>\n"
            "     или <code>/add_post https://t.me/c/123456789/123</code>"
        )
    source_chat, mids = parsed
    qid = queue_add(source_chat_id=source_chat if isinstance(source_chat, int) else source_chat, message_ids=mids, caption_override=None)
    await m.answer(f"Добавлено в очередь копирования: <code>{qid}</code>")

@dp.message(Command("add_album"))
async def cmd_add_album(m: Message):
    """
    Процедура:
      1) Перешли ВСЕ фото альбома из канала боту (как forward).
      2) Пришли /add_album — мы возьмём последний собранный альбом от тебя.
    """
    if not is_admin(m.from_user.id):
        return await m.answer("Нет прав.")

    rec = _get_latest_album(m.from_user.id)
    if not rec:
        return await m.answer("Не найден недавний альбом. Сначала перешли альбом из канала, затем повтори /add_album.")

    # сортируем ids на всякий случай
    ids = sorted(set(int(x) for x in rec["message_ids"]))
    qid = queue_add(
        source_chat_id=int(rec["source_chat_id"]),
        message_ids=ids,
        caption_override=unify_caption(rec.get("caption") or "")
    )
    _clear_album(m.from_user.id)
    await m.answer(f"✅ Альбом добавлен в очередь копирования: <code>{qid}</code>\n"
                   f"Элементов: <b>{len(ids)}</b>")

@dp.message(Command("clear_album"))
async def cmd_clear_album(m: Message):
    if not is_admin(m.from_user.id):
        return await m.answer("Нет прав.")
    _clear_album(m.from_user.id)
    await m.answer("Кэш последнего альбома очищен.")

# ---------------- Точка входа ----------------
async def main():
    init_db()
    log.info("✅ Бот запущен для @%s (TZ=%s)", str(CHANNEL_ID).lstrip("@"), TZ_NAME)
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
