# main.py
import os
import asyncio
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ChatType
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

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()
ADMINS_RAW = os.getenv("ADMINS", "").strip()
ADMINS = [a.strip() for a in ADMINS_RAW.split(",") if a.strip()]
TZ_NAME = os.getenv("TZ", "Europe/Moscow")
tz = ZoneInfo(TZ_NAME)

def now_str() -> str:
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

def is_admin(uid: int) -> bool:
    return (str(uid) in ADMINS) or (ADMINS_RAW.startswith("@") and False)

def unify_caption(text: str | None) -> str:
    # тот же форматтер, что в scheduler
    text = (text or "").strip()
    text = text.replace("Цена -", "Цена —").replace("Цена — ", "Цена — ")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    text = "\n".join(lines)
    if "layoutplacebuy" not in text:
        text += "\n\n@layoutplacebuy"
    return text

props = DefaultBotProperties(parse_mode=ParseMode.HTML, disable_web_page_preview=True)
bot = Bot(BOT_TOKEN, default=props)
dp = Dispatcher(storage=MemoryStorage())

# ---------------- /start ----------------
@dp.message(Command("start"))
async def cmd_start(m: Message):
    help_text = (
        "Готов к работе.\n\n"
        "<b>Команды</b>:\n"
        "/myid — показать твой Telegram ID\n"
        "/enqueue &lt;текст&gt; — положить объявление в очередь (старый режим)\n"
        "/queue — показать размер очередей\n"
        "/post_oldest — опубликовать самое старое и удалить похожие (старый режим)\n"
        "/add_post &lt;ссылка-на-сообщение&gt; — добавить старый пост из канала в очередь перепостов\n"
        "/add_post (реплаем на пересланное сообщение) — тоже добавит, с поддержкой альбомов\n"
        "/now — текущее время (TZ)\n"
    )
    await m.answer(help_text)

@dp.message(Command("myid"))
async def cmd_myid(m: Message):
    await m.answer(f"Твой Telegram ID: <code>{m.from_user.id}</code>")

@dp.message(Command("now"))
async def cmd_now(m: Message):
    await m.answer(f"Серверное время: <b>{now_str()}</b>")

# ---------------- очереди: старый режим (текст) ----------------
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
    # показываем размеры двух очередей
    from storage.db import queue_count_pending
    count_text = 0
    oldest = get_oldest()
    if oldest:
        # просто индикатор, что есть
        count_text = 1
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
    # тут раньше был вызов send_to_channel(text) — оставим как заглушку:
    await bot.send_message(CHANNEL_ID, text)
    similar = find_similar_ids(ad_id, threshold=0.88)
    removed = bulk_delete([ad_id] + similar)
    await m.answer(
        f"Опубликовано объявление <code>{ad_id}</code> в канал.\n"
        f"Удалено из очереди объявлений: <b>{removed}</b> (включая похожие)."
    )

# ---------------- Новая команда: /add_post ----------------
def _parse_link(arg: str):
    """
    Поддерживает:
      - https://t.me/username/123
      - https://t.me/c/CHAT_ID/123  (чат id без -100, мы восстановим)
    Возвращает (source_chat_id:int, [message_ids:list[int]])
    """
    arg = (arg or "").strip()
    if not arg.startswith("http"):
        return None

    try:
        # простенький парсер без внешних либ
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
            # для copy_message можно использовать @username
            return username, [msg]
    except Exception:
        return None

@dp.message(Command("add_post"))
async def cmd_add_post(m: Message, command: CommandObject):
    if not is_admin(m.from_user.id):
        return await m.answer("Нет прав.")
    args = (command.args or "").strip()

    # 1) если реплай на пересланный пост — вытащим пакет альбома
    if m.reply_to_message:
        rm = m.reply_to_message
        if rm.forward_from_chat:
            source_chat_id = rm.forward_from_chat.id
            media_group_id = rm.media_group_id
            ids = [rm.message_id]
            if media_group_id:
                # собрать все из альбома в этом чате (личка с ботом)
                # в рамках апдейта у нас только одно сообщение, поэтому добавим только текущее
                # (альбом сохранится как список message_ids, копировать будем по одному)
                pass
            qid = queue_add(source_chat_id=source_chat_id, message_ids=ids, caption_override=unify_caption(rm.caption or rm.text or ""))
            return await m.answer(f"Добавлено в очередь копирования: <code>{qid}</code>")

    # 2) ссылка
    parsed = _parse_link(args) if args else None
    if not parsed:
        return await m.answer(
            "Использование:\n"
            "• Ответом на пересланное сообщение из канала: <code>/add_post</code>\n"
            "• Или: <code>/add_post https://t.me/username/123</code> "
            "или <code>/add_post https://t.me/c/123456789/123</code>"
        )

    source_chat, mids = parsed
    qid = queue_add(source_chat_id=source_chat if isinstance(source_chat, int) else source_chat, message_ids=mids, caption_override=None)
    await m.answer(f"Добавлено в очередь копирования: <code>{qid}</code>")

# ---------------- Точка входа ----------------
async def main():
    init_db()
    log.info("✅ Бот запущен для @%s (TZ=%s)", str(CHANNEL_ID).lstrip("@"), TZ_NAME)
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
