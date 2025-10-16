# main.py
import asyncio
import logging
from typing import Dict, List, Optional

from aiogram import Bot, Dispatcher, F, types
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, InputMediaPhoto, InputMediaVideo, InputMediaDocument

from config import TOKEN as BOT_TOKEN, CHANNEL_ID as _CHANNEL_ID, TZ as _TZ, ALBUM_URL, CONTACT_TEXT
from storage.db import (
    init_db,
    enqueue_text,
    enqueue_media,
    get_oldest,
    get_count,
    delete_by_id,
)

# ---------- базовая настройка ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("layoutplace_bot")

# канал может быть @username или -100...
CHANNEL_ID = _CHANNEL_ID
TZ = _TZ

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())

# ===== единый стиль =====
def normalize_caption(raw: str) -> str:
    raw = (raw or "").strip()
    # добавляем блок ссылок внизу и убираем эмодзи
    lines = [l for l in raw.splitlines() if l.strip()]
    text = "\n".join(lines)
    text += (
        "\n\n"
        "Общий альбом: " + ALBUM_URL + "\n"
        "Покупка/вопросы: " + CONTACT_TEXT
    )
    return text

# ===== кэш альбомов (по media_group_id) =====
# Когда ты пересылаешь альбом боту, сообщения летят пачкой.
# Мы собираем элементы 2 минуты; /add_post (ответом) заберёт актуальную пачку.
albums_cache: Dict[str, Dict[str, any]] = {}
ALBUM_TTL_SEC = 120

def _vacuum_albums_cache():
    import time
    now = time.time()
    for k in list(albums_cache.keys()):
        if now - albums_cache[k]["ts"] > ALBUM_TTL_SEC:
            del albums_cache[k]

def _add_album_piece(m: Message):
    import time
    _vacuum_albums_cache()
    gid = m.media_group_id
    if not gid:
        return
    if gid not in albums_cache:
        albums_cache[gid] = {"ts": time.time(), "items": [], "src_ids": [], "src_chat": None, "caption": ""}
    bucket = albums_cache[gid]
    bucket["ts"] = time.time()

    # источник (если форвард из канала)
    if m.forward_from_chat:
        bucket["src_chat"] = m.forward_from_chat.id
        if m.forward_from_message_id:
            bucket["src_ids"].append(m.forward_from_message_id)

    # подпись ловим один раз
    if m.caption and not bucket["caption"]:
        bucket["caption"] = m.caption

    if m.photo:
        fid = m.photo[-1].file_id
        bucket["items"].append({"type": "photo", "file_id": fid})
    elif m.video:
        bucket["items"].append({"type": "video", "file_id": m.video.file_id})
    elif m.document:
        bucket["items"].append({"type": "document", "file_id": m.document.file_id})

# ---------- Команды ----------

@dp.message(Command("start"))
async def cmd_start(m: Message):
    help_text = (
        "Бот готов к работе.\n\n"
        "<b>Команды:</b>\n"
        "/enqueue &lt;текст&gt; — добавить текст в очередь\n"
        "/add_post — добавить форвард (фото/альбом) в очередь (вызови ответом на пересланное сообщение)\n"
        "/queue — показать размер очереди\n"
        "/post_oldest — опубликовать самый старый пост вручную\n"
        "/clear_queue — очистить очередь\n"
        "/test_preview — проверить превью админу\n"
    )
    await m.answer(help_text, disable_web_page_preview=True)

@dp.message(Command("enqueue"))
async def cmd_enqueue(m: Message, command: CommandObject):
    text = (command.args or "").strip()
    if not text:
        await m.answer("Использование: /enqueue <текст>")
        return
    norm = normalize_caption(text)
    item_id = enqueue_text(norm)
    await m.answer(f"Текст добавлен в очередь (id={item_id}). В очереди: {get_count()}.")

@dp.message(F.media_group_id | F.photo | F.video | F.document)
async def on_any_media(m: Message):
    # ловим куски альбомов в кэш
    if m.media_group_id:
        _add_album_piece(m)
    # одиночное медиа тоже можно добавить командой /add_post ответом

@dp.message(Command("add_post"))
async def cmd_add_post(m: Message):
    """
    Добавляет в очередь альбом/фото. Использовать как reply на пересланное сообщение из канала.
    """
    if not m.reply_to_message:
        await m.answer("Сделай /add_post ответом на пересланное из канала сообщение (фото/альбом).")
        return

    src = m.reply_to_message

    media: List[Dict[str, str]] = []
    src_chat_id: Optional[int] = None
    src_ids: List[int] = []
    caption: str = src.caption or ""

    # если это часть альбома — собираем из кэша всю группу
    if src.media_group_id and src.media_group_id in albums_cache:
        bucket = albums_cache[src.media_group_id]
        media = bucket["items"]
        src_chat_id = bucket["src_chat"]
        src_ids = bucket["src_ids"]
        if bucket["caption"]:
            caption = bucket["caption"]
    else:
        # одиночное
        if src.forward_from_chat:
            src_chat_id = src.forward_from_chat.id
            if src.forward_from_message_id:
                src_ids = [src.forward_from_message_id]
        if src.photo:
            media = [{"type": "photo", "file_id": src.photo[-1].file_id}]
        elif src.video:
            media = [{"type": "video", "file_id": src.video.file_id}]
        elif src.document:
            media = [{"type": "document", "file_id": src.document.file_id}]
        else:
            await m.answer("Не вижу медиа. Перешли фото/альбом и вызови /add_post ответом.")
            return

    if not media:
        await m.answer("Альбом пуст. Перешли заново и повтори /add_post.")
        return

    norm_caption = normalize_caption(caption)
    item_id = enqueue_media(norm_caption, media, src_chat_id=src_chat_id, src_msg_ids=src_ids)

    # подчистим кэш по группе, если был
    if src.media_group_id and src.media_group_id in albums_cache:
        del albums_cache[src.media_group_id]

    await m.answer(f"Медиа добавлено в очередь (id={item_id}). В очереди: {get_count()}.")

@dp.message(Command("queue"))
async def cmd_queue(m: Message):
    await m.answer(f"В очереди: {get_count()}.")

@dp.message(Command("clear_queue"))
async def cmd_clear(m: Message):
    from storage.db import clear_all
    n = clear_all()
    await m.answer(f"Очередь очищена. Удалено записей: {n}.")

async def _send_to_channel(item: dict) -> Optional[List[int]]:
    """
    Публикация в канал. Возвращает список новых message_id (для логов) или None.
    """
    text: str = item["text"]
    media: List[Dict[str, str]] = item["media"]

    if not media:
        msg = await bot.send_message(CHANNEL_ID, text, disable_web_page_preview=True)
        return [msg.message_id]

    # альбом
    input_media: List[types.InputMedia] = []
    for i, it in enumerate(media):
        t = it["type"]
        fid = it["file_id"]
        if t == "photo":
            input_media.append(InputMediaPhoto(media=fid, caption=text if i == 0 else None, parse_mode=ParseMode.HTML))
        elif t == "video":
            input_media.append(InputMediaVideo(media=fid, caption=text if i == 0 else None, parse_mode=ParseMode.HTML))
        elif t == "document":
            input_media.append(InputMediaDocument(media=fid, caption=text if i == 0 else None, parse_mode=ParseMode.HTML))
    msgs = await bot.send_media_group(CHANNEL_ID, input_media)
    return [m.message_id for m in msgs]

@dp.message(Command("post_oldest"))
async def cmd_post_oldest(m: Message):
    rows = get_oldest(1)
    if not rows:
        await m.answer("Очередь пуста.")
        return
    item = rows[0]
    new_ids = await _send_to_channel(item)

    # попытка удалить старый исходный пост из канала (если мы его знали)
    if item.get("src_chat_id") and item.get("src_msg_ids"):
        for mid in item["src_msg_ids"]:
            try:
                await bot.delete_message(item["src_chat_id"], mid)
            except Exception as e:
                log.warning(f"Не смог удалить старое сообщение {mid}: {e}")

    delete_by_id(item["id"])
    await m.answer(f"Опубликовано. Осталось в очереди: {get_count()}.")

@dp.message(Command("test_preview"))
async def cmd_test_preview(m: Message):
    from config import ADMINS
    text = "<b>Тестовое превью</b>\nПост был бы тут за 45 минут до публикации."
    for aid in ADMINS:
        try:
            await bot.send_message(aid, text, disable_web_page_preview=True)
        except Exception as e:
            log.warning(f"Не удалось отправить превью админу {aid}: {e}")
    await m.answer("Ок, превью отправлено админам (если они нажали /start боту).")

# ---------- Точка входа ----------
async def main():
    init_db()
    log.info("✅ Бот запущен для %s (TZ=%s)", str(CHANNEL_ID).strip("@"), TZ)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
