# main.py
import asyncio, logging, time, json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import List, Dict, Optional, Tuple

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import Message, InputMediaPhoto
from aiogram.filters import Command

from config import (
    TOKEN, CHANNEL_ID, TZ, ADMINS,
    POST_TIMES, PREVIEW_BEFORE_MIN
)
from storage.db import (
    init_db, enqueue, dequeue_oldest, get_count, peek_oldest,
    meta_get, meta_set
)
from utils.text import normalize_caption

logging.basicConfig(level=logging.INFO,
                    format="%(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("layoutplace_bot")

bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# ------------- helpers -------------

def tznow() -> datetime:
    return datetime.now(ZoneInfo(TZ))

def _today_slots() -> List[datetime]:
    base = tznow().date()
    out = []
    for t in POST_TIMES:
        hh, mm = [int(x) for x in t.strip().split(":")]
        out.append(datetime(base.year, base.month, base.day, hh, mm, tzinfo=ZoneInfo(TZ)))
    return out

async def _notify_admins(text: str):
    for uid in ADMINS:
        try:
            await bot.send_message(uid, text, disable_web_page_preview=True)
        except Exception as e:
            log.warning(f"Админ {uid} недоступен: {e}")

async def _send_to_channel(items: List[Dict], caption: str) -> int:
    # альбом
    if len(items) > 1:
        media = []
        for i, it in enumerate(items):
            cap = caption if i == 0 else ""
            media.append(InputMediaPhoto(media=it["file_id"], caption=cap))
        msgs = await bot.send_media_group(CHANNEL_ID, media)
        return msgs[0].message_id
    else:
        it = items[0]
        msg = await bot.send_photo(CHANNEL_ID, it["file_id"], caption=caption)
        return msg.message_id

def _collect_items_from_message(msg: Message) -> List[Dict]:
    items: List[Dict] = []
    # если это пересланное из канала фото/док
    m = msg.reply_to_message or msg
    if m.photo:
        # берём максимальный размер
        items.append({"type": "photo", "file_id": m.photo[-1].file_id})
    elif m.media_group_id and m.photo:
        items.append({"type": "photo", "file_id": m.photo[-1].file_id})
    elif m.document and m.document.mime_type.startswith("image/"):
        items.append({"type": "photo", "file_id": m.document.file_id})
    return items

def _src_tuple(msg: Message) -> Optional[Tuple[int,int]]:
    m = msg.reply_to_message
    if not m: 
        return None
    # если пост реально из канала
    if m.forward_from_chat and (m.forward_from_chat.type.value == "channel"):
        return (m.forward_from_chat.id, m.forward_from_message_id)
    # если это не forward, попробуем по чату/ид
    if m.chat and m.chat.type.value == "channel":
        return (m.chat.id, m.message_id)
    return None

# ------------- handlers -------------

@dp.message(Command("start"))
async def cmd_start(m: Message):
    help_text = (
        "Бот готов к работе.\n\n"
        "Команды:\n"
        "/myid — показать твой Telegram ID\n"
        "/add_post — сделай ответом на пересланное из канала сообщение (фото/альбом)\n"
        "/queue — показать размер очереди\n"
        "/post_oldest — опубликовать старый пост вручную\n"
        "/clear_queue — очистить очередь\n"
        "/test_preview — тестовое превью админам\n"
        "/now — текущее время"
    )
    await m.answer(help_text, disable_web_page_preview=True)

@dp.message(Command("myid"))
async def cmd_myid(m: Message):
    await m.answer(f"Твой ID: <code>{m.from_user.id}</code>")

@dp.message(Command("queue"))
async def cmd_queue(m: Message):
    await m.answer(f"В очереди: {get_count()}.")

@dp.message(Command("clear_queue"))
async def cmd_clear(m: Message):
    # примитивно — достаём всё
    removed = 0
    while dequeue_oldest():
        removed += 1
    await m.answer(f"Очищено: {removed}.")

@dp.message(Command("add_post"))
async def cmd_add_post(m: Message):
    if not (m.reply_to_message or m.photo or m.document):
        await m.answer("Сделай /add_post ответом на пересланное из канала сообщение (фото/альбом).")
        return

    items = _collect_items_from_message(m)
    if not items:
        await m.answer("Не нашёл фото. Пришли пересланный пост с фото/альбомом.")
        return

    # caption берём из пересланного сообщения, если есть
    src_msg = m.reply_to_message or m
    raw_caption = (src_msg.caption or "").strip()
    caption = normalize_caption(raw_caption)

    qid = enqueue(items=items, caption=caption, src=_src_tuple(m))
    await m.answer(f"Медиа добавлено в очередь (id={qid}). В очереди: {get_count()}.")

@dp.message(Command("post_oldest"))
async def cmd_post_oldest(m: Message):
    task = dequeue_oldest()
    if not task:
        await m.answer("Очередь пуста.")
        return
    # публикация
    new_msg_id = await _send_to_channel(task["items"], task["caption"])
    # пробуем удалить источник
    if task["src"]:
        src_chat, src_msg = task["src"]
        try:
            await bot.delete_message(src_chat, src_msg)
        except Exception as e:
            log.warning(f"Не смог удалить старое сообщение {src_chat}/{src_msg}: {e}")
    await m.answer(f"Опубликовано. Осталось в очереди: {get_count()}.")

@dp.message(Command("test_preview"))
async def cmd_test_preview(m: Message):
    await _notify_admins("Тестовое превью\nПост был бы тут за 45 минут до публикации.")
    await m.answer("Ок, превью отправлено админам (если они нажали /start боту).")

@dp.message(Command("now"))
async def cmd_now(m: Message):
    await m.answer(str(tznow()))

# ------------- scheduler -------------

async def _catch_up_if_needed():
    """При старте публикуем все пропущенные слоты сегодня (если очередь не пуста)."""
    now = tznow()
    last_key = "last_slot_ts"
    last_ts = int(meta_get(last_key) or "0")
    last_dt = datetime.fromtimestamp(last_ts, tz=ZoneInfo(TZ)) if last_ts else None

    for slot in _today_slots():
        if slot <= now and (not last_dt or slot > last_dt):
            task = dequeue_oldest()
            if not task:
                break
            await _send_to_channel(task["items"], task["caption"])
            if task["src"]:
                try:
                    await bot.delete_message(task["src"][0], task["src"][1])
                except Exception as e:
                    log.warning(f"Catch-up delete failed: {e}")
            meta_set(last_key, str(int(slot.timestamp())))
            log.info(f"Catch-up: опубликован слот {slot.isoformat()}")

async def run_scheduler():
    log.info(f"Scheduler TZ={TZ}, times={','.join(POST_TIMES)}, preview_before={PREVIEW_BEFORE_MIN} min")
    await _catch_up_if_needed()

    last_key = "last_slot_ts"
    preview_key = "preview_for_ts"   # чтобы превью ушло один раз

    while True:
        now = tznow()

        # превью
        for slot in _today_slots():
            preview_at = slot - timedelta(minutes=PREVIEW_BEFORE_MIN)
            if preview_at <= now < slot:
                # отправим превью один раз на этот слот
                if meta_get(preview_key) != str(int(slot.timestamp())):
                    peek = peek_oldest()
                    if peek:
                        await _notify_admins(
                            "Превью: следующий пост через "
                            f"{(slot - now).seconds // 60} мин.\n\n"
                            f"{peek['caption'][:500]}"
                        )
                    meta_set(preview_key, str(int(slot.timestamp())))

        # публикация
        for slot in _today_slots():
            last_ts = int(meta_get(last_key) or "0")
            last_dt = datetime.fromtimestamp(last_ts, tz=ZoneInfo(TZ)) if last_ts else None

            if slot <= now and (not last_dt or slot > last_dt):
                task = dequeue_oldest()
                if task:
                    await _send_to_channel(task["items"], task["caption"])
                    if task["src"]:
                        try:
                            await bot.delete_message(task["src"][0], task["src"][1])
                        except Exception as e:
                            log.warning(f"Delete source failed: {e}")
                    meta_set(last_key, str(int(slot.timestamp())))
                    log.info(f"Posted for slot {slot.isoformat()}")
                else:
                    # запомним, чтобы не крутиться бесконечно на пустой очереди
                    meta_set(last_key, str(int(slot.timestamp())))
        await asyncio.sleep(20)

# ------------- entry -------------

async def _run():
    init_db()
    # запускаем планировщик параллельно с polling
    asyncio.create_task(run_scheduler())
    await dp.start_polling(bot)

if __name__ == "__main__":
    log.info("Starting bot instance...")
    asyncio.run(_run())
