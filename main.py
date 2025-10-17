import asyncio, logging, os, pytz, re
from datetime import datetime, time as dtime, timedelta
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.exceptions import TelegramBadRequest

from config import (
    TOKEN, CHANNEL_ID, ADMINS, TZ, SLOTS_CSV, PREV_MIN,
    ALBUM_URL, CONTACT_TEXT,
    AUTO_POST, CATCH_UP_MISSED, POST_WINDOW_SECONDS
)
from storage import db
from utils.text import normalize_caption

logger = logging.getLogger("layoutplace_bot")
logging.basicConfig(level=logging.INFO)

bot = Bot(TOKEN)
dp  = Dispatcher()
rt  = Router()
dp.include_router(rt)

# ------------ helpers ------------
def _tznow():
    return datetime.now(pytz.timezone(TZ))

def _parse_slots(csv: str) -> list[dtime]:
    slots = []
    for s in csv.split(","):
        s = s.strip()
        if not s: continue
        hh, mm = s.split(":")
        slots.append(dtime(int(hh), int(mm)))
    return slots

SLOTS = _parse_slots(SLOTS_CSV)
PREVIEW_DELTA = timedelta(minutes=PREV_MIN)

async def _dm_admins(text: str):
    for aid in ADMINS:
        try:
            await bot.send_message(aid, text, disable_web_page_preview=True)
        except Exception as e:
            logger.warning(f"DM админу {aid} не доставлено: {e}")

# ------------ commands ------------
@rt.message(Command("start"))
async def cmd_start(m: Message):
    db.init_db()
    txt = (
        "Бот готов к работе.\n\n"
        "Команды:\n"
        "/myid — показать твой Telegram ID\n"
        "/add_post — сделай ЭТОЙ командой ответом на пересланное из канала сообщение (фото/альбом)\n"
        "/queue — показать размер очереди\n"
        "/clear_queue — очистить очередь\n"
        "/post_oldest — опубликовать старый пост вручную\n"
        "/test_preview — отправить тестовое превью админам\n"
        "/now — текущее время\n\n"
        f"Слоты сегодня: {', '.join([t.strftime('%H:%M') for t in SLOTS])} "
        f"(превью за {PREV_MIN} мин; авто-постинг: {'вкл' if AUTO_POST else 'выкл'})"
    )
    await m.answer(txt, disable_web_page_preview=True)

@rt.message(Command("myid"))
async def cmd_myid(m: Message):
    await m.answer(str(m.from_user.id))

def _src_tuple(m: Message) -> tuple[int,int] | None:
    if not m.reply_to_message:
        return None
    rm = m.reply_to_message
    if rm.forward_from_chat:
        return (rm.forward_from_chat.id, rm.forward_from_message_id)
    if rm.sender_chat and str(getattr(rm.sender_chat, "type", "")) == "chat_type.CHANNEL":
        return (rm.chat.id, rm.message_id)
    return None

@rt.message(Command("add_post"))
async def cmd_add_post(m: Message):
    if m.from_user.id not in ADMINS:
        return
    src = _src_tuple(m)
    if not src:
        await m.answer("Сделай /add_post **ответом** на пересланное из канала сообщение (фото/альбом).")
        return
    cap = None
    rm = m.reply_to_message
    if rm and (rm.caption or rm.text):
        cap = rm.caption or rm.text
    cap = normalize_caption(cap or "", ALBUM_URL, CONTACT_TEXT)

    qid = db.enqueue(src_chat_id=src[0], src_msg_id=src[1], caption=cap)
    await m.answer(f"Медиа добавлено в очередь (id={qid}). В очереди: {db.count()}.")

@rt.message(Command("queue"))
async def cmd_queue(m: Message):
    await m.answer(f"В очереди: {db.count()}.")

@rt.message(Command("clear_queue"))
async def cmd_clear(m: Message):
    if m.from_user.id not in ADMINS:
        return
    db.clear()
    await m.answer("Очищено.")

@rt.message(Command("test_preview"))
async def cmd_test_preview(m: Message):
    await send_preview()

@rt.message(Command("now"))
async def cmd_now(m: Message):
    await m.answer(_tznow().strftime("%Y-%m-%d %H:%M:%S %Z"))

@rt.message(Command("post_oldest"))
async def cmd_post_oldest(m: Message):
    if m.from_user.id not in ADMINS:
        return
    ok, msg = await publish_oldest()
    await m.answer(msg)

# ------------ core actions ------------
async def publish_oldest() -> tuple[bool,str]:
    row = db.dequeue_oldest()
    if not row:
        return False, "Очередь пустая."
    _id, src_chat_id, src_msg_id, caption = row

    # удалить предыдущую публикацию бота (если была)
    last = db.get_last_published_id()
    if last:
        try:
            await bot.delete_message(CHANNEL_ID, last)
        except TelegramBadRequest as e:
            logger.warning(f"Не смог удалить старый пост {CHANNEL_ID}/{last}: {e}")

    # опубликовать
    try:
        sent = await bot.copy_message(
            chat_id=CHANNEL_ID,
            from_chat_id=src_chat_id,
            message_id=src_msg_id,
            caption=caption or None
        )
        db.set_last_published_id(sent.message_id)
        return True, f"Опубликовано. Осталось в очереди: {db.count()}."
    except TelegramBadRequest as e:
        return False, f"Не удалось опубликовать: {e.message}"

async def send_preview():
    peek = db.peek_oldest()
    if not peek:
        await _dm_admins("Превью: очередь пуста.")
        return
    _, _, _, caption = peek
    text = "Превью (за 45 минут):\n\n" + (caption or "— без подписи —")
    await _dm_admins(text)

# ------------ scheduler: превью + авто-постинг ------------
async def _scheduler_loop():
    tz = pytz.timezone(TZ)
    await asyncio.sleep(2)
    slog = logging.getLogger("layoutplace_scheduler")
    slog.info(
        f"Scheduler TZ={TZ}, times={','.join([t.strftime('%H:%M') for t in SLOTS])}, "
        f"preview_before={PREV_MIN} min, auto_post={AUTO_POST}"
    )

    preview_sent: set[str] = set()
    posted: set[str] = set()

    while True:
        now = _tznow()

        for t in SLOTS:
            slot_dt = tz.localize(datetime.combine(now.date(), t))
            preview_at = slot_dt - PREVIEW_DELTA

            # 1) превью за PREV_MIN
            key_prev = f"prev:{preview_at:%Y-%m-%d %H:%M}"
            if preview_at <= now < preview_at + timedelta(seconds=POST_WINDOW_SECONDS):
                if key_prev not in preview_sent:
                    await send_preview()
                    preview_sent.add(key_prev)

            # 2) авто-постинг в сам слот (без «догонялок», если выключены)
            key_post = f"post:{slot_dt:%Y-%m-%d %H:%M}"
            should_post_window = slot_dt <= now < slot_dt + timedelta(seconds=POST_WINDOW_SECONDS)
            missed = now > slot_dt + timedelta(seconds=POST_WINDOW_SECONDS)

            if AUTO_POST:
                if should_post_window and key_post not in posted:
                    ok, msg = await publish_oldest()
                    if not ok:
                        # пустая очередь — молчим, чтобы не спамить
                        pass
                    posted.add(key_post)
                elif missed and CATCH_UP_MISSED and key_post not in posted:
                    # «догонялка» (по умолчанию выключена)
                    ok, msg = await publish_oldest()
                    posted.add(key_post)

        # полночь — сбрасываем «маркеры»
        if now.hour == 0 and now.minute < 2:
            preview_sent.clear()
            posted.clear()

        await asyncio.sleep(2)

# ------------ entry ------------
async def _run():
    db.init_db()
    asyncio.create_task(_scheduler_loop())
    await dp.start_polling(bot)
