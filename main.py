import os
import re
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ChatType
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message,
    FSInputFile,
    InputMediaPhoto,
    InputMediaVideo,
    InputMediaDocument,
)

from storage.db import (
    init_db,
    enqueue,
    dequeue_oldest,
    peek_all,
    stats,
    delete_by_id,
    last_id,
    clear_queue,
    get_last_channel_msg_id,
    set_last_channel_msg_id,
)

# ---------- logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s:%(name)s:%(message)s",
)
log = logging.getLogger("layoutplace_bot")
sched_log = logging.getLogger("layoutplace_scheduler")

# ---------- env ----------
def _must(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise RuntimeError(f"ENV {name} пуст. Задай переменную окружения.")
    return v

TOKEN = _must("TOKEN")
CHANNEL_ID = int(_must("CHANNEL_ID"))
TZ_NAME = os.getenv("TZ", "Europe/Moscow")
ADMINS = [int(x) for x in os.getenv("ADMINS", "").replace(";", ",").split(",") if x.strip().isdigit()]
POST_TIMES = [t.strip() for t in os.getenv("POST_TIMES", "12:00,16:00,20:00").split(",") if t.strip()]
PREVIEW_BEFORE_MIN = int(os.getenv("PREVIEW_BEFORE_MIN", os.getenv("PREVIEW_MINUTES", "45")))
ALBUM_URL = os.getenv("ALBUM_URL", "").strip()
CONTACT = os.getenv("CONTACT", os.getenv("CONTACT_TEXT", "@layoutplacebuy")).strip()

tz = pytz.timezone(TZ_NAME)

# ---------- bot/dispatcher ----------
bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# ---------- helpers ----------

def _is_admin(user_id: Optional[int]) -> bool:
    return bool(user_id and user_id in ADMINS)

def normalize_text(raw: str) -> str:
    """Привести текст к единому виду и дописать постоянные блоки."""
    text = raw.strip()

    # нормализуем разделители/дефисы
    text = re.sub(r" ?— ?", " — ", text)
    text = re.sub(r"\r\n", "\n", text)
    # лишние пустые строки
    text = re.sub(r"\n{3,}", "\n\n", text)

    # добавим постоянные хвосты, если их нет
    tail_album = f"\nОбщее наличие: {ALBUM_URL}" if ALBUM_URL and "Общее наличие" not in text and ALBUM_URL not in text else ""
    tail_contact = f"\nПокупка/вопросы: {CONTACT}" if CONTACT and CONTACT not in text else ""

    if tail_album or tail_contact:
        if not text.endswith("\n"):
            text += "\n"
        text += (tail_album + tail_contact).lstrip("\n")

    return text

def build_media_items_from_message(m: Message) -> Tuple[List[Dict[str, str]], str]:
    """
    Сформировать список media-элементов + подпись из одиночного сообщения.
    Возвращает (items, caption)
    """
    items: List[Dict[str, str]] = []
    caption = (m.caption or m.text or "").strip()

    if m.photo:
        fid = m.photo[-1].file_id
        items.append({"type": "photo", "file_id": fid})
    elif m.video:
        items.append({"type": "video", "file_id": m.video.file_id})
    elif m.document and (m.document.mime_type or "").startswith("image/"):
        items.append({"type": "photo", "file_id": m.document.file_id})
    elif m.document:
        items.append({"type": "document", "file_id": m.document.file_id})
    else:
        # текстовое — флаг: пустой список, но caption есть
        pass

    return items, caption

def _src_tuple(m: Message) -> Tuple[Optional[int], Optional[int]]:
    src_chat_id = None
    src_msg_id = None
    if (m.forward_from_chat and m.forward_from_message_id):
        src_chat_id = m.forward_from_chat.id
        src_msg_id = m.forward_from_message_id
    elif m.is_topic_message and m.message_thread_id:
        src_chat_id = m.chat.id
        src_msg_id = m.message_thread_id
    return src_chat_id, src_msg_id

# ---------- album aggregator (по media_group_id) ----------
_AGGR: Dict[str, Dict[str, Any]] = {}  # group_id -> {"messages":[Message,...], "task": asyncio.Task}

ALBUM_WINDOW = 0.9  # сек. ждать догрузку сообщений в альбоме

async def _flush_album(group_id: str):
    pack = _AGGR.pop(group_id, None)
    if not pack:
        return
    messages: List[Message] = pack["messages"]

    # соберём items и подпись из первого сообщения с подписью
    items: List[Dict[str, str]] = []
    caption: str = ""
    for msg in messages:
        its, cap = build_media_items_from_message(msg)
        items.extend(its)
        if not caption:
            caption = cap

    caption = normalize_text(caption)
    qid = enqueue(items, caption, _src_tuple(messages[0]))
    await messages[0].answer(f"✅ Альбом добавлен в очередь как один пост. #{qid}")

def _collect_album(m: Message) -> bool:
    """Собираем сообщения с одинаковым media_group_id в единый альбом. Возвращает True, если перехватили обработку."""
    gid = m.media_group_id
    if not gid:
        return False
    pack = _AGGR.get(gid)
    if not pack:
        # создать и запустить отложенный флэш
        task = asyncio.create_task(_delayed_flush(gid))
        _AGGR[gid] = {"messages": [m], "task": task}
    else:
        pack["messages"].append(m)
    return True

async def _delayed_flush(gid: str):
    await asyncio.sleep(ALBUM_WINDOW)
    await _flush_album(gid)

# ---------- posting ----------

async def _delete_prev_channel_post():
    prev = get_last_channel_msg_id()
    if not prev:
        return
    try:
        await bot.delete_message(CHANNEL_ID, prev)
    except Exception as e:
        sched_log.warning("Не смог удалить старое сообщение %s/%s: %s", CHANNEL_ID, prev, e)

async def post_queue_item(row: Dict[str, Any]) -> bool:
    """Отправить один элемент очереди в канал. Вернёт True/False."""
    payload: List[Dict[str, str]] = __import__("json").loads(row["payload"])
    caption: str = row.get("caption") or ""

    # перед постингом удалим предыдущий пост бота
    await _delete_prev_channel_post()

    msg_id: Optional[int] = None
    try:
        if not payload:
            # текстовый пост
            sent = await bot.send_message(CHANNEL_ID, normalize_text(caption))
            msg_id = sent.message_id
        elif len(payload) == 1:
            it = payload[0]
            cap = normalize_text(caption)
            if it["type"] == "photo":
                sent = await bot.send_photo(CHANNEL_ID, it["file_id"], caption=cap)
            elif it["type"] == "video":
                sent = await bot.send_video(CHANNEL_ID, it["file_id"], caption=cap)
            else:
                sent = await bot.send_document(CHANNEL_ID, it["file_id"], caption=cap)
            msg_id = sent.message_id
        else:
            media = []
            for idx, it in enumerate(payload):
                cap = normalize_text(caption) if idx == 0 else None
                if it["type"] == "photo":
                    media.append(InputMediaPhoto(media=it["file_id"], caption=cap))
                elif it["type"] == "video":
                    media.append(InputMediaVideo(media=it["file_id"], caption=cap))
                else:
                    media.append(InputMediaDocument(media=it["file_id"], caption=cap))
            res = await bot.send_media_group(CHANNEL_ID, media)
            msg_id = res[0].message_id if res else None

        if msg_id:
            set_last_channel_msg_id(msg_id)
        return True
    except Exception as e:
        log.exception("Ошибка отправки в канал: %s", e)
        return False

# ---------- scheduler (только предпросмотр) ----------

def _now_tz() -> datetime:
    return datetime.now(tz)

def _today_targets() -> List[datetime]:
    targets: List[datetime] = []
    dt = _now_tz()
    for t in POST_TIMES:
        try:
            h, m = map(int, t.split(":"))
            targets.append(dt.replace(hour=h, minute=m, second=0, microsecond=0))
        except Exception:
            continue
    return targets

_preview_marks: set[str] = set()

async def scheduler_task():
    sched_log.info("Scheduler запущен.")
    while True:
        try:
            now = _now_tz()
            for target in _today_targets():
                preview_at = target - timedelta(minutes=PREVIEW_BEFORE_MIN)
                key = f"{target.date()}_{target.hour:02d}{target.minute:02d}"
                if preview_at <= now < target and key not in _preview_marks:
                    _preview_marks.add(key)
                    # отправим превью в ЛС первому админу
                    if ADMINS:
                        q = peek_all()
                        text = (
                            "⏳ Предпросмотр публикации через "
                            f"{PREVIEW_BEFORE_MIN} мин ({target.strftime('%H:%M')})\n\n"
                            f"В очереди сейчас: {len(q)}."
                        )
                        try:
                            await bot.send_message(ADMINS[0], text)
                        except Exception:
                            pass
            await asyncio.sleep(30)
        except Exception as e:
            sched_log.exception("Ошибка в планировщике: %s", e)
            await asyncio.sleep(5)

# ---------- commands ----------

@dp.message(CommandStart())
async def cmd_start(m: Message):
    if not _is_admin(m.from_user.id):
        return
    st = stats()
    await m.answer(
        "Привет!\n"
        "Команды:\n"
        "/add_post — просто перешли пост/альбом сюда\n"
        "/queue — показать очередь\n"
        "/post_oldest — отправить самый старый пост в канал\n"
        "/del <id> — удалить из очереди\n"
        "/del_last — удалить последний\n"
        "/clear_queue — очистить очередь\n"
        f"\nСейчас в очереди: <b>{st['queued']}</b>."
    )

@dp.message(Command("queue"))
async def cmd_queue(m: Message):
    if not _is_admin(m.from_user.id):
        return
    rows = peek_all()
    if not rows:
        await m.answer("Очередь пуста.")
        return
    lines = [f"# {len(rows)} в очереди:"]
    for r in rows:
        ts = datetime.fromtimestamp(r["created_at"]).strftime("%d.%m %H:%M")
        lines.append(f"#{r['id']} · {ts}")
    await m.answer("\n".join(lines))

@dp.message(Command("post_oldest"))
async def cmd_post_oldest(m: Message):
    if not _is_admin(m.from_user.id):
        return
    row = dequeue_oldest()
    if not row:
        await m.answer("Очередь пуста.")
        return
    ok = await post_queue_item(row)
    await m.answer("✅ Отправлено в канал." if ok else "⚠️ Не удалось отправить (см. логи).")

@dp.message(Command("del"))
async def cmd_del(m: Message):
    if not _is_admin(m.from_user.id):
        return
    args = (m.text or "").split(maxsplit=1)
    if len(args) < 2:
        await m.answer("Укажи ID: <code>/del 12</code> (узнай через /queue)")
        return
    arg = args[1].lstrip("#")
    if not arg.isdigit():
        await m.answer("ID должен быть числом.")
        return
    qid = int(arg)
    deleted = delete_by_id(qid)
    await m.answer(f"✅ Удалено: {deleted}")

@dp.message(Command("del_last"))
async def cmd_del_last(m: Message):
    if not _is_admin(m.from_user.id):
        return
    lid = last_id()
    if lid is None:
        await m.answer("Очередь пуста.")
        return
    deleted = delete_by_id(lid)
    await m.answer(f"✅ Удалён последний #{lid} (удалено {deleted}).")

@dp.message(Command("clear_queue"))
async def cmd_clear_queue(m: Message):
    if not _is_admin(m.from_user.id):
        return
    removed = clear_queue()
    await m.answer(f"🗑 Очередь очищена. Удалено: {removed}")

# ---------- intake: пересланные посты и альбомы ----------

@dp.message(F.chat.type.in_({ChatType.PRIVATE}))
async def intake(m: Message):
    if not _is_admin(m.from_user.id):
        return

    # если альбом — копим
    if m.media_group_id:
        if _collect_album(m):
            return

    # одиночное сообщение
    items, caption = build_media_items_from_message(m)

    # если это чистый текст — просто нормализуем и пишем в очередь
    caption = normalize_text(caption)
    qid = enqueue(items, caption, _src_tuple(m))

    # уведомим
    k = "Альбом" if len(items) > 1 else ("Медиа-пост" if items else "Текстовый пост")
    await m.answer(f"✅ {k} добавлен в очередь. #{qid}")

# ---------- entry ----------

async def run_bot():
    init_db()
    # параллельно — планировщик предпросмотра
    asyncio.create_task(scheduler_task())
    log.info("🚀 Стартуем Layoutplace Bot...")
    log.info("Scheduler TZ=%s, times=%s, preview_before=%s мин", TZ_NAME, POST_TIMES, PREVIEW_BEFORE_MIN)
    await dp.start_polling(bot)
