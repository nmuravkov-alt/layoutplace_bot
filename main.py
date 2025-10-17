import os
import asyncio
import logging
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    Message, InputMediaPhoto, InlineKeyboardMarkup, InlineKeyboardButton
)

from storage.db import (
    init_db, enqueue, dequeue_oldest, peek_oldest, get_count, set_meta, get_meta
)

# ============== ЛОГИ ==============
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("layoutplace_bot")
sched_log = logging.getLogger("layoutplace_scheduler")

# ============== НАСТРОЙКИ ИЗ ENV ==============
TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not TOKEN or ":" not in TOKEN:
    raise RuntimeError("Переменная окружения BOT_TOKEN не задана или некорректна.")

# ID канала, куда постим. Можно @username, но для удаления старого сообщения нужен числовой id.
CHANNEL_ID_ENV = os.getenv("CHANNEL_ID", "").strip()
CHANNEL_ID = CHANNEL_ID_ENV if CHANNEL_ID_ENV.startswith("@") else int(CHANNEL_ID_ENV or "-1000000000000")

ADMINS = []
for raw in os.getenv("ADMINS", "").replace(";", ",").split(","):
    raw = raw.strip()
    if raw:
        try:
            ADMINS.append(int(raw))
        except:
            pass

TZ = os.getenv("TZ", "Europe/Moscow")
ZONE = ZoneInfo(TZ)

POST_TIMES_RAW = os.getenv("POST_TIMES", "12:00,16:00,20:00")
POST_TIMES = []
for t in POST_TIMES_RAW.split(","):
    t = t.strip()
    if not t:
        continue
    hh, mm = t.split(":")
    POST_TIMES.append(dtime(hour=int(hh), minute=int(mm)))

PREVIEW_BEFORE_MIN = int(os.getenv("PREVIEW_BEFORE_MIN", "45"))

ALBUM_URL = os.getenv("ALBUM_URL", "https://vk.com/market-222108341?screen=group&section=album_26")
CONTACT  = os.getenv("CONTACT", "@layoutplacebuy")

# ============== ИНИТ БОТА ==============
bot = Bot(TOKEN, parse_mode=None)  # без parse_mode — чтобы не падать на «can't parse entities»
dp = Dispatcher()


# ============== ХЕЛПЕРЫ ==============
def _now():
    return datetime.now(ZONE)

def _next_slots():
    """Вернёт список ближайших слотов (сегодня/завтра) как datetime."""
    now = _now()
    today = now.date()
    slots = []
    for tt in POST_TIMES:
        dt = datetime.combine(today, tt, tzinfo=ZONE)
        if dt >= now:
            slots.append(dt)
    if not slots:
        # все на сегодня прошли — добавляем завтрашние
        tomorrow = today + timedelta(days=1)
        for tt in POST_TIMES:
            slots.append(datetime.combine(tomorrow, tt, tzinfo=ZONE))
    return slots

def _slot_key(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M")

def _preview_key(dt: datetime) -> str:
    return f"preview_sent::{_slot_key(dt)}"

def format_caption(original: str) -> str:
    """
    Приводим к единому стилю без эмодзи и добавляем две неизменяемые строки.
    Ничего не форматируем в HTML, просто чистим пробелы.
    """
    if not original:
        original = ""
    text = original.replace("\u200b", "").strip()  # убрать zero-width
    lines = [ln.rstrip() for ln in text.splitlines()]
    # Удаляем пустые хвосты
    while lines and not lines[-1].strip():
        lines.pop()
    base = "\n".join(lines).strip()

    tail = (
        f"\n\nОбщий альбом: {ALBUM_URL}\n"
        f"Покупка/вопросы: {CONTACT}"
    )
    # Если уже есть эти строки — не дублируем
    if ALBUM_URL in base:
        tail = tail.replace(f"\n\nОбщий альбом: {ALBUM_URL}", "")
    if CONTACT in base:
        tail = tail.replace(f"\nПокупка/вопросы: {CONTACT}", "")

    return (base + tail).strip()


async def _notify_admins(text: str, kb: InlineKeyboardMarkup | None = None):
    for aid in ADMINS:
        try:
            await bot.send_message(aid, text, reply_markup=kb)
        except Exception as e:
            sched_log.warning(f"Админ {aid} недоступен: {e}")


async def _send_preview_for_slot(slot_dt: datetime):
    """Отправить превью СТАРЕЙШЕГО поста админу за PREVIEW_BEFORE_MIN до слота. Не постить!"""
    # не дублируем
    flag_key = _preview_key(slot_dt)
    if get_meta(flag_key, "0") == "1":
        return

    item = peek_oldest()
    if not item:
        await _notify_admins(f"🔔 [{_slot_key(slot_dt)}] В очереди нет постов.")
        set_meta(flag_key, "1")
        return

    # Подготовим подпись в едином стиле
    cap = format_caption(item["caption"])

    # Соберём превью сообщения для админа
    header = f"🔔 Превью [{_slot_key(slot_dt)}]\nБудет доступен к постингу через /post_oldest"
    footer = f"\n\nВ очереди сейчас: {get_count()}."
    text = f"{header}\n\n{cap}{footer}"

    # Кнопка-подсказка
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Запостить старейший (/post_oldest)", callback_data="noop")]
    ])

    # Пытаемся приложить медиа: если это набор — шлём группу, иначе фото + текст
    items = item["items"]
    try:
        if items and len(items) > 1:
            media = []
            for i, it in enumerate(items):
                if it["type"] == "photo":
                    if i == 0:
                        media.append(InputMediaPhoto(media=it["file_id"], caption=text))
                    else:
                        media.append(InputMediaPhoto(media=it["file_id"]))
            # отправим каждому админу
            for aid in ADMINS:
                try:
                    msgs = await bot.send_media_group(aid, media)
                    # догонка клавиатуры отдельным сообщением
                    await bot.send_message(aid, "Нажми /post_oldest в чате с ботом, когда будет время постинга.", reply_markup=kb)
                except Exception as e:
                    sched_log.warning(f"Не удалось отправить превью альбом админу {aid}: {e}")
        else:
            # одиночное фото или без фото
            if items and items[0]["type"] == "photo":
                for aid in ADMINS:
                    try:
                        await bot.send_photo(aid, photo=items[0]["file_id"], caption=text, reply_markup=kb)
                    except Exception as e:
                        sched_log.warning(f"Не удалось отправить превью фото админу {aid}: {e}")
            else:
                await _notify_admins(text, kb=kb)
    finally:
        # отметим, что превью отправлено для этого слота
        set_meta(flag_key, "1")


async def scheduler_loop():
    """
    Планировщик ТОЛЬКО присылает превью за PREVIEW_BEFORE_MIN до слота.
    Ничего автоматически не постит — постинг вручную через /post_oldest.
    """
    sched_log.info(f"Scheduler TZ={TZ}, times={','.join([t.strftime('%H:%M') for t in POST_TIMES])}, preview_before={PREVIEW_BEFORE_MIN} min")

    while True:
        try:
            slots = _next_slots()
            if slots:
                slot = slots[0]
                preview_at = slot - timedelta(minutes=PREVIEW_BEFORE_MIN)
                now = _now()

                # если мы пересекли момент превью (или ровно попали) — шлём превью
                if now >= preview_at and now < slot + timedelta(minutes=1):
                    await _send_preview_for_slot(slot)
        except Exception as e:
            sched_log.error(f"scheduler error: {e}")
        await asyncio.sleep(20)  # достаточно часто, но без фанатизма


# ============== ОБРАБОТКА ВХОДЯЩИХ ==============

# Временное накопление альбомов по media_group_id (живёт в памяти процесса)
_MEDIA_CACHE: dict[str, dict] = {}
_MEDIA_TTL_SEC = 10

def _cleanup_media_cache():
    now = datetime.now().timestamp()
    to_del = []
    for k, v in _MEDIA_CACHE.items():
        if now - v["ts"] > _MEDIA_TTL_SEC:
            to_del.append(k)
    for k in to_del:
        _MEDIA_CACHE.pop(k, None)

def _src_tuple(m: Message):
    """
    Если сообщение переслано из канала — вернём (chat_id, message_id) оригинала,
    чтобы потом попытаться удалить.
    """
    try:
        if m.forward_from_chat and (str(getattr(m.forward_from_chat, "type", "")) == "channel"):
            src_chat_id = m.forward_from_chat.id
            src_msg_id = getattr(m, "forward_from_message_id", None)
            if src_chat_id and src_msg_id:
                return (int(src_chat_id), int(src_msg_id))
    except:
        pass
    return None

@dp.message(Command("start"))
async def cmd_start(m: Message):
    text = (
        "Привет!\n\n"
        "Команды:\n"
        "• /add_post — перешли пост из канала (фото/альбом + описание), я добавлю в очередь.\n"
        "• /queue — показать размер очереди.\n"
        "• /post_oldest — запостить старейший пост из очереди в канал.\n\n"
        f"Время слотов: {', '.join([t.strftime('%H:%M') for t in POST_TIMES])} ({TZ}).\n"
        f"Превью за {PREVIEW_BEFORE_MIN} минут — в ЛС админам."
    )
    await m.answer(text)

@dp.message(Command("queue"))
async def cmd_queue(m: Message):
    await m.answer(f"В очереди: {get_count()}.")

@dp.message(Command("post_oldest"))
async def cmd_post_oldest(m: Message):
    # Только админы
    if m.from_user and m.from_user.id not in ADMINS:
        return
    task = dequeue_oldest()
    if not task:
        await m.answer("Очередь пустая.")
        return

    caption = format_caption(task["caption"])
    items = task["items"]

    try:
        if items and len(items) > 1:
            media = []
            for i, it in enumerate(items):
                if it["type"] == "photo":
                    if i == 0:
                        media.append(InputMediaPhoto(media=it["file_id"], caption=caption))
                    else:
                        media.append(InputMediaPhoto(media=it["file_id"]))
            msgs = await bot.send_media_group(CHANNEL_ID, media)
            posted_msg_id = msgs[0].message_id if msgs else None
        else:
            if items and items[0]["type"] == "photo":
                msg = await bot.send_photo(CHANNEL_ID, photo=items[0]["file_id"], caption=caption)
                posted_msg_id = msg.message_id
            else:
                # на всякий — текстом
                msg = await bot.send_message(CHANNEL_ID, caption)
                posted_msg_id = msg.message_id

        # попытка удалить старое сообщение в источнике
        if task["src"]:
            try:
                await bot.delete_message(task["src"][0], task["src"][1])
            except Exception as e:
                logging.warning(f"Не смог удалить старое сообщение {task['src'][0]}/{task['src'][1]}: {e}")

        await m.answer(f"Готово. Опубликовано.")
    except Exception as e:
        await m.answer(f"Не удалось запостить: {e}")

@dp.message(Command("add_post"))
async def cmd_add_post_hint(m: Message):
    await m.answer("Перешли боту пост из канала (фото/альбом с подписью). Я добавлю в очередь.")

@dp.message(F.media_group_id | F.photo | F.caption | F.forward_from_chat)
async def any_message(m: Message):
    """
    Ловим пересланные из канала посты.
    Поддерживаем одиночные фото и альбомы (по media_group_id).
    """
    # Только админы могут добавлять
    if not m.from_user or m.from_user.id not in ADMINS:
        return

    _cleanup_media_cache()

    # Если альбом
    if m.media_group_id:
        key = str(m.media_group_id)
        bucket = _MEDIA_CACHE.get(key)
        if not bucket:
            bucket = {"ts": datetime.now().timestamp(), "items": [], "caption": "", "src": _src_tuple(m)}
            _MEDIA_CACHE[key] = bucket

        # накапливаем фото
        if m.photo:
            photo = m.photo[-1]  # максимальное качество
            bucket["items"].append({"type": "photo", "file_id": photo.file_id})
        # подпись только один раз возьмём, любую непустую
        if (m.caption or "").strip():
            bucket["caption"] = m.caption.strip()

        # подождём, пока весь альбом придёт; пользователь сам ничего не жмёт —
        # мы добавим альбом в очередь по команде от Telegram после таймаута.
        # Простая эвристика: как только фотка пришла — ставим короткую задержку и оформляем.
        await asyncio.sleep(1.0)
        # если новых частей не прибыло за TTL — считаем альбом законченным и кладём в очередь
        # (практически — альбом добавится после последнего элемента)
        qid = enqueue(bucket["items"], bucket["caption"], bucket["src"])
        _MEDIA_CACHE.pop(key, None)
        await m.answer(f"Добавил в очередь альбом. Сейчас в очереди: {get_count()}.")
        return

    # Одиночное фото/текст
    items = []
    if m.photo:
        items.append({"type": "photo", "file_id": m.photo[-1].file_id})

    caption = (m.caption or m.text or "").strip()
    src = _src_tuple(m)
    if not items and not caption:
        return  # не интересует

    qid = enqueue(items, caption, src)
    await m.answer(f"Добавил в очередь. Сейчас в очереди: {get_count()}.")


# ============== СТАРТ ==============
async def run_bot():
    init_db()
    log.info(f"Starting bot instance...")
    # отдельная корутина-планировщик
    asyncio.create_task(scheduler_loop())
    await dp.start_polling(bot)
