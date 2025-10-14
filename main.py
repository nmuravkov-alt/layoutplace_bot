# main.py
import asyncio
import logging
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from html import escape as html_escape
import re

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, CallbackQuery
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
    job_create,
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

# ---------------- Антиспам/транслит ----------------
FORBIDDEN = {"опт", "оптом", "скидка", "подписчики", "ставки", "казино", "взаимка"}

RU2LAT = str.maketrans({
    "а":"a","б":"b","в":"v","г":"g","д":"d","е":"e","ё":"e","ж":"zh","з":"z","и":"i","й":"y",
    "к":"k","л":"l","м":"m","н":"n","о":"o","п":"p","р":"r","с":"s","т":"t","у":"u","ф":"f",
    "х":"h","ц":"ts","ч":"ch","ш":"sh","щ":"sch","ы":"y","э":"e","ю":"yu","я":"ya",
    "ъ":"", "ь":""
})

def translit_hashtag(s: str) -> str:
    s = s.strip().lower()
    return re.sub(r"[^a-z0-9]+", "", s.translate(RU2LAT))

# ---------------- Шаблон + парсер ----------------
SIZE_TOKENS = {"xs","s","m","l","xl","xxl","xxxl","30","31","32","33","34","36","38","40","42","44","46","48","50","52","54"}
PRICE_RE = re.compile(r"(\d[\d\s]{2,})\s*(?:₽|руб|р|rub|rub\.?)?", re.I)

def parse_struct(text: str):
    """
    Пытаемся вытащить brand/size/price/city/desc из произвольного текста.
    Поддержка ключей: бренд/brand, размер/size, цена/price, город/city, описание/desc.
    Если ключей нет — пытаемся угадать: первая «словесная» — бренд, токен из SIZE_TOKENS — размер,
    число — цена, слово с большой буквы в конце — город.
    """
    raw = text.strip()
    # нормализуем разделители
    lines = re.split(r"[,\n|;]+", raw)

    data = {"brand":"", "size":"", "price":"","city":"", "desc":""}

    # 1) по ключам
    for part in lines:
        p = part.strip()
        low = p.lower()
        if not p:
            continue
        for key, aliases in {
            "brand": ["бренд", "brand"],
            "size": ["размер", "size"],
            "price":["цена","price"],
            "city": ["город","city","г."],
            "desc": ["описание","desc","опис"],
        }.items():
            for a in aliases:
                if low.startswith(a + ":") or low.startswith(a+" "):
                    data[key] = p.split(":",1)[-1].strip()
                    break

    # 2) эвристики
    tokens = [t.strip() for t in lines if t.strip()]

    # бренд — первое слово, если не задано
    if not data["brand"] and tokens:
        data["brand"] = tokens[0].split()[0].title()

    # размер — из токенов
    if not data["size"]:
        for t in tokens:
            tt = t.lower().strip()
            if tt in SIZE_TOKENS:
                data["size"] = tt.upper()
                break
            # вариант "Размер L"
            m = re.search(r"(?:размер|size)\s*([XSML\d]{1,4})", tt, re.I)
            if m:
                data["size"] = m.group(1).upper()
                break

    # цена — ищем число
    if not data["price"]:
        m = PRICE_RE.search(raw)
        if m:
            data["price"] = re.sub(r"\s+","", m.group(1))

    # город — последнее "слово с большой" или после "г."
    if not data["city"]:
        m = re.search(r"(?:г\.|город)\s*([A-Za-zА-Яа-я\- ]{2,})", raw, re.I)
        if m:
            data["city"] = m.group(1).strip().title()
        else:
            words = [w for w in re.split(r"[\n,;]+", raw) if w.strip()]
            if words:
                tail = words[-1].strip()
                if len(tail.split())<=3 and any(ch.isalpha() for ch in tail):
                    data["city"] = tail.title()

    # описание — весь текст минус найденные куски (упрощённо)
    data["desc"] = raw

    return data

def format_post(d):
    brand = d.get("brand","").strip()
    size  = d.get("size","").strip().upper()
    price = d.get("price","").strip()
    city  = d.get("city","").strip().title()
    desc  = d.get("desc","").strip()

    # авто-хэштеги (латиница)
    tags = []
    if brand:
        tags.append("#" + translit_hashtag(brand))
    if size:
        tags.append("#" + translit_hashtag(size))
    if city:
        tags.append("#" + translit_hashtag(city))

    price_line = f"💸 Цена: {price}₽" if price else "💸 Цена: —"
    size_line  = f"📏 Размер: {size}" if size else "📏 Размер: —"
    brand_line = f"👕 Бренд: {brand}" if brand else "👕 Бренд: —"
    city_line  = f"📍 Город: {city}" if city else "📍 Город: —"

    body = (
        f"{brand_line}\n"
        f"{size_line}\n"
        f"{price_line}\n"
        f"{city_line}\n—\n"
        f"{desc}\n"
        f"{' '.join(tags)}"
    ).strip()
    return body

# ---------------- Утилиты ----------------
def _is_admin(m: Message | CallbackQuery) -> bool:
    uid = (m.from_user.id if m.from_user else None) if m else None
    return bool(uid and uid in ADMINS)

def _now_str() -> str:
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

async def safe_send_channel(text: str):
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
        "/enqueue &lt;текст&gt; — добавить объявление (с авто-шаблоном, хэштегами и антиспамом)\n"
        "/queue — размер очереди\n"
        "/queue_list [N] — показать N ближайших (по умолчанию 10)\n"
        "/delete &lt;id&gt; — удалить объявление по ID\n"
        "/post_oldest — опубликовать самое старое и удалить похожие\n"
        "/post_at HH:MM — запланировать публикацию <i>самого старого</i> на время (МСК)\n"
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

    # антиспам
    low = raw.lower()
    if any(word in low for word in FORBIDDEN):
        return await m.answer("🚫 Объявление отклонено антиспамом.")

    # парсинг + шаблон
    data = parse_struct(raw)
    formatted = format_post(data)

    # анти-дубль
    dup_id = is_duplicate(formatted)
    if dup_id:
        return await m.answer(f"⚠️ Такой пост уже в очереди (ID: <code>{dup_id}</code>).")

    ad_id = db_enqueue(formatted)
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
    try:
        n = int((command.args or "").strip() or "10")
        n = max(1, min(50, n))
    except ValueError:
        n = 10
    items = db_list_queue(n)
    if not items:
        return await m.answer("Очередь пуста.")
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

    await safe_send_channel(text)

    similar = find_similar_ids(ad_id, threshold=0.88)
    removed = bulk_delete([ad_id] + similar)

    now_h = _now_str()
    await _notify_admins(
        f"✅ Опубликовано ({now_h}). ID: <code>{ad_id}</code>. "
        f"Удалено похожих (включая исходный): <b>{removed}</b>."
    )
    if POST_REPORT_TO_CHANNEL:
        await safe_send_channel(f"ℹ️ Пост опубликован. ID: {ad_id}. Удалено похожих: {removed}.")
    await m.answer("✅ Опубликовано в канал.\n" f"🗑 Удалено (вместе с похожими): <b>{removed}</b>")

@dp.message(Command("post_at"))
async def cmd_post_at(m: Message, command: CommandObject):
    """Запланировать публикацию САМОГО СТАРОГО объявления на HH:MM по МСК"""
    if not _is_admin(m):
        return await m.answer("Нет прав.")
    arg = (command.args or "").strip()
    if not re.match(r"^\d{1,2}:\d{2}$", arg):
        return await m.answer("Использование: /post_at HH:MM (по МСК)")

    hh, mm = map(int, arg.split(":"))
    if hh < 0 or hh > 23 or mm < 0 or mm > 59:
        return await m.answer("Неверное время. Пример: /post_at 18:30")

    row = get_oldest()
    if not row:
        return await m.answer("Очередь пуста, нечего планировать.")

    ad_id, _ = row
    now = datetime.now(tz)
    run_at = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if run_at <= now:
        run_at += timedelta(days=1)

    job_id = job_create(ad_id, int(run_at.timestamp()))
    await m.answer(
        f"🗓 Запланировано: ID <code>{ad_id}</code> на {run_at.strftime('%Y-%m-%d %H:%M')} ({TZ}).\n"
        f"Job: <code>{job_id}</code>"
    )

# ---------------- Callback «Опубликовать сейчас» ----------------
@dp.callback_query(F.data == "postnow")
async def cb_postnow(q: CallbackQuery):
    if not _is_admin(q):
        return await q.answer("Нет прав.", show_alert=True)

    row = get_oldest()
    if not row:
        await q.answer("Очередь пуста.", show_alert=True)
        return
    ad_id, text = row

    await safe_send_channel(text)
    similar = find_similar_ids(ad_id, threshold=0.88)
    removed = bulk_delete([ad_id] + similar)

    now_h = _now_str()
    await _notify_admins(
        f"✅ Опубликовано по кнопке «Опубликовать сейчас» ({now_h}). "
        f"ID: <code>{ad_id}</code>. Удалено похожих: <b>{removed}</b>."
    )
    if POST_REPORT_TO_CHANNEL:
        await safe_send_channel(f"ℹ️ Пост опубликован. ID: {ad_id}. Удалено похожих: {removed}.")

    try:
        await q.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await q.answer("Опубликовано.", show_alert=False)

# ---------------- Точка входа ----------------
async def main():
    init_db()
    log.info("✅ Бот запущен для %s (TZ=%s)", CHANNEL_ID, TZ)
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
