import asyncio
import logging
import os
from datetime import datetime, timedelta

import pytz
from aiogram import Bot, Dispatcher, F, types
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message, InputMediaPhoto, InlineKeyboardButton, InlineKeyboardMarkup

# ---------------------------
# Логи
# ---------------------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("layoutplace_bot")

# ---------------------------
# ENV
# ---------------------------
TOKEN = os.getenv("TOKEN", "").strip()
if not TOKEN or ":" not in TOKEN:
    raise RuntimeError("ENV TOKEN пуст или имеет неверный формат. Задайте корректный токен бота.")

CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()
# допускаем @username или -100...
if CHANNEL_ID.startswith("@"):
    TARGET_CHAT = CHANNEL_ID  # username канала
else:
    # пробуем привести к int
    try:
        TARGET_CHAT = int(CHANNEL_ID)
    except Exception:
        raise RuntimeError("ENV CHANNEL_ID должен быть @username или -100XXXXXXXXXX")

ADMINS = [int(x) for x in os.getenv("ADMINS", "").split(",") if x.strip().isdigit()]
if not ADMINS:
    log.warning("ADMINS не задан — превью/уведомления прислать будет некому.")

TZ = os.getenv("TZ", "Europe/Moscow")
POST_TIMES = [t.strip() for t in os.getenv("POST_TIMES", "12:00,16:00,20:00").split(",") if t.strip()]
PREVIEW_MINUTES = int(os.getenv("PREVIEW_MINUTES", "45"))

# Единый хвост поста (можно править ENV-ами при желании)
ALBUM_URL = os.getenv("ALBUM_URL", "https://vk.com/market-222108341?screen=group&section=album_26")
CONTACT_TEXT = os.getenv("CONTACT_TEXT", "@layoutplacebuy")

bot = Bot(token=TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()

# ---------------------------
# Очередь постов (в памяти)
# Элемент: {"items":[{"type":"photo","file_id":...}, ...], "caption": "..."}
# ---------------------------
QUEUE: list[dict] = []

# ---------------------------
# Буфер альбомов: по пользователю храним текущую сборку
# KEY: user_id
# VALUE: {"mg_id": str, "items": [...], "caption": str, "ts": float}
# ---------------------------
ALBUM_BUFFER: dict[int, dict] = {}

ALBUM_COLLECT_WINDOW = 1.0  # секунды ожидания довлета всех частей альбома


# ===========================
# ========= УТИЛЫ ===========
# ===========================

def build_caption(raw: str) -> str:
    """
    Приводим к общему виду: добавляем внизу неизменяемые строки.
    Без эмодзи — оставляем как есть, только добавляем хвост.
    """
    raw = (raw or "").strip()

    tail = (
        "\n\n"
        f"Общий альбом: {ALBUM_URL}\n"
        f"Покупка/вопросы: {CONTACT_TEXT}"
    )
    # Не дублируем, если пользователь сам вставил хвост
    if "Покупка/вопросы:" in raw or "Общий альбом:" in raw:
        return raw
    return (raw + tail).strip()


def _pick_preview_text(items: list[dict], caption: str) -> str:
    photos = sum(1 for x in items if x["type"] == "photo")
    base = caption.strip() or "(без подписи)"
    prefix = f"Фотографий: {photos}\n\n" if photos else ""
    return prefix + base


async def _notify_admins(text: str):
    for aid in ADMINS:
        try:
            await bot.send_message(aid, text, disable_web_page_preview=True)
        except Exception as e:
            log.warning(f"Не удалось отправить админу {aid}: {e}")


async def _delete_last_in_channel():
    """
    Мягко пытаемся удалить последнее сообщение в канале, чтобы не было дублей.
    Для username-канала Telegram не даёт получить историю через get_chat_history,
    поэтому используем chat.get_updates — в aiogram нет готового. Делать нельзя.
    Решение: делаем «мягкое удаление» только для id-каналов (supergroup/private).
    """
    if isinstance(TARGET_CHAT, int):
        try:
            # Получить последнее сообщение нельзя напрямую,
            # но можно попытаться удалить «предыдущее» опубликованное нами,
            # если мы его сохранили. Для простоты — пропускаем.
            pass
        except Exception:
            pass
    # Ничего не делаем для username-каналов — Telegram API ограничивает.


async def publish_to_channel(items: list[dict], caption: str) -> list[int]:
    """
    Публикует пост в канал (или один медиа, или альбом).
    Возвращает список message_id опубликованных сообщений.
    """
    await _delete_last_in_channel()  # «мягкое» — см. комментарий внутри

    published_ids: list[int] = []
    safe_caption = caption.strip()

    photos = [it for it in items if it["type"] == "photo"]

    # 1 фото
    if len(photos) == 1:
        msg = await bot.send_photo(TARGET_CHAT, photos[0]["file_id"], caption=safe_caption)
        published_ids.append(msg.message_id)

    # альбом
    elif len(photos) > 1:
        media = []
        for idx, ph in enumerate(photos):
            if idx == 0:
                media.append(InputMediaPhoto(media=ph["file_id"], caption=safe_caption))
            else:
                media.append(InputMediaPhoto(media=ph["file_id"]))
        msgs = await bot.send_media_group(TARGET_CHAT, media=media)
        published_ids.extend(m.message_id for m in msgs)

    # без фото — текст
    else:
        msg = await bot.send_message(TARGET_CHAT, safe_caption, disable_web_page_preview=True)
        published_ids.append(msg.message_id)

    # уведомление админам
    await _notify_admins("✅ Пост опубликован.\n\n" + safe_caption[:1000])
    return published_ids


def _merge_album_piece(user_id: int, msg: Message):
    """Сливаем очередной кусок альбома в буфер для этого пользователя."""
    mg_id = msg.media_group_id
    if not mg_id:
        return

    buf = ALBUM_BUFFER.get(user_id)
    if not buf or buf.get("mg_id") != mg_id:
        # создаём новый буфер
        ALBUM_BUFFER[user_id] = {
            "mg_id": mg_id,
            "items": [],
            "caption": msg.caption or "",
            "ts": asyncio.get_running_loop().time(),
        }
        buf = ALBUM_BUFFER[user_id]

    # файлик
    if msg.photo:
        buf["items"].append({"type": "photo", "file_id": msg.photo[-1].file_id})

    # захватываем подпись только если она появилась и ранее пустая
    if msg.caption and not buf.get("caption"):
        buf["caption"] = msg.caption

    # обновляем таймштамп
    buf["ts"] = asyncio.get_running_loop().time()


async def _finalize_album_later(user_id: int, mg_id: str):
    """Через ALBUM_COLLECT_WINDOW секунд считаем, что альбом собран."""
    await asyncio.sleep(ALBUM_COLLECT_WINDOW)
    buf = ALBUM_BUFFER.get(user_id)
    if not buf:
        return
    if buf.get("mg_id") != mg_id:
        return
    # просто держим буфер — команда /add_post его подхватит
    # чистить не будем до /add_post (или 2 минуты неактивности)


def _get_ready_album_from_buffer(user_id: int) -> tuple[list[dict], str] | None:
    """Если в буфере есть актуальный альбом (за последние ~120с) — возвращаем его."""
    buf = ALBUM_BUFFER.get(user_id)
    if not buf:
        return None
    now = asyncio.get_running_loop().time()
    if now - buf.get("ts", 0) > 120:
        # протух
        ALBUM_BUFFER.pop(user_id, None)
        return None
    items = buf.get("items", [])
    caption = buf.get("caption") or ""
    if items:
        # НЕ очищаем — на случай, если пользователь нажмёт /add_post повторно по ошибке.
        return items, caption
    return None


def _extract_single_from_message(m: Message) -> tuple[list[dict], str]:
    """Извлекаем ОДНО фото и подпись из конкретного сообщения (не альбом)."""
    items: list[dict] = []
    caption = m.caption or m.text or ""
    if m.photo:
        items.append({"type": "photo", "file_id": m.photo[-1].file_id})
    return items, caption


# ===========================
# ======= ХЕНДЛЕРЫ =========
# ===========================

@dp.message(Command("start"))
async def cmd_start(m: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Постить сейчас (самый старый)", callback_data="post_oldest")
    ]])
    text = (
        "Привет! Перешли мне пост из канала (с фото/альбомом и описанием), затем отправь /add_post — "
        "я добавлю его в очередь и опубликую по расписанию (12:00 / 16:00 / 20:00). "
        f"За {PREVIEW_MINUTES} минут до выхода пришлю превью в личку админам.\n\n"
        "Команды:\n"
        "• /add_post — добавить последний пересланный пост (или альбом) в очередь\n"
        "• /post_oldest — вручную опубликовать самый старый пост из очереди\n"
        "• /queue — показать размер очереди\n"
    )
    await m.answer(text, reply_markup=kb)


@dp.callback_query(F.data == "post_oldest")
async def cq_post_oldest(cq: types.CallbackQuery):
    await _post_oldest_impl(cq.message)
    await cq.answer("Опубликовано")


@dp.message(Command("queue"))
async def cmd_queue(m: Message):
    await m.answer(f"В очереди: {len(QUEUE)}.")


@dp.message(F.media_group_id)
async def on_any_album_piece(m: Message):
    """
    Любая часть альбома попадает сюда: складываем в буфер и запускаем таймер.
    Это нужно, чтобы потом /add_post смог забрать весь альбом целиком.
    """
    user_id = m.from_user.id
    _merge_album_piece(user_id, m)
    asyncio.create_task(_finalize_album_later(user_id, m.media_group_id))


@dp.message(Command("add_post"))
async def cmd_add_post(m: Message):
    """
    Добавляет пост в очередь.
    Логика:
      1) если недавно пересылали альбом — забираем его из буфера
      2) иначе берём одиночное сообщение (реплай или сам m)
    """
    user_id = m.from_user.id

    # 1) пробуем взять готовый альбом
    ready = _get_ready_album_from_buffer(user_id)
    if ready:
        items, caption = ready
    else:
        # 2) одиночное
        src = m.reply_to_message or m
        items, caption = _extract_single_from_message(src)

    if not items and not caption:
        await m.answer("❌ Не нашёл ни фото/альбома, ни текста. Перешли пост и снова /add_post.")
        return

    final_caption = build_caption(caption)
    QUEUE.append({"items": items, "caption": final_caption})
    await m.answer("✅ Пост добавлен в очередь.")


@dp.message(Command("post_oldest"))
async def cmd_post_oldest(m: Message):
    await _post_oldest_impl(m)


async def _post_oldest_impl(m: Message):
    if not QUEUE:
        await m.answer("Очередь пуста.")
        return
    task = QUEUE.pop(0)
    await publish_to_channel(task["items"], task["caption"])
    await m.answer("📢 Старый пост опубликован.")


# ===========================
# ======= ПЛАНИРОВЩИК =======
# ===========================

async def scheduler():
    tz = pytz.timezone(TZ)
    log.info(f"Scheduler TZ={TZ}, times={','.join(POST_TIMES)}, preview_before={PREVIEW_MINUTES} min")
    seen_preview_for_minute: set[str] = set()  # защита от дублей превью в одну минуту
    seen_post_for_minute: set[str] = set()     # защита от дублей поста в одну минуту

    while True:
        now = datetime.now(tz)

        for t_str in POST_TIMES:
            hh, mm = map(int, t_str.split(":"))
            slot_dt = now.replace(hour=hh, minute=mm, second=0, microsecond=0)

            # если слот уже прошёл сегодня — сдвигаем на завтра при сравнении
            if slot_dt < now - timedelta(minutes=61):
                slot_dt = slot_dt + timedelta(days=1)

            # превью-окно
            preview_dt = slot_dt - timedelta(minutes=PREVIEW_MINUTES)

            # ключи для «один раз в минуту»
            prev_key = f"{preview_dt:%Y%m%d%H%M}"
            post_key = f"{slot_dt:%Y%m%d%H%M}"

            # превью
            if now.strftime("%Y%m%d%H%M") == prev_key and prev_key not in seen_preview_for_minute:
                seen_preview_for_minute.add(prev_key)
                if QUEUE:
                    preview_text = _pick_preview_text(QUEUE[0]["items"], QUEUE[0]["caption"])
                    await _notify_admins(f"⏰ Через {PREVIEW_MINUTES} минут запланирован пост:\n\n{preview_text[:1500]}")

            # публикация
            if now.strftime("%Y%m%d%H%M") == post_key and post_key not in seen_post_for_minute:
                seen_post_for_minute.add(post_key)
                if QUEUE:
                    task = QUEUE.pop(0)
                    await publish_to_channel(task["items"], task["caption"])

        await asyncio.sleep(2)  # частота проверки


# ===========================
# ========== RUN ============
# ===========================

async def run_bot():
    asyncio.create_task(scheduler())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(run_bot())
