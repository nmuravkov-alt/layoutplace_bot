import asyncio
import logging
import os
from datetime import datetime, timedelta

import pytz
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandObject
from aiogram.enums import ParseMode
from aiogram.types import Message, InputMediaPhoto
from aiogram.utils.media_group import MediaGroupBuilder

from storage.db import init_db, enqueue, dequeue_oldest, get_count, peek_oldest, get_all
from utils import normalize_text, build_final_caption

# ----------------- CONFIG -----------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("layoutplace_bot")

TOKEN = os.getenv("TOKEN", "").strip()
if not TOKEN or ":" not in TOKEN:
    raise RuntimeError("ENV TOKEN пуст или имеет неверный формат. Задайте корректный токен бота.")

# Комма-разделённый список ID админов
ADMINS = []
_adm = os.getenv("ADMINS", "").strip()
if _adm:
    for part in _adm.replace(";", ",").split(","):
        p = part.strip()
        if p.isdigit():
            ADMINS.append(int(p))
# На всякий случай подхватим дефолт из переписки
for default_id in (469734432, 6773668793):
    if default_id not in ADMINS:
        ADMINS.append(default_id)

# Куда постим
# Можно указать @username канала, но для удаления старого нужен numeric id формата -100...
_channel_env = os.getenv("CHANNEL_ID", "").strip()
if _channel_env.startswith("@"):
    CHANNEL_ID = _channel_env  # постить можно и так
else:
    try:
        CHANNEL_ID = int(_channel_env)
    except Exception:
        CHANNEL_ID = -1001758490510  # твой канал из переписки

TZ = os.getenv("TZ", "Europe/Moscow")
tz = pytz.timezone(TZ)

# слоты автопоста
POST_TIMES = [t.strip() for t in os.getenv("POST_TIMES", "12:00,16:00,20:00").split(",")]
PREVIEW_BEFORE_MIN = int(os.getenv("PREVIEW_BEFORE_MIN", "45"))

# неизменяемые ссылки
ALBUM_URL = os.getenv("ALBUM_URL", "https://vk.com/market-222108341?screen=group&section=album_26")
CONTACT = os.getenv("CONTACT", "@layoutplacebuy")

bot = Bot(TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()

# Буфер для альбомов (media_group)
ALBUM_BUFFER = {}  # media_group_id -> list[photo_file_id]
ALBUM_TTL_SEC = 90


# ----------------- HELP -----------------
def help_text() -> str:
    return (
        "Команды:\n"
        "/queue – показать очередь\n"
        "/post_oldest – постить самый старый из очереди (без автодогонялки)\n"
        "/test_preview – прислать превью ближайшего слота\n\n"
        "Как добавить пост:\n"
        "• Перешли боту пост (фото+подпись или просто фото/текст). Бот сам положит в очередь в едином стиле.\n"
        "• Для альбомов (несколько фото) пересылай как альбом.\n"
        "\nПубликация:\n"
        "• Автослоты: 12:00 / 16:00 / 20:00 (TZ=" + TZ + ")\n"
        f"• Превью за {PREVIEW_BEFORE_MIN} мин в ЛС админам.\n"
        "• Старый оригинал после постинга удаляется (если можно).\n"
    )


# ----------------- UTIL -----------------
def _now():
    return datetime.now(tz)


def _src_tuple(m: Message):
    # если сообщение переслано из канала — сохраняем откуда
    try:
        if m.forward_from_chat and (str(getattr(m.forward_from_chat, "type", "")) == "channel"):
            return (m.forward_from_chat.id, m.forward_from_message_id)
    except Exception:
        pass
    # если нет — попробуем из reply_to (когда добавляют по ответу на канал)
    if m.reply_to_message and (str(getattr(m.reply_to_message.chat, "type", "")) == "channel"):
        return (m.reply_to_message.chat.id, m.reply_to_message.message_id)
    return (None, None)


async def _send_preview_to_admins(text: str):
    for aid in ADMINS:
        try:
            await bot.send_message(aid, text, disable_web_page_preview=True)
        except Exception as e:
            log.warning(f"Админ {aid} недоступен: {e}")


async def _send_preview_of_oldest():
    peek = peek_oldest()
    if not peek:
        return
    items = peek["items"]
    cap = peek["caption"] or ""
    uni = build_final_caption(normalize_text(cap), ALBUM_URL, CONTACT)

    # отправляем КАК ПРЕВЬЮ в ЛС админам (только текст и первая картинка)
    head = f"⚠️ Превью следующего поста (id={peek['id']})\n\n{uni}"
    if items and any(i.get("type") == "photo" for i in items):
        first_photo = next(i for i in items if i.get("type") == "photo")
        for aid in ADMINS:
            try:
                await bot.send_photo(aid, first_photo["file_id"], caption=head)
            except Exception as e:
                log.warning(f"Не смог отправить превью фото админу {aid}: {e}")
    else:
        await _send_preview_to_admins(head)


async def _post_task(task: dict) -> bool:
    """Постит одну задачу в канал, пытается удалить источник."""
    items = task["items"]
    cap_raw = task["caption"] or ""
    caption = build_final_caption(normalize_text(cap_raw), ALBUM_URL, CONTACT)

    try:
        photos = [i for i in items if i.get("type") == "photo"]
        if len(photos) > 1:
            mg = MediaGroupBuilder()
            for idx, p in enumerate(photos):
                if idx == 0:
                    mg.add_photo(media=p["file_id"], caption=caption)
                else:
                    mg.add_photo(media=p["file_id"])
            await bot.send_media_group(CHANNEL_ID, media=mg.build())
        elif len(photos) == 1:
            await bot.send_photo(CHANNEL_ID, photo=photos[0]["file_id"], caption=caption)
        else:
            # только текст
            await bot.send_message(CHANNEL_ID, caption, disable_web_page_preview=True)

        # удалить источник
        if task.get("src"):
            src_chat_id, src_msg_id = task["src"]
            if src_chat_id and src_msg_id:
                try:
                    await bot.delete_message(src_chat_id, src_msg_id)
                except Exception as e:
                    log.warning(f"Не смог удалить старое сообщение {src_chat_id}/{src_msg_id}: {e}")

        return True
    except Exception as e:
        log.exception(f"Публикация не удалась: {e}")
        return False


def _next_slots_today():
    base = _now().replace(second=0, microsecond=0)
    out = []
    for ts in POST_TIMES:
        hh, mm = ts.split(":")
        candidate = base.replace(hour=int(hh), minute=int(mm))
        out.append(candidate)
    return out


# ----------------- SCHED -----------------
async def scheduler_loop():
    """
    Каждую минуту:
    - если сейчас ровно слот — постим ОДИН самый старый (если есть)
    - если сейчас за PREVIEW_BEFORE_MIN до слота — шлём превью
    """
    posted_marks = set()
    preview_marks = set()
    while True:
        now = _now().replace(second=0, microsecond=0)
        # сброс маркеров в новый день
        if len(posted_marks) > 10 or len(preview_marks) > 10:
            posted_marks = {m for m in posted_marks if m.date() == now.date()}
            preview_marks = {m for m in preview_marks if m.date() == now.date()}

        for slot in _next_slots_today():
            # превью
            pv = slot - timedelta(minutes=PREVIEW_BEFORE_MIN)
            if now == pv and pv not in preview_marks:
                preview_marks.add(pv)
                if get_count() > 0:
                    await _send_preview_of_oldest()

            # сам пост
            if now == slot and slot not in posted_marks:
                posted_marks.add(slot)
                if get_count() > 0:
                    task = dequeue_oldest()
                    if task:
                        await _post_task(task)

        await asyncio.sleep(60)


# ----------------- HANDLERS -----------------
@dp.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer(help_text(), disable_web_page_preview=True)


@dp.message(Command("queue"))
async def cmd_queue(m: Message):
    cnt = get_count()
    lines = [f"В очереди: {cnt}"]
    if cnt:
        items = get_all(limit=min(cnt, 10))
        for it in items:
            lines.append(f"- id={it['id']} ({len([x for x in it['items'] if x.get('type')=='photo'])} фото)")
    await m.answer("\n".join(lines))


@dp.message(Command("post_oldest"))
async def cmd_post_oldest(m: Message):
    task = dequeue_oldest()
    if not task:
        await m.answer("Очередь пустая.")
        return
    ok = await _post_task(task)
    await m.answer("Готово." if ok else "Ошибка публикации, смотри логи.")


@dp.message(Command("test_preview"))
async def cmd_test_preview(m: Message):
    if get_count() == 0:
        await m.answer("В очереди пусто, превью нечего показывать.")
        return
    await _send_preview_of_oldest()
    await m.answer("Превью отправлено админам в ЛС.")


# Добавление: фото/альбом/текст — автоматически в очередь
@dp.message(F.media_group_id.as_("mgid") | F.photo | F.text)
async def inbox(m: Message, mgid: int | None = None):
    # только приватные чаты с админами
    if m.chat.type != "private" or (m.from_user and m.from_user.id not in ADMINS):
        return

    # 1) копим альбом по media_group_id
    if mgid:
        # накапливаем
        photos = ALBUM_BUFFER.get(mgid, [])
        # берём самый большой файл_id
        if m.photo:
            photos.append(m.photo[-1].file_id)
        ALBUM_BUFFER[mgid] = photos
        # ждём завершения группы — в aiogram 3 альбом приходит батчем,
        # но надёжнее подождать небольшой таймаут перед сборкой.
        await asyncio.sleep(1.0)
        # проверим, все ли части уже пришли — эвристика: если следующее сообщение не из этой группы, соберём.
        # Проще: если через 1 сек мы здесь — пытаемся собрать.
        items = [{"type": "photo", "file_id": fid} for fid in ALBUM_BUFFER.get(mgid, [])]
        if not items:
            return
        # подпись только из первого сообщения группы
        caption = (m.caption or m.text or "").strip()
        qid = enqueue(items=items, caption=caption, src=_src_tuple(m))
        # очищаем буфер
        ALBUM_BUFFER.pop(mgid, None)
        await m.answer(f"Добавил в очередь (id={qid}). Фото: {len(items)}")
        return

    # 2) одиночное фото
    if m.photo:
        items = [{"type": "photo", "file_id": m.photo[-1].file_id}]
        caption = (m.caption or "").strip()
        qid = enqueue(items=items, caption=caption, src=_src_tuple(m))
        await m.answer(f"Добавил одно фото в очередь (id={qid}).")
        return

    # 3) просто текст — тоже кладём (будет текстовый пост)
    if (m.text or "").strip():
        qid = enqueue(items=[], caption=m.text, src=_src_tuple(m))
        await m.answer(f"Добавил текстовый пост в очередь (id={qid}).")
        return


# ----------------- RUN -----------------
async def run_bot():
    init_db()
    # Параллельно стартуем планировщик
    asyncio.create_task(scheduler_loop())
    await dp.start_polling(bot)
