# main.py
import asyncio
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any

from aiogram import Bot, Dispatcher, F, types
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command

from config import (
    TOKEN as BOT_TOKEN,
    CHANNEL_ID as _CHANNEL_ID,
    TZ as _TZ,
    ADMINS,
    ALBUM_URL,
    CONTACT_TEXT,
)
from storage.db import (
    init_db,
    db_enqueue,
    get_count,
    get_oldest,
    pop_oldest,
    clear_queue,
)

# -----------------------------------------------------------------------------
# ЛОГИ
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
log = logging.getLogger("layoutplace_bot")

# -----------------------------------------------------------------------------
# БОТ/ДИСПЕТЧЕР
# -----------------------------------------------------------------------------
props = DefaultBotProperties(parse_mode=ParseMode.HTML)
bot = Bot(BOT_TOKEN, default=props)
dp = Dispatcher()

CHANNEL_ID = _CHANNEL_ID
TZ = _TZ

# -----------------------------------------------------------------------------
# УТИЛЫ: единый текст, очистка эмодзи, дата/время
# -----------------------------------------------------------------------------
import re
_EMOJI_RE = re.compile(
    "["                     # простое «вырежи эмодзи»
    "\U0001F600-\U0001F64F"
    "\U0001F300-\U0001F5FF"
    "\U0001F680-\U0001F6FF"
    "\U0001F1E0-\U0001F1FF"
    "\U00002700-\U000027BF"
    "\U0001F900-\U0001F9FF"
    "\U00002600-\U000026FF"
    "\U0001FA70-\U0001FAFF"
    "]+", flags=re.UNICODE
)

def strip_emojis(text: str) -> str:
    return _EMOJI_RE.sub("", text)

def normalize_caption(raw: str) -> str:
    """Единый стиль без эмодзи и с 2 финальными строками."""
    text = strip_emojis(raw or "").strip()

    # убираем лишние пустые строки
    text = re.sub(r"\n{3,}", "\n\n", text)

    footer = (
        f"\n\n"
        f"Общий альбом: {ALBUM_URL}\n"
        f"Покупка/вопросы: {CONTACT_TEXT}"
    )
    # если вдруг уже есть такие строки — не дублируем
    if "Общий альбом:" in text:
        text = re.sub(r"Общий альбом:.*", f"Общий альбом: {ALBUM_URL}", text)
    else:
        text += footer

    return text.strip()

def now_local() -> datetime:
    return datetime.now(ZoneInfo(TZ))

# -----------------------------------------------------------------------------
# НАДЕЖНОЕ УДАЛЕНИЕ ИСХОДНИКА (пересланный пост/альбом)
# -----------------------------------------------------------------------------
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.types import Message, ChatMemberAdministrator, ChatMemberOwner

async def _extract_source_from_forward(msg: Message) -> tuple[Optional[int], Optional[int]]:
    origin = getattr(msg, "forward_origin", None)
    try:
        if origin and getattr(origin, "type", None) == "channel":
            return origin.chat.id, origin.message_id
    except Exception:
        pass

    if getattr(msg, "forward_from_chat", None) and getattr(msg, "forward_from_message_id", None):
        return msg.forward_from_chat.id, msg.forward_from_message_id

    return None, None

async def _bot_is_admin_in(chat_id: int) -> bool:
    try:
        me = await bot.me()
        member = await bot.get_chat_member(chat_id, me.id)
        return isinstance(member, (ChatMemberAdministrator, ChatMemberOwner))
    except TelegramForbiddenError:
        return False
    except Exception:
        return False

async def _delete_range(chat_id: int, center_message_id: int, radius: int = 9) -> bool:
    deleted_any = False
    start_id = max(1, center_message_id - radius)
    end_id = center_message_id + radius
    for mid in range(start_id, end_id + 1):
        try:
            await bot.delete_message(chat_id, mid)
            deleted_any = True
        except TelegramBadRequest as e:
            s = str(e)
            if "message can't be deleted" in s or "message to delete not found" in s:
                continue
        except TelegramForbiddenError:
            return deleted_any
        except Exception as e:
            log.warning(f"delete_range err {chat_id}/{mid}: {e}")
    return deleted_any

async def delete_source_message(forwarded_message: Message) -> bool:
    src_chat_id, src_msg_id = await _extract_source_from_forward(forwarded_message)
    if not src_chat_id or not src_msg_id:
        log.warning("delete_source_message: нет данных об источнике (возможно, сообщение «скопировано», а не «переслано»)")
        return False
    if not await _bot_is_admin_in(src_chat_id):
        log.warning(f"delete_source_message: бот не админ в источнике {src_chat_id}")
        return False
    ok = await _delete_range(src_chat_id, src_msg_id)
    if not ok:
        log.warning(f"delete_source_message: не удалось удалить {src_chat_id}/{src_msg_id}")
    return ok

async def delete_source_by_ids(src_chat_id: Optional[int], src_msg_id: Optional[int]) -> bool:
    if not src_chat_id or not src_msg_id:
        return False
    if not await _bot_is_admin_in(src_chat_id):
        log.warning(f"delete_source_by_ids: бот не админ в {src_chat_id}")
        return False
    return await _delete_range(src_chat_id, src_msg_id)

# -----------------------------------------------------------------------------
# ПУБЛИКАЦИЯ В КАНАЛ
# -----------------------------------------------------------------------------
from aiogram.types import InputMediaPhoto, InputMediaVideo

async def publish_entry(entry: Dict[str, Any]) -> int:
    """
    Публикует запись из очереди в канал и возвращает message_id первого отправленного сообщения.
    entry:
      type: "single"|"album"
      caption: str
      photo_file_id / video_file_id (для простого случая)
      src_chat_id, src_msg_id
    """
    caption = normalize_caption(entry.get("caption", "") or "")
    first_mid = 0

    if entry.get("type") == "single":
        if entry.get("photo_file_id"):
            sent = await bot.send_photo(CHANNEL_ID, entry["photo_file_id"], caption=caption, disable_web_page_preview=True)
            first_mid = sent.message_id
        elif entry.get("video_file_id"):
            sent = await bot.send_video(CHANNEL_ID, entry["video_file_id"], caption=caption, disable_web_page_preview=True)
            first_mid = sent.message_id
        else:
            sent = await bot.send_message(CHANNEL_ID, caption, disable_web_page_preview=True)
            first_mid = sent.message_id
    else:
        # Простейший альбом (если заранее сохранил хотя бы 1 кадр)
        medias = []
        if entry.get("photo_file_id"):
            medias.append(InputMediaPhoto(media=entry["photo_file_id"], caption=caption))
        if entry.get("video_file_id"):
            medias.append(InputMediaVideo(media=entry["video_file_id"], caption=caption))

        if medias:
            sent_list = await bot.send_media_group(CHANNEL_ID, medias)
            first_mid = sent_list[0].message_id
        else:
            sent = await bot.send_message(CHANNEL_ID, caption, disable_web_page_preview=True)
            first_mid = sent.message_id

    return first_mid

# -----------------------------------------------------------------------------
# ХЕЛП / ДОПУСК
# -----------------------------------------------------------------------------
def _is_admin(user_id: int) -> bool:
    return user_id in ADMINS

HELP = (
    "Бот готов к работе.\n\n"
    "<b>Команды:</b>\n"
    "/myid — показать твой Telegram ID\n"
    "/add_post — сделать ответом на пересланное из канала сообщение (фото/альбом)\n"
    "/queue — показать размер очереди\n"
    "/post_oldest — опубликовать старый пост вручную\n"
    "/clear_queue — очистить очередь\n"
    "/test_preview — тестовое превью админам\n"
    "/now — текущее время"
)

@dp.message(Command("start"))
async def cmd_start(m: types.Message):
    if not _is_admin(m.from_user.id):
        await m.answer("Доступ только для админов.")
        return
    # уведомление о запуске присылает runner, тут просто help
    await m.answer(HELP, disable_web_page_preview=True)

@dp.message(Command("myid"))
async def cmd_myid(m: types.Message):
    await m.answer(f"Твой ID: <code>{m.from_user.id}</code>")

@dp.message(Command("now"))
async def cmd_now(m: types.Message):
    await m.answer(f"Серверное время: <code>{now_local().strftime('%Y-%m-%d %H:%M:%S')}</code> ({TZ})")

# -----------------------------------------------------------------------------
# ДОБАВЛЕНИЕ В ОЧЕРЕДЬ
# -----------------------------------------------------------------------------
@dp.message(Command("add_post"))
async def cmd_add_post(m: types.Message):
    if not _is_admin(m.from_user.id):
        await m.answer("Доступ только для админов.")
        return
    if not m.reply_to_message:
        await m.answer("Сделай /add_post ответом на пересланное из канала сообщение (фото/альбом).")
        return

    fwd = m.reply_to_message
    src_chat_id, src_msg_id = await _extract_source_from_forward(fwd)
    if not src_chat_id or not src_msg_id:
        await m.answer("Это не похоже на «пересланное из канала». Перешли именно из канала (не «скопировать»).")
        return

    entry: Dict[str, Any] = {
        "type": "album" if fwd.media_group_id else "single",
        "media_group_id": fwd.media_group_id,
        "caption": fwd.caption or fwd.text or "",
        "src_chat_id": src_chat_id,
        "src_msg_id": src_msg_id,
    }

    if getattr(fwd, "photo", None):
        entry["photo_file_id"] = fwd.photo[-1].file_id
    if getattr(fwd, "video", None):
        entry["video_file_id"] = fwd.video.file_id

    post_id = db_enqueue(entry)
    await m.answer(f"Медиа добавлено в очередь (id={post_id}). В очереди: {get_count()}.")

# -----------------------------------------------------------------------------
# ОЧЕРЕДЬ / ПУБЛИКАЦИЯ
# -----------------------------------------------------------------------------
@dp.message(Command("queue"))
async def cmd_queue(m: types.Message):
    if not _is_admin(m.from_user.id):
        await m.answer("Доступ только для админов.")
        return
    await m.answer(f"В очереди: {get_count()}.")

@dp.message(Command("clear_queue"))
async def cmd_clear(m: types.Message):
    if not _is_admin(m.from_user.id):
        await m.answer("Доступ только для админов.")
        return
    n = clear_queue()
    await m.answer(f"Очищено: {n}. Теперь в очереди: {get_count()}.")

@dp.message(Command("post_oldest"))
async def cmd_post_oldest(m: types.Message):
    if not _is_admin(m.from_user.id):
        await m.answer("Доступ только для админов.")
        return
    entry = get_oldest()
    if not entry:
        await m.answer("Очередь пуста.")
        return

    # Можно вынимать после успешной отправки — чтобы не потерять на ошибке
    try:
        await publish_entry(entry)
    except Exception as e:
        log.exception(f"Ошибка публикации: {e}")
        await m.answer("Не удалось опубликовать (смотри логи).")
        return

    # удаляем исходник
    try:
        ok = await delete_source_by_ids(entry.get("src_chat_id"), entry.get("src_msg_id"))
        if not ok:
            log.warning("Исходник не удалён (проверь права админа у бота в канале-источнике).")
    except Exception as e:
        log.warning(f"Ошибка удаления исходника: {e}")

    # вынимаем из очереди после успеха
    _ = pop_oldest()
    await m.answer(f"Опубликовано. Осталось в очереди: {get_count()}.")

# -----------------------------------------------------------------------------
# ПРЕВЬЮ
# -----------------------------------------------------------------------------
async def notify_admins(text: str):
    for admin_id in ADMINS:
        try:
            await bot.send_message(admin_id, text, disable_web_page_preview=True)
        except Exception as e:
            log.warning(f"Админ {admin_id} недоступен: {e}")

@dp.message(Command("test_preview"))
async def cmd_test_preview(m: types.Message):
    if not _is_admin(m.from_user.id):
        await m.answer("Доступ только для админов.")
        return
    await notify_admins("Тестовое превью\nПост был бы тут за 45 минут до публикации.")

# -----------------------------------------------------------------------------
# СТАРТ
# -----------------------------------------------------------------------------
async def _run():
    init_db()
    await notify_admins(f"🚀 Бот запускается (канал {_CHANNEL_ID}, TZ={TZ}). Если таких уведомлений два — запущены два инстанса.")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(_run())
