# main.py
import asyncio
import logging
from typing import List, Dict, Any, Optional

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message

from config import BOT_TOKEN, CHANNEL_ID, TZ, ADMINS
from storage.db import init_db, enqueue, get_count, cache_album_upsert, cache_album_get, cache_album_clear
from utils.text import build_caption
from scheduler import run_scheduler

logging.basicConfig(level=logging.INFO)
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# ---- Утилиты ----
def _is_admin(user_id: int) -> bool:
    return user_id in ADMINS

def _extract_media_item(msg: Message) -> Optional[Dict[str, Any]]:
    """
    Возвращает {"type":..., "file_id":...} для одиночного сообщения
    """
    if msg.photo:
        return {"type": "photo", "file_id": msg.photo[-1].file_id}
    if msg.video:
        return {"type": "video", "file_id": msg.video.file_id}
    if msg.document:
        return {"type": "document", "file_id": msg.document.file_id}
    return None

# ---- Кеш альбомов: слушаем любые медиа с media_group_id в ЛС от админов ----
@dp.message(F.media_group_id)
async def cache_album_handler(m: Message):
    if not _is_admin(m.from_user.id) or m.chat.type != "private":
        return
    it = _extract_media_item(m)
    if not it:
        return
    # собираем всю группу по мере прихода — простая стратегия: читаем/обновляем
    current = cache_album_get(m.media_group_id) or []
    # избегаем дублей по file_id
    if all(x["file_id"] != it["file_id"] for x in current):
        current.append(it)
        cache_album_upsert(m.media_group_id, current)

# ---- Команды ----
@dp.message(Command("start"))
async def cmd_start(m: Message):
    text = (
        "Бот готов к работе.\n\n"
        "Команды:\n"
        "/myid — показать твой Telegram ID\n"
        "/add_post — сделать ответом на пересланное из канала сообщение (фото/альбом)\n"
        "/queue — показать размер очереди\n"
        "/post_oldest — опубликовать старый пост вручную\n"
        "/clear_albums_cache — очистить буфер альбомов\n"
        "/test_preview — тестовое превью админам\n"
        "/now — текущее время\n\n"
        "Подсказка: для /add_post сделай её ответом (reply) на пересланное сообщение из канала."
    )
    await m.answer(text, disable_web_page_preview=True)

@dp.message(Command("myid"))
async def cmd_myid(m: Message):
    await m.answer(f"Твой ID: <code>{m.from_user.id}</code>")

@dp.message(Command("clear_albums_cache"))
async def cmd_clear_cache(m: Message):
    if not _is_admin(m.from_user.id):
        return
    cache_album_clear()
    await m.answer("Кеш альбомов очищен.")

@dp.message(Command("queue"))
async def cmd_queue(m: Message):
    await m.answer(f"В очереди: {get_count()}.")

@dp.message(Command("test_preview"))
async def cmd_test_preview(m: Message):
    if not _is_admin(m.from_user.id):
        return
    from datetime import datetime
    from zoneinfo import ZoneInfo
    now = datetime.now(ZoneInfo(TZ)).strftime("%Y-%m-%d %H:%M:%S")
    for aid in ADMINS:
        try:
            await bot.send_message(aid, f"Тестовое превью\nПост был бы тут за 45 минут до публикации.\n{now}")
        except Exception as e:
            logging.warning(f"Админ {aid} недоступен: {e}")
    await m.answer("Ок, превью отправлено админам (если они нажали /start боту).")

@dp.message(Command("now"))
async def cmd_now(m: Message):
    from datetime import datetime
    from zoneinfo import ZoneInfo
    tz = ZoneInfo(TZ)
    await m.answer(datetime.now(tz).strftime(f"Серверное время: %Y-%m-%d %H:%M:%S ({TZ})"))

# --- add_post: только ответом на пересланное из канала сообщение ---
@dp.message(Command("add_post"))
async def cmd_add_post(m: Message):
    if not _is_admin(m.from_user.id):
        return
    if not m.reply_to_message:
        await m.answer("Сделай /add_post ответом на пересланное из канала сообщение (фото/альбом).")
        return

    src_msg = m.reply_to_message

    # Исходник (для удаления): работает, если сообщение реально переслано из канала
    src_chat_id = getattr(src_msg.forward_from_chat, "id", None)
    src_msg_id  = getattr(src_msg, "forward_from_message_id", None)
    src_tuple   = (src_chat_id, src_msg_id) if (src_chat_id and src_msg_id) else None

    items: List[Dict[str, Any]] = []

    if src_msg.media_group_id:
        # альбом — достанем из кеша по media_group_id
        cached = cache_album_get(src_msg.media_group_id)
        if not cached:
            await m.answer("Не нашёл фото этой группы в кеше. Перешли альбом ещё раз и повтори /add_post.")
            return
        items = cached
        caption = src_msg.caption or ""
    else:
        it = _extract_media_item(src_msg)
        if not it:
            await m.answer("Это не фото/видео/документ. Перешли медиа из канала и сделай /add_post ответом.")
            return
        items = [it]
        caption = src_msg.caption or ""

    qid = enqueue(items=items, caption=caption, src=src_tuple)
    await m.answer(f"Медиа добавлено в очередь (id={qid}). В очереди: {get_count()}.")

@dp.message(Command("post_oldest"))
async def cmd_post_oldest(m: Message):
    if not _is_admin(m.from_user.id):
        return
    from storage.db import dequeue_oldest
    task = dequeue_oldest()
    if not task:
        await m.answer("Очередь пуста.")
        return

    # локальный постинг (используем ту же функцию, что и планировщик)
    from scheduler import _publish  # type: ignore
    await _publish(bot, task)
    await m.answer(f"Опубликовано. Осталось в очереди: {get_count()}.")


# ====== Запуск ======
async def _run():
    init_db()
    # фоновый планировщик
    asyncio.create_task(run_scheduler(bot))
    # старт бота
    await dp.start_polling(bot)

if __name__ == "__main__":
    logging.info("Starting bot instance...")
    asyncio.run(_run())
