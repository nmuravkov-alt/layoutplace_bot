# main.py
import os
import asyncio
import logging
from typing import Optional, Tuple, List

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, InputMediaPhoto, InputMediaVideo
from aiogram.enums import ParseMode
from aiogram.filters import Command

# --- конфиг: берем из config.py (если есть) или из ENV ---
try:
    from config import (
        TOKEN,
        CHANNEL_ID,
        ADMINS,
        TZ,
        POST_TIMES,
        PREVIEW_MINUTES,
    )
except Exception:
    TOKEN = os.getenv("BOT_TOKEN", "")
    CHANNEL_ID = int(os.getenv("CHANNEL_ID", "-1000000000000"))
    # перечисли через запятую
    ADMINS = [int(x) for x in os.getenv("ADMINS", "").replace(" ", "").split(",") if x]
    TZ = os.getenv("TZ", "Europe/Moscow")
    POST_TIMES = os.getenv("POST_TIMES", "12:00,16:00,20:00")
    PREVIEW_MINUTES = int(os.getenv("PREVIEW_MINUTES", "45"))

# --- БД ---
from storage.db import (
    init_db,
    enqueue,
    dequeue_oldest,
    get_count,
    list_queue,
    wipe_queue,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("layoutplace_bot")

bot = Bot(TOKEN)
dp = Dispatcher()

# ========= УТИЛЫ ===========

def _is_admin(user_id: int) -> bool:
    return user_id in ADMINS

def normalize_caption(text: Optional[str]) -> str:
    """
    Приводим описание к единому стилю без эмодзи и добавляем хвост.
    Ничего лишнего не трогаем, просто чистим и доклеиваем.
    """
    if not text:
        text = ""
    text = text.strip()

    # Хвост без эмодзи, как договаривались
    tail = (
        "\n\n"
        "Доставка по всему миру\n"
        "Общий альбом: https://vk.com/market-222108341?screen=group&section=album_26\n"
        "Покупка/вопросы: @layoutplacebuy"
    )

    # Если хвоста нет — добавим
    if "vk.com/market-222108341" not in text and "@layoutplacebuy" not in text:
        text = f"{text}{tail}"
    return text

def _src_tuple(msg: Message) -> Optional[Tuple[int, int]]:
    """
    ВАЖНО: фикc под Aiogram v3.
    Раньше m.forward_from_chat.type был enum, теперь это строка.
    Возвращаем (chat_id, message_id) исходного поста (канал или пересланный из канала).
    Работает ТОЛЬКО если команда даётся в ответ на сообщение (reply).
    """
    m = msg.reply_to_message
    if not m:
        return None

    # Переслано из канала
    if m.forward_from_chat and getattr(m.forward_from_chat, "type", "") == "channel":
        return (m.forward_from_chat.id, m.forward_from_message_id)

    # Сообщение из канала (если команда выполняется прямо в канале)
    if m.chat and getattr(m.chat, "type", "") == "channel":
        return (m.chat.id, m.message_id)

    return None

async def _send_help(m: Message):
    help_text = (
        "Привет! Я помогу с очередью постов для канала.\n\n"
        "Команды (нужно писать в ЛС боту или в админском чате, где бот есть):\n"
        "/start — показать это сообщение\n"
        "/queue — показать размер очереди\n"
        "/list — показать первые 10 элементов очереди\n"
        "/wipe — очистить очередь (только админы)\n\n"
        "Добавление в очередь (два способа):\n"
        "1) Ответ на пересланное из канала сообщение: напиши /add_post в ответ на пересланный пост (фото/альбом/видео/текст). Бот добавит исходник в очередь и потом опубликует с копированием медиа.\n"
        "2) Ручной текст: отправь боту описание поста и затем команду /enqueue в ответ — он положит текст в очередь как текстовый пост.\n\n"
        "Постинг по расписанию: 12:00, 16:00, 20:00 по Europe/Moscow. За 45 минут до поста бот шлёт превью в ЛС админам. "
        "Чтобы бот мог писать тебе в ЛС, сначала нажми /start боту в личку.\n\n"
        "Важно: при репосте из канала бот пытается удалить старое сообщение в источнике. Для этого у бота должны быть права администратора в исходном канале с разрешением на удаление сообщений."
    )
    # ВАЖНО: никаких угловых скобок — иначе Telegram подумает, что это HTML-тег
    await m.answer(help_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

# ========= ХЕНДЛЕРЫ ===========

@dp.message(Command("start"))
async def cmd_start(m: Message):
    await _send_help(m)

@dp.message(Command("help"))
async def cmd_help(m: Message):
    await _send_help(m)

@dp.message(Command("queue"))
async def cmd_queue(m: Message):
    if not _is_admin(m.from_user.id):
        return
    try:
        count = get_count()
        await m.answer(f"В очереди: {count}.")
    except Exception as e:
        await m.answer(f"Ошибка: {e}")

@dp.message(Command("list"))
async def cmd_list(m: Message):
    if not _is_admin(m.from_user.id):
        return
    try:
        items = list_queue(limit=10)
        if not items:
            await m.answer("Очередь пустая.")
            return
        lines = []
        for it in items:
            # Пытаемся красиво показать кратко
            qid = it.get("id", "?")
            src = it.get("src")
            cap = (it.get("caption") or "").strip()
            cap_short = (cap[:70] + "…") if len(cap) > 70 else cap
            if src and isinstance(src, (list, tuple)) and len(src) == 2:
                src_text = f"{src[0]}/{src[1]}"
            else:
                src_text = "—"
            lines.append(f"#{qid} | src: {src_text} | {cap_short}")
        await m.answer("Первые 10:\n" + "\n".join(lines))
    except Exception as e:
        await m.answer(f"Ошибка: {e}")

@dp.message(Command("wipe"))
async def cmd_wipe(m: Message):
    if not _is_admin(m.from_user.id):
        return
    try:
        wipe_queue()
        await m.answer("Очередь очищена.")
    except Exception as e:
        await m.answer(f"Ошибка: {e}")

@dp.message(Command("enqueue"))
async def cmd_enqueue(m: Message):
    """
    Ручная постановка текста в очередь.
    Делается ответом на СВОЁ текстовое сообщение (без медиа).
    """
    if not _is_admin(m.from_user.id):
        return
    if not m.reply_to_message or not (m.reply_to_message.text or m.reply_to_message.caption):
        await m.answer("Использование: пришли текст поста, а потом в ответ на него команду /enqueue.")
        return

    raw_text = m.reply_to_message.text or m.reply_to_message.caption or ""
    caption = normalize_caption(raw_text)
    try:
        # кладём как текстовый пост (без src), items = []
        qid = enqueue(items=[], caption=caption, src=None)
        await m.answer(f"Добавил текстовый пост в очередь, id={qid}.")
    except Exception as e:
        await m.answer(f"Ошибка: {e}")

@dp.message(Command("add_post"))
async def cmd_add_post(m: Message):
    """
    Главная команда: ставим в очередь исходный пост из канала.
    Делаем это в ответ на пересланное сообщение из канала (или на сообщение из самого канала).
    """
    if not _is_admin(m.from_user.id):
        return

    src = _src_tuple(m)
    if not src:
        await m.answer(
            "Сделай /add_post ответом на сообщение из канала (или пересланное из канала). "
            "Так я смогу скопировать медиа и удалить старый пост после перепубликации."
        )
        return

    # Попробуем взять подпись из реплая (если есть) и нормализовать
    base = m.reply_to_message
    raw_caption = (base.caption or base.text or "").strip()
    caption = normalize_caption(raw_caption)

    try:
        qid = enqueue(items=None, caption=caption, src=src)  # src-режим
        await m.answer(f"Добавил в очередь исходный пост {src[0]}/{src[1]}, id={qid}.")
    except Exception as e:
        await m.answer(f"Ошибка: {e}")

@dp.message(Command("post_oldest"))
async def cmd_post_oldest(m: Message):
    """
    Ручной постинг самого старого элемента (для теста).
    Реальный постинг по времени делает планировщик (scheduler), этот хендлер просто вызывает ту же логику через БД.
    """
    if not _is_admin(m.from_user.id):
        return
    try:
        task = dequeue_oldest()
        if not task:
            await m.answer("Очередь пустая.")
            return

        # task должен содержать либо src (перепост из канала), либо items/caption (собранный медиа-пост)
        caption = normalize_caption(task.get("caption"))

        if task.get("src"):
            src_chat_id, src_msg_id = task["src"]
            # копируем исходное сообщение в наш канал (без ссылки на автора)
            res = await bot.copy_message(
                chat_id=CHANNEL_ID,
                from_chat_id=src_chat_id,
                message_id=src_msg_id,
                caption=caption or None,
                parse_mode=ParseMode.HTML,
                disable_notification=False
            )
            # Пытаемся удалить старое сообщение (нужно право админа в исходном канале)
            try:
                await bot.delete_message(chat_id=src_chat_id, message_id=src_msg_id)
            except Exception as del_err:
                logging.warning(f"Не смог удалить старое сообщение {src_chat_id}/{src_msg_id}: {del_err}")
            await m.answer(f"Опубликовано в канал, новое id={res.message_id}.")
            return

        # вариант: в БД лежит собранный список медиа (items)
        items: List[dict] = task.get("items") or []
        if items:
            # Если альбом
            if len(items) > 1:
                media = []
                for i, it in enumerate(items):
                    t = it.get("type")
                    file_id = it.get("file_id")
                    if not file_id:
                        continue
                    if t == "photo":
                        media.append(InputMediaPhoto(media=file_id, caption=caption if i == 0 else None, parse_mode=ParseMode.HTML))
                    elif t == "video":
                        media.append(InputMediaVideo(media=file_id, caption=caption if i == 0 else None, parse_mode=ParseMode.HTML))
                if media:
                    await bot.send_media_group(chat_id=CHANNEL_ID, media=media)
                    await m.answer("Опубликован альбом.")
                    return
            else:
                # одиночное медиа
                it = items[0]
                t = it.get("type")
                fid = it.get("file_id")
                if t == "photo":
                    await bot.send_photo(chat_id=CHANNEL_ID, photo=fid, caption=caption, parse_mode=ParseMode.HTML)
                elif t == "video":
                    await bot.send_video(chat_id=CHANNEL_ID, video=fid, caption=caption, parse_mode=ParseMode.HTML)
                else:
                    await bot.send_message(chat_id=CHANNEL_ID, text=caption, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                await m.answer("Опубликовано.")
                return

        # fallback — просто текст
        await bot.send_message(chat_id=CHANNEL_ID, text=caption, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        await m.answer("Опубликовано (текст).")

    except Exception as e:
        await m.answer(f"Ошибка: {e}")

# ========= ЗАПУСК =========

async def _run():
    init_db()
    log.info("Starting bot instance...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(_run())
