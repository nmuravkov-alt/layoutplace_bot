# main.py
import asyncio
import logging
import os
from datetime import datetime

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ChatType
from aiogram.filters import Command, CommandObject
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message

from storage.db import (
    init_db,
    enqueue as db_enqueue,
    get_oldest,
    delete_by_id,
    find_similar_ids,
    bulk_delete,
)

# ----------------- Конфиг из переменных окружения -----------------

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()      # @username или -100...
ADMINS_RAW = os.getenv("ADMINS", "").strip()          # id через запятую
TZ = os.getenv("TZ", "Europe/Moscow")

def _parse_admins(s: str) -> set[int]:
    ids = set()
    for part in s.replace(" ", "").split(","):
        if not part:
            continue
        try:
            ids.add(int(part))
        except ValueError:
            pass
    return ids

ADMINS: set[int] = _parse_admins(ADMINS_RAW)

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан")

# ----------------- Логирование -----------------

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("layoutplace_bot")

# ----------------- Вспомогалки -----------------

def is_admin(m: Message) -> bool:
    return m.from_user and m.from_user.id in ADMINS

def now_str() -> str:
    # просто человекочитаемое время (без зависимостей)
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

async def send_to_channel(bot: Bot, text: str) -> None:
    """
    Отправка в канал. CHANNEL_ID может быть @username либо numeric id.
    Отключаем превью ссылок.
    """
    await bot.send_message(
        chat_id=CHANNEL_ID,
        text=text,
        disable_web_page_preview=True,
    )

# ----------------- Бот / Диспетчер -----------------

bot = Bot(
    BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)
dp = Dispatcher(storage=MemoryStorage())

# ----------------- Команды -----------------

@dp.message(Command("start"))
async def cmd_start(m: Message):
    help_text = (
        "Готов к работе.\n\n"
        "<b>Команды</b>:\n"
        "/myid — показать твой Telegram ID\n"
        "/enqueue &lt;текст&gt; — положить объявление в очередь\n"
        "/queue — показать размер очереди\n"
        "/post_oldest — опубликовать самое старое и удалить похожие\n"
        "/now — текущее время сервера\n"
    )
    await m.answer(help_text)

@dp.message(Command("myid"))
async def cmd_myid(m: Message):
    await m.answer(f"Твой Telegram ID: <code>{m.from_user.id}</code>")

@dp.message(Command("now"))
async def cmd_now(m: Message):
    await m.answer(f"Серверное время: <b>{now_str()}</b>")

@dp.message(Command("enqueue"))
async def cmd_enqueue(m: Message, command: CommandObject):
    """
    /enqueue <текст объявления>
    """
    text = (command.args or "").strip()
    if not text:
        await m.answer("Пришли текст: <code>/enqueue текст объявления</code>")
        return

    ad_id = db_enqueue(text)
    await m.answer(f"Ок, добавил в очередь (#<code>{ad_id}</code>).")

@dp.message(Command("queue"))
async def cmd_queue(m: Message):
    # Покажем первые несколько и размер
    ad = get_oldest()
    if not ad:
        await m.answer("Очередь пустая.")
        return

    # посчитаем размер быстро (без отдельной функции)
    # маленькая БД — можно грубо пробежаться
    # но чтобы не тянуть все — дадим только тизер
    preview = ad["text"]
    if len(preview) > 400:
        preview = preview[:400] + "…"

    await m.answer(
        "Самое раннее в очереди:\n\n"
        f"{preview}\n\n"
        "<i>Чтобы опубликовать и удалить похожие — используй /post_oldest</i>"
    )

@dp.message(Command("post_oldest"))
async def cmd_post_oldest(m: Message):
    """
    Для админов. Берёт самое раннее объявление,
    публикует в канал и удаляет дубликаты + сам пост.
    """
    if not is_admin(m):
        await m.answer("Нет прав.")
        return

    ad = get_oldest()
    if not ad:
        await m.answer("Очередь пустая — публиковать нечего.")
        return

    ad_id = ad["id"]
    ad_text = ad["text"]

    # 1) Отправляем в канал
    try:
        await send_to_channel(bot, ad_text)
    except Exception as e:
        log.exception("Не смог отправить пост в канал")
        await m.answer(f"Ошибка при отправке в канал: <code>{e}</code>")
        return

    # 2) Ищем похожие и удаляем их вместе с опубликованным
    try:
        similar_ids = find_similar_ids(ad_text, threshold=0.88, exclude_id=ad_id)
        deleted_sim = bulk_delete(similar_ids)
        deleted_main = delete_by_id(ad_id)
        await m.answer(
            f"Опубликовано в канал.\n"
            f"Удалено похожих: <b>{deleted_sim}</b>.\n"
            f"Удалён сам пост из очереди: <b>{deleted_main}</b>."
        )
        log.info(
            "Published oldest #%s and removed %s similar",
            ad_id, deleted_sim
        )
    except Exception as e:
        log.exception("Ошибка при чистке очереди")
        await m.answer(f"Опубликовано, но при чистке очереди случилась ошибка: <code>{e}</code>")

# ------------- Фоллбек (чаты/группы) -------------

@dp.message(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def ignore_groups(m: Message):
    # Чтобы бот молчал в группах, если туда случайно добавят
    pass

# ----------------- Запуск -----------------

async def main():
    init_db()
    log.info(
        "layoutplace_bot | Запущен бот @%s для канала %s",
        (await bot.get_me()).username,
        CHANNEL_ID,
    )
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("Bot stopped")
