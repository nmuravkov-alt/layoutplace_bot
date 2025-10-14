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

# ---------------- Конфиг из переменных окружения ----------------

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()
ADMINS_RAW = os.getenv("ADMINS", "").strip()
TZ = os.getenv("TZ", "Europe/Moscow")

# преобразуем список админов в int
ADMINS = []
for a in ADMINS_RAW.split(","):
    a = a.strip()
    if not a:
        continue
    try:
        ADMINS.append(int(a))
    except ValueError:
        pass

# ---------------- Настройка логгирования ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("layoutplace_bot")

# ---------------- Инициализация бота ----------------
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())

# ---------------- Команды ----------------

@dp.message(Command("start"))
async def cmd_start(m: Message):
    help_text = (
        "Готов к работе.\n\n"
        "<b>Команды:</b>\n"
        "/myid – показать твой Telegram ID\n"
        "/enqueue <текст> – положить объявление в очередь\n"
        "/queue – показать размер очереди\n"
        "/post_oldest – опубликовать самое старое и удалить похожие\n"
        "/now – текущее время сервера"
    )
    await m.answer(help_text)


@dp.message(Command("myid"))
async def cmd_myid(m: Message):
    await m.answer(f"Твой Telegram ID: <code>{m.from_user.id}</code>")


@dp.message(Command("now"))
async def cmd_now(m: Message):
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    await m.answer(f"<b>Серверное время:</b> {now_str}")


@dp.message(Command("enqueue"))
async def cmd_enqueue(m: Message, command: CommandObject):
    text = command.args
    if not text:
        await m.answer("❗ Использование: /enqueue <текст объявления>")
        return

    ad_id = db_enqueue(text)
    await m.answer(f"✅ Объявление добавлено в очередь (ID: <code>{ad_id}</code>)")


@dp.message(Command("queue"))
async def cmd_queue(m: Message):
    oldest = get_oldest()
    if not oldest:
        await m.answer("Очередь пуста.")
        return

    await m.answer("В очереди есть объявления, можно постить.")


@dp.message(Command("post_oldest"))
async def cmd_post_oldest(m: Message):
    oldest = get_oldest()
    if not oldest:
        await m.answer("Очередь пуста.")
        return

    ad_id, text = oldest

    # Публикуем в канал
    await bot.send_message(CHANNEL_ID, text)

    # Удаляем похожие + сам пост
    similar = find_similar_ids(ad_id, threshold=0.88)
    removed = bulk_delete([ad_id] + similar)

    await m.answer(
        f"✅ Опубликовано объявление <code>{ad_id}</code> в канал.\n"
        f"🗑 Удалено из очереди объявлений: <b>{removed}</b> (включая похожие)."
    )


# ---------------- Точка входа ----------------

async def main():
    await init_db()
    log.info("✅ Бот запущен для @%s (TZ=%s)", CHANNEL_ID.strip("@"), TZ)
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
