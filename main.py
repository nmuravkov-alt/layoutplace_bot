# main.py
import asyncio
import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
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

# -------------------- Конфиг из переменных окружения --------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()     # @username или -100...
ADMINS_RAW = os.getenv("ADMINS", "").strip()         # id через запятую
TZ = os.getenv("TZ", "Europe/Moscow")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан")
if not CHANNEL_ID:
    raise RuntimeError("CHANNEL_ID не задан")

# разберём админов (целые ID)
ADMINS: set[int] = set()
for piece in (ADMINS_RAW or "").replace(" ", "").split(","):
    if piece:
        try:
            ADMINS.add(int(piece))
        except ValueError:
            pass  # если случайно передали @username — игнорируем

# -------------------- Логирование --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("layoutplace_bot")

# -------------------- Вспомогалки --------------------
def now_str() -> str:
    tz = ZoneInfo(TZ)
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

def is_admin(m: Message) -> bool:
    uid = (m.from_user.id if m and m.from_user else None)
    return uid in ADMINS

async def send_to_channel(bot: Bot, text: str):
    await bot.send_message(
        chat_id=CHANNEL_ID,
        text=text,
        disable_web_page_preview=False,
    )

def _ad_fields(ad) -> tuple[int, str]:
    """Поддержка и tuple, и dict от storage.db.get_oldest()"""
    if ad is None:
        return (0, "")
    if isinstance(ad, dict):
        return int(ad.get("id", 0)), str(ad.get("text", ""))
    # ожидаем (id, text, ...)
    try:
        _id = int(ad[0])
        _text = str(ad[1])
        return _id, _text
    except Exception:
        return (0, "")

async def post_oldest_and_cleanup(bot: Bot, *, reply_to: Message | None = None):
    """Постим самое старое объявление в канал и чистим похожие."""
    ad = get_oldest()
    ad_id, ad_text = _ad_fields(ad)

    if not ad_id or not ad_text.strip():
        msg = "Очередь пуста — нечего постить."
        if reply_to:
            await reply_to.answer(msg)
        else:
            log.info(msg)
        return

    # публикуем в канал
    await send_to_channel(bot, ad_text)

    # собираем похожие (включая сам пост)
    similar_ids = set(find_similar_ids(ad_text) or [])
    similar_ids.add(ad_id)

    # удаляем скопом
    bulk_delete(list(similar_ids))

    done_msg = f"Опубликовано и удалено {len(similar_ids)} объявл."
    if reply_to:
        await reply_to.answer(done_msg)
    else:
        log.info(done_msg)

# -------------------- Запуск бота --------------------
async def main():
    # инициализация БД (синхронная)
    init_db()

    bot = Bot(
        BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher(storage=MemoryStorage())

    # -------------------- Команды --------------------
    @dp.message(Command("start"))
    async def cmd_start(m: Message):
        help_text = (
            "Готов к работе.\n\n"
            "<b>Команды</b>:\n"
            "/myid — показать твой Telegram ID\n"
            "/enqueue <текст> — положить объявление в очередь (только админ)\n"
            "/post_oldest — опубликовать самое старое и удалить похожие (только админ)\n"
            "/next — то же самое, что /post_oldest (только админ)\n"
            "/now — текущее время (TZ)\n"
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
        if not is_admin(m):
            return await m.answer("Нет прав.")

        text = (command.args or "").strip()
        if not text:
            return await m.answer("Формат: <code>/enqueue ТЕКСТ_ОБЪЯВЛЕНИЯ</code>")

        db_enqueue(text)
        await m.answer("Добавил в очередь ✅")

    @dp.message(Command("post_oldest"))
    async def cmd_post_oldest(m: Message):
        if not is_admin(m):
            return await m.answer("Нет прав.")
        await post_oldest_and_cleanup(bot, reply_to=m)

    @dp.message(Command("next"))
    async def cmd_next(m: Message):
        if not is_admin(m):
            return await m.answer("Нет прав.")
        await post_oldest_and_cleanup(bot, reply_to=m)

    # ---------------------------------------------------------------------
    log.info(f"Запущен бот @{(await bot.get_me()).username} для канала {CHANNEL_ID} (TZ {TZ})")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
