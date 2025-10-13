# main.py
import os
import asyncio
import logging
from datetime import datetime
from typing import List

import pytz
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.client.default import DefaultBotProperties  # <<< ВАЖНО: правильный default

# ── Конфиг из окружения ───────────────────────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()     # @channel_name или -100...
ADMINS_RAW = os.getenv("ADMINS", "").strip()         # "123,456"
TZ = os.getenv("TZ", "Europe/Moscow").strip()

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан.")
if not CHANNEL_ID:
    raise RuntimeError("CHANNEL_ID не задан.")

def parse_admins(s: str) -> List[int]:
    out: List[int] = []
    for p in s.replace(" ", "").split(","):
        if not p:
            continue
        try:
            out.append(int(p))
        except ValueError:
            pass
    return out

ADMINS = parse_admins(ADMINS_RAW)
TZINFO = pytz.timezone(TZ)

# ── Логирование ───────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("layoutplace_bot")

# ── Утилиты ───────────────────────────────────────────────────────────────────
def is_admin(user_id: int) -> bool:
    # если ADMINS не задан — разрешим всем (удобно на старте)
    return user_id in ADMINS if ADMINS else True

async def send_to_channel(bot: Bot, text: str) -> None:
    await bot.send_message(
        chat_id=CHANNEL_ID,
        text=text,
        disable_web_page_preview=True,
    )

# ── Хендлеры ──────────────────────────────────────────────────────────────────
async def on_start(message: Message):
    # без разметки, чтобы <теги> в подсказке не ломали ответ
    txt = (
        "Готов к работе.\n"
        "/myid — показать твой Telegram ID\n"
        "/post <текст> — опубликовать сразу в канал\n"
        "/now — текущее время (TZ)\n"
        f"\nКанал: {CHANNEL_ID}  •  TZ: {TZ}"
    )
    await message.answer(txt, parse_mode=None)

async def on_myid(message: Message):
    await message.answer(f"Твой Telegram ID: <code>{message.from_user.id}</code>")

async def on_now(message: Message):
    now = datetime.now(TZINFO).strftime("%Y-%m-%d %H:%M:%S")
    await message.answer(f"Серверное время: <b>{now}</b>  <i>({TZ})</i>")

async def on_post(message: Message, bot: Bot):
    if not is_admin(message.from_user.id):
        return await message.answer("Нет прав.")
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        return await message.answer("Использование: <code>/post Текст поста</code>")
    await send_to_channel(bot, parts[1].strip())
    await message.answer("✅ Отправлено в канал.")

# ── Запуск ────────────────────────────────────────────────────────────────────
async def main():
    bot = Bot(
        BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)  # <<< фикс
    )
    dp = Dispatcher()

    dp.message.register(on_start, Command("start"))
    dp.message.register(on_myid, Command("myid"))
    dp.message.register(on_now, Command("now"))
    dp.message.register(on_post, Command("post"))

    me = await bot.get_me()
    logger.info(f"Запущен бот @{me.username} для канала {CHANNEL_ID} (TZ {TZ})")

    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Остановлен.")
