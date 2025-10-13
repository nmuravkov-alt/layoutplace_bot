# main.py
import os
import asyncio
import logging
from datetime import datetime
from typing import List

import pytz
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message

# ── Конфиг из переменных окружения ────────────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()  # @channel_name или -1001234567890
ADMINS_RAW = os.getenv("ADMINS", "").strip()      # список id через запятую
TZ = os.getenv("TZ", "Europe/Moscow").strip()

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан (env).")
if not CHANNEL_ID:
    raise RuntimeError("CHANNEL_ID не задан (env).")

# распарсим админов
def _parse_admins(s: str) -> List[int]:
    out = []
    for part in s.replace(" ", "").split(","):
        if not part:
            continue
        try:
            out.append(int(part))
        except ValueError:
            # пропускаем то, что не int
            continue
    return out

ADMINS: List[int] = _parse_admins(ADMINS_RAW)
TZINFO = pytz.timezone(TZ)

# ── Логирование ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("layoutplace_bot")


# ── Хелперы ───────────────────────────────────────────────────────────────────
def is_admin(user_id: int) -> bool:
    return user_id in ADMINS if ADMINS else True  # если список пуст — все админы

async def send_to_channel(bot: Bot, text: str) -> None:
    await bot.send_message(
        chat_id=CHANNEL_ID,
        text=text,
        disable_web_page_preview=True,   # чтобы ссылки не раздували пост
    )


# ── Хендлеры ──────────────────────────────────────────────────────────────────
async def on_start(message: Message, bot: Bot):
    # ВКЛЮЧАЕМ БЕЗ РАЗМЕТКИ: parse_mode=None → иначе <текст> воспримется как тег
    text = (
        "Готов к работе.\n"
        "/myid — показать твой Telegram ID\n"
        "/post <текст> — опубликовать сразу в канал\n"
        "/now — текущее время (TZ)\n"
        "\n"
        f"Канал: {CHANNEL_ID}  •  TZ: {TZ}"
    )
    await message.answer(text, parse_mode=None)

async def on_myid(message: Message):
    await message.answer(f"Твой Telegram ID: <code>{message.from_user.id}</code>")

async def on_now(message: Message):
    now = datetime.now(TZINFO).strftime("%Y-%m-%d %H:%M:%S")
    await message.answer(f"Серверное время: <b>{now}</b>  <i>({TZ})</i>")

async def on_post(message: Message, bot: Bot):
    if not is_admin(message.from_user.id):
        return await message.answer("Нет прав.")

    # Берём всё после пробела: "/post <текст>"
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        return await message.answer("Использование: <code>/post Текст поста</code>")

    text = parts[1].strip()
    await send_to_channel(bot, text)
    await message.answer("✅ Отправлено в канал.")


# ── Запуск ────────────────────────────────────────────────────────────────────
async def main():
    bot = Bot(BOT_TOKEN, default=ParseMode.HTML)        # глобально HTML ОК
    dp = Dispatcher()

    # Регистрация хендлеров (aiogram v3)
    dp.message.register(on_start, Command("start"))
    dp.message.register(on_myid, Command("myid"))
    dp.message.register(on_now, Command("now"))
    dp.message.register(on_post, Command("post"))

    me = await bot.get_me()
    logger.info(
        f"Запущен бот @{me.username} для канала {CHANNEL_ID} (TZ {TZ})"
    )

    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Остановлен.")
