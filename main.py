import os
import asyncio
import json
import logging
import pytz
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.exceptions import TelegramConflictError, TelegramBadRequest

# ---------------- НАСТРОЙКИ ----------------

TOKEN = os.getenv("TOKEN", "")
if not TOKEN or ":" not in TOKEN:
    raise RuntimeError("ENV TOKEN пуст или неверный")

ADMINS = [int(x) for x in os.getenv("ADMINS", "").split(",") if x]
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "-1001758490510"))
CONTACT = os.getenv("CONTACT", "@layoutplacebuy")
ALBUM_URL = os.getenv("ALBUM_URL", "")
POST_TIMES = [t.strip() for t in os.getenv("POST_TIMES", "12:00,16:00,20:00").split(",")]
PREVIEW_BEFORE_MIN = int(os.getenv("PREVIEW_BEFORE_MIN", "45"))
TZ = os.getenv("TZ", "Europe/Moscow")

# ---------------- ЛОГИ ----------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("layoutplace_bot")

# ---------------- ОБЪЕКТЫ ----------------
bot = Bot(TOKEN, parse_mode="HTML")
dp = Dispatcher()
tz = pytz.timezone(TZ)

# Очередь постов в памяти (sqlite можно подключить отдельно)
queue = []


# ---------------- УТИЛИТЫ ----------------
def now_tz() -> datetime:
    return datetime.now(tz)


def normalize_text(text: str) -> str:
    """Приводим текст поста к нужному шаблону."""
    text = text.strip()

    if not text:
        return ""

    # Добавляем фиксированные строки в конце
    footer = f"\n\n#толстовки\nОбщий альбом: {ALBUM_URL}\nПокупка/вопросы: {CONTACT}"
    if footer not in text:
        text = f"{text}{footer}"

    return text


# ---------------- ФУНКЦИИ ----------------
async def send_preview():
    for admin in ADMINS:
        try:
            await bot.send_message(admin, "⏰ Напоминание: постинг через 45 минут.")
        except Exception as e:
            log.warning(f"Не удалось отправить превью админу {admin}: {e}")


async def post_oldest():
    """Публикуем первый пост из очереди"""
    if not queue:
        log.info("Очередь пуста, нечего постить.")
        return False

    item = queue.pop(0)

    # Удаляем старое сообщение из канала, если было
    try:
        await bot.delete_message(CHANNEL_ID, item.get("last_msg_id"))
    except TelegramBadRequest:
        pass
    except Exception as e:
        log.warning(f"Не удалось удалить старое сообщение: {e}")

    caption = normalize_text(item["caption"])

    try:
        if item["media"]:
            media = [
                types.InputMediaPhoto(media=ph, caption=caption if i == 0 else None)
                for i, ph in enumerate(item["media"])
            ]
            msgs = await bot.send_media_group(CHANNEL_ID, media)
            log.info(f"Отправлен альбом в канал {CHANNEL_ID}")
            item["last_msg_id"] = msgs[0].message_id
        else:
            msg = await bot.send_message(CHANNEL_ID, caption)
            item["last_msg_id"] = msg.message_id
    except Exception as e:
        log.exception(f"Ошибка при постинге: {e}")
        return False

    return True


async def scheduler_loop():
    log.info(f"Scheduler TZ={TZ}, times={POST_TIMES}, preview_before={PREVIEW_BEFORE_MIN} мин")

    while True:
        try:
            now = now_tz()

            # Определяем ближайший слот постинга
            slots = []
            for t in POST_TIMES:
                h, m = map(int, t.split(":"))
                dt = tz.localize(datetime(now.year, now.month, now.day, h, m))
                if dt < now:
                    dt += timedelta(days=1)
                slots.append(dt)

            next_slot = min(slots)
            preview_time = next_slot - timedelta(minutes=PREVIEW_BEFORE_MIN)

            sleep_preview = (preview_time - now_tz()).total_seconds()
            if sleep_preview > 0:
                await asyncio.sleep(sleep_preview)
                await send_preview()

            sleep_post = (next_slot - now_tz()).total_seconds()
            if sleep_post > 0:
                await asyncio.sleep(sleep_post)
                await post_oldest()

        except asyncio.CancelledError:
            log.info("Scheduler остановлен")
            break
        except Exception as e:
            log.exception(f"Ошибка планировщика: {e}")
            await asyncio.sleep(5)


# ---------------- КОМАНДЫ ----------------
@dp.message(Command("start"))
async def cmd_start(m: types.Message):
    await m.answer("✅ Бот запущен и готов к работе.\n"
                   "Посты добавляются через пересылку сообщений.\n"
                   "Команды:\n"
                   "• /add_post — добавить пост в очередь\n"
                   "• /post_oldest — опубликовать ближайший пост")


@dp.message(Command("add_post"))
async def cmd_add_post(m: types.Message):
    """Добавляем пересланный пост (текст + фото) в очередь"""
    if not m.reply_to_message:
        await m.answer("Перешли сообщение с постом и ответь на него этой командой.")
        return

    r = m.reply_to_message

    media = []
    caption = ""
    if r.photo:
        media.append(r.photo[-1].file_id)
        caption = r.caption or ""
    elif r.text:
        caption = r.text
    elif r.media_group_id:
        media.append(r.photo[-1].file_id)

    queue.append({
        "media": media,
        "caption": caption,
        "created_at": now_tz().isoformat(),
    })

    await m.answer(f"Пост добавлен в очередь. Сейчас {len(queue)} в очереди.")


@dp.message(Command("post_oldest"))
async def cmd_post_oldest(m: types.Message):
    ok = await post_oldest()
    await m.answer("✅ Пост опубликован." if ok else "⚠️ Очередь пуста.")


# ---------------- ХУКИ ----------------
_scheduler_task = None

async def on_startup():
    global _scheduler_task
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass
    if not _scheduler_task or _scheduler_task.done():
        _scheduler_task = asyncio.create_task(scheduler_loop())
        log.info("Scheduler запущен.")

async def on_shutdown():
    global _scheduler_task
    if _scheduler_task and not _scheduler_task.done():
        _scheduler_task.cancel()
        try:
            await _scheduler_task
        except asyncio.CancelledError:
            pass
        log.info("Scheduler остановлен.")


# ---------------- ТОЧКА ВХОДА ----------------
async def run_bot():
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    log.info("🚀 Стартуем Layoutplace Bot...")

    backoff = 1.0
    while True:
        try:
            await dp.start_polling(bot, allowed_updates=None)
            break
        except TelegramConflictError:
            log.error("⚠️ Конфликт polling — бот уже запущен где-то. Перезапуск...")
            try:
                await bot.delete_webhook(drop_pending_updates=True)
            except Exception:
                pass
            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.5, 10.0)
        except Exception as e:
            log.exception(f"Polling error: {e}")
            await asyncio.sleep(3)
