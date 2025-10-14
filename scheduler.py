# scheduler.py
import os
import asyncio
import logging
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from storage.db import (
    init_db,
    queue_next_pending,
    queue_mark_status,
    queue_count_pending,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("scheduler")

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()  # @username или -100...
ADMINS = [a.strip() for a in os.getenv("ADMINS", "").split(",") if a.strip()]
TZ_NAME = os.getenv("TZ", "Europe/Moscow")
# часы публикации
TIMES_RAW = os.getenv("TIMES", "12:00,16:00,20:00")
# превью за N минут до слота
PREVIEW_BEFORE_MIN = int(os.getenv("PREVIEW_BEFORE_MIN", "45"))

tz = ZoneInfo(TZ_NAME)

def _parse_times(s: str) -> list[dtime]:
    out = []
    for token in s.split(","):
        token = token.strip()
        if not token:
            continue
        h, m = token.split(":")
        out.append(dtime(hour=int(h), minute=int(m)))
    return out

TIMES = _parse_times(TIMES_RAW)

def _utcnow():
    return datetime.now(tz)

def _next_run(now: datetime, slots: list[dtime]) -> datetime:
    today_slots = [datetime.combine(now.date(), t, tzinfo=tz) for t in slots]
    future = [dt for dt in today_slots if dt > now]
    if future:
        return future[0]
    # завтра, самый ранний слот
    tomorrow = now.date() + timedelta(days=1)
    return datetime.combine(tomorrow, slots[0], tzinfo=tz)

async def _notify_admins(bot: Bot, text: str):
    for aid in ADMINS:
        try:
            await bot.send_message(aid, text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        except Exception as e:
            log.exception("Не удалось отправить админу %s: %s", aid, e)

# ---------------- форматирование подписи ----------------
def unify_caption(text: str | None) -> str:
    text = (text or "").strip()

    # простые правки
    text = text.replace("Цена -", "Цена —").replace("Цена — ", "Цена — ")
    text = text.replace("Размер:", "Размер:").replace("Состояние :", "Состояние :").replace("Состояние:", "Состояние :")
    # убираем двойные пробелы и пустые строки
    lines = [ln.strip() for ln in text.splitlines()]
    lines = [ln for ln in lines if ln]
    text = "\n".join(lines)

    # добавим «Общие» ссылки, если их нет
    if "layoutplacebuy" not in text:
        text += "\n\n@layoutplacebuy"
    if "#штаны" in text or "#куртки" in text or "#аксессуары" in text:
        # ок — теги уже есть
        pass

    return text

# --------------- копирование поста и удаление оригинала ---------------
async def copy_and_delete(bot: Bot, source_chat_id: int, message_ids: list[int], target: str | int, caption_override: str | None):
    # Копируем пачкой по одному сообщению
    posted_message_ids: list[int] = []
    caption = unify_caption(caption_override)
    for idx, mid in enumerate(message_ids):
        try:
            if idx == 0 and caption:
                msg = await bot.copy_message(
                    chat_id=target,
                    from_chat_id=source_chat_id,
                    message_id=mid,
                    caption=caption,
                    parse_mode=ParseMode.HTML
                )
            else:
                msg = await bot.copy_message(
                    chat_id=target,
                    from_chat_id=source_chat_id,
                    message_id=mid
                )
            posted_message_ids.append(msg.message_id)
        except Exception as e:
            log.exception("Ошибка копирования message_id=%s: %s", mid, e)
            raise

    # Удаляем оригиналы (если у бота есть права)
    for mid in message_ids:
        try:
            await bot.delete_message(chat_id=source_chat_id, message_id=mid)
        except Exception:
            # не критично — бывает нет прав на удаление старых сообщений
            pass

    return posted_message_ids

# ---------------- основной цикл ----------------
async def run_scheduler():
    props = DefaultBotProperties(parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    bot = Bot(BOT_TOKEN, default=props)

    await _notify_admins(bot, f"🕒 Планировщик запущен.\nСлоты: <code>{TIMES_RAW}</code>\nПревью: <b>{PREVIEW_BEFORE_MIN}</b> мин.\nОчередь: <b>{queue_count_pending()}</b>")

    while True:
        now = _utcnow()
        next_slot = _next_run(now, TIMES)
        # момент превью
        preview_at = next_slot - timedelta(minutes=PREVIEW_BEFORE_MIN)
        if preview_at < now:
            # если «опоздали» — превью сразу
            preview_at = now + timedelta(seconds=5)

        # Ждём превью
        delay_preview = max(0.0, (preview_at - _utcnow()).total_seconds())
        await asyncio.sleep(delay_preview)

        row = queue_next_pending()
        if row:
            # превью админу
            preview_text = (
                f"👀 Предпросмотр на {next_slot.strftime('%H:%M')}:\n"
                f"<i>источник</i>: <code>{row['source_chat_id']}</code>\n"
                f"<i>messages</i>: <code>{row['message_ids']}</code>"
            )
            await _notify_admins(bot, preview_text)
            queue_mark_status(row["id"], "previewed")
        else:
            await _notify_admins(bot, "ℹ️ Очередь пуста — публиковать нечего.")

        # Ждём сам слот
        delay_post = max(0.0, (next_slot - _utcnow()).total_seconds())
        await asyncio.sleep(delay_post)

        # Публикация
        row = queue_next_pending()
        if not row:
            log.info("Слот %s: очередь пуста", next_slot)
            continue

        try:
            message_ids = [int(x) for x in eval(row["message_ids"])]
        except Exception:
            import json
            message_ids = [int(x) for x in json.loads(row["message_ids"])]

        try:
            await copy_and_delete(
                bot=bot,
                source_chat_id=int(row["source_chat_id"]),
                message_ids=message_ids,
                target=CHANNEL_ID,
                caption_override=row.get("caption_override")
            )
            queue_mark_status(row["id"], "posted")
            await _notify_admins(bot, f"✅ Опубликовано из <code>{row['source_chat_id']}</code> ids={message_ids}")
        except Exception as e:
            queue_mark_status(row["id"], "error")
            await _notify_admins(bot, f"❌ Ошибка публикации id={row['id']}: <code>{e}</code>")

async def main():
    init_db()
    log.info("Scheduler  TZ=%s, times=%s, preview_before=%s min", TZ_NAME, TIMES_RAW, PREVIEW_BEFORE_MIN)
    await run_scheduler()

if __name__ == "__main__":
    asyncio.run(main())
