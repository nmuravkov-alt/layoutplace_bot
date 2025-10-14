# scheduler.py
import os
import asyncio
import logging
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo
from typing import List
import json

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramUnauthorizedError, TelegramBadRequest

# наши функции БД
from storage.db import (
    init_db,
    queue_next_pending,
    queue_mark_status,
    queue_count_pending,
)

# конфиг (токен / канал / TZ / админы)
from config import TOKEN as BOT_TOKEN, CHANNEL_ID, TZ as TZ_NAME, ADMINS

# ----------------- ЛОГИ -----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("scheduler")

# ----------------- ПАРАМЕТРЫ РАСПИСАНИЯ -----------------
# Слоты публикации (локальное время TZ_NAME)
TIMES_RAW = os.getenv("TIMES", "12:00,16:00,20:00")
# За сколько минут до слота присылать превью админу
PREVIEW_BEFORE_MIN = int(os.getenv("PREVIEW_BEFORE_MIN", "45"))

tz = ZoneInfo(TZ_NAME)

def _parse_times(s: str) -> List[dtime]:
    out: List[dtime] = []
    for token in s.split(","):
        token = token.strip()
        if not token:
            continue
        h, m = token.split(":")
        out.append(dtime(hour=int(h), minute=int(m)))
    return out or [dtime(12, 0), dtime(16, 0), dtime(20, 0)]

TIMES = _parse_times(TIMES_RAW)

def _now() -> datetime:
    return datetime.now(tz)

def _next_run(now: datetime) -> datetime:
    today_slots = [datetime.combine(now.date(), t, tzinfo=tz) for t in TIMES]
    future = [dt for dt in today_slots if dt > now]
    if future:
        return future[0]
    # иначе — первый слот завтрашнего дня
    tomorrow = now.date() + timedelta(days=1)
    return datetime.combine(tomorrow, TIMES[0], tzinfo=tz)

# ----------------- СЕРВИСНЫЕ -----------------
async def _notify_admins(bot: Bot, text: str):
    for aid in ADMINS:
        try:
            await bot.send_message(aid, text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        except TelegramUnauthorizedError:
            log.warning("Админ %s недоступен (Unauthorized). Нужно нажать /start боту в ЛС.", aid)
        except Exception as e:
            log.warning("Не удалось отправить админу %s: %s", aid, e)

def unify_caption(text: str | None) -> str:
    """Приводим подпись к общему формату (как в main.py)."""
    text = (text or "").strip()
    text = text.replace("Цена -", "Цена —")
    while "  " in text:
        text = text.replace("  ", " ")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    text = "\n".join(lines)
    if "layoutplacebuy" not in text.lower():
        text += "\n\n@layoutplacebuy"
    return text

async def copy_and_delete(
    bot: Bot,
    source_chat_id: int | str,
    message_ids: List[int],
    target: int | str,
    caption_override: str | None,
):
    """
    Копируем пост/альбом без автора (copy_message).
    Первому сообщению ставим унифицированную подпись.
    После публикации пытаемся удалить оригиналы.
    """
    new_caption = unify_caption(caption_override)
    for idx, mid in enumerate(message_ids):
        if idx == 0 and new_caption:
            await bot.copy_message(
                chat_id=target,
                from_chat_id=source_chat_id,
                message_id=mid,
                caption=new_caption,
                parse_mode=ParseMode.HTML,
            )
        else:
            await bot.copy_message(
                chat_id=target,
                from_chat_id=source_chat_id,
                message_id=mid,
            )

    # удаляем оригиналы (если у бота есть право delete в источнике)
    for mid in message_ids:
        try:
            await bot.delete_message(chat_id=source_chat_id, message_id=mid)
        except TelegramBadRequest:
            # нет прав / слишком старо / уже удалено — не критично
            pass
        except Exception as e:
            log.debug("Ошибка удаления исходного %s/%s: %s", source_chat_id, mid, e)

# ----------------- ПРЕВЬЮ -----------------
async def send_preview(bot: Bot, row: dict, when_post: datetime):
    """
    Отправляем текстовое превью с метаданными — только в личку админам.
    """
    caption = row.get("caption_override") or ""
    text = (
        "👀 <b>Предпросмотр публикации</b>\n"
        f"🕒 Время поста: <code>{when_post.strftime('%Y-%m-%d %H:%M')}</code> ({TZ_NAME})\n"
        f"Источник: <code>{row['source_chat_id']}</code>\n"
        f"Messages: <code>{row['message_ids']}</code>\n\n"
        f"{caption}"
    )
    await _notify_admins(bot, text)

# ----------------- ОСНОВНОЙ ЦИКЛ -----------------
async def run_scheduler():
    # В aiogram v3 в DefaultBotProperties нельзя указывать disable_web_page_preview
    props = DefaultBotProperties(parse_mode=ParseMode.HTML)
    bot = Bot(BOT_TOKEN, default=props)

    await _notify_admins(
        bot,
        f"🕒 Планировщик запущен.\nСлоты: <code>{TIMES_RAW}</code>\n"
        f"Превью: <b>{PREVIEW_BEFORE_MIN}</b> мин.\nВ очереди: <b>{queue_count_pending()}</b>",
    )

    while True:
        now = _now()
        next_slot = _next_run(now)
        preview_at = next_slot - timedelta(minutes=PREVIEW_BEFORE_MIN)
        if preview_at < now:
            preview_at = now + timedelta(seconds=5)

        # ---- ждём превью ----
        delay_preview = max(0.0, (preview_at - _now()).total_seconds())
        await asyncio.sleep(delay_preview)

        row = queue_next_pending()
        if row:
            await send_preview(bot, row, next_slot)
            queue_mark_status(row["id"], "previewed")
            log.info("Отправлено превью для task #%s (пост в %s).", row["id"], next_slot.strftime("%H:%M"))
        else:
            await _notify_admins(bot, "ℹ️ Очередь пуста — публиковать нечего.")

        # ---- ждём сам слот ----
        delay_post = max(0.0, (next_slot - _now()).total_seconds())
        await asyncio.sleep(delay_post)

        row = queue_next_pending()
        if not row:
            log.info("Слот %s: очередь пуста.", next_slot.strftime("%H:%M"))
            continue

        # message_ids: JSON-строка в БД
        try:
            message_ids = [int(x) for x in json.loads(row["message_ids"])]
        except Exception:
            # совместимость со старыми записями
            message_ids = [int(x) for x in eval(row["message_ids"])]

        try:
            await copy_and_delete(
                bot=bot,
                source_chat_id=row["source_chat_id"],
                message_ids=message_ids,
                target=CHANNEL_ID,
                caption_override=row.get("caption_override"),
            )
            queue_mark_status(row["id"], "posted")
            await _notify_admins(
                bot, f"✅ Опубликовано из <code>{row['source_chat_id']}</code> — ids={message_ids}"
            )
            log.info("✅ Posted task #%s", row["id"])
        except Exception as e:
            queue_mark_status(row["id"], "error")
            await _notify_admins(bot, f"❌ Ошибка публикации id={row['id']}: <code>{e}</code>")
            log.exception("Ошибка публикации task #%s: %s", row["id"], e)

# ----------------- ENTRY -----------------
async def main():
    init_db()
    log.info("Scheduler  TZ=%s, times=%s, preview_before=%s min", TZ_NAME, TIMES_RAW, PREVIEW_BEFORE_MIN)
    await run_scheduler()

if __name__ == "__main__":
    asyncio.run(main())
