# scheduler.py
import os
import asyncio
import logging
from datetime import datetime, time, timedelta

import pytz
from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.types import InputMediaPhoto

# ---------- Импорт из вашей БД ----------
from storage.db import (
    init_db,
    get_oldest,
    delete_by_id,
    find_similar_ids,
    bulk_delete,
)

# ---------- ENV / Конфиг ----------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()  # @username или numeric id
TZ_NAME = os.getenv("TZ", "Europe/Moscow").strip()

# Времена автопостинга (локальные для TZ_NAME). Формат: "12:00,16:00,20:00"
TIMES_RAW = os.getenv("TIMES", "12:00,16:00,20:00")
SLOT_TIMES: list[time] = []
for part in TIMES_RAW.replace(" ", "").split(","):
    try:
        hh, mm = part.split(":")
        SLOT_TIMES.append(time(int(hh), int(mm)))
    except Exception:
        pass
if not SLOT_TIMES:
    SLOT_TIMES = [time(12, 0), time(16, 0), time(20, 0)]

# Админы (user_id через запятую)
ADMINS_RAW = os.getenv("ADMINS", "").strip()
ADMINS: list[int] = []
if ADMINS_RAW:
    for chunk in ADMINS_RAW.replace(" ", "").split(","):
        if not chunk:
            continue
        try:
            ADMINS.append(int(chunk))
        except ValueError:
            # если вдруг передали @username — пропустим
            pass

# За сколько минут до поста слать превью
PREVIEW_MIN = int(os.getenv("PREVIEW_MIN", "10"))

# Логгер
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("scheduler")

# TZ
try:
    tz = pytz.timezone(TZ_NAME)
except Exception:
    tz = pytz.timezone("Europe/Moscow")


# ---------- Утилиты построения поста ----------
def _extract_media_urls(item) -> list[str]:
    """
    Пытаемся вытащить список URL фото из записи БД.
    Поддерживаем несколько возможных ключей.
    """
    candidates = []
    # частые варианты ключей
    for k in ("media", "photos", "images", "pics"):
        v = item.get(k) if isinstance(item, dict) else None
        if v:
            if isinstance(v, list):
                candidates = [str(x) for x in v if x]
            elif isinstance(v, str):
                # возможно строка с запятыми
                pieces = [p.strip() for p in v.split(",") if p.strip()]
                candidates = pieces
            break
    return candidates


def _extract_text(item) -> str:
    """
    Пытаемся вытащить текст объявления.
    """
    # частые варианты ключей
    for k in ("text", "caption", "body", "content"):
        if isinstance(item, dict) and k in item and item[k]:
            return str(item[k])
    # если item — не dict, пробуем как есть
    if isinstance(item, str):
        return item
    return ""


def build_caption_and_media(item):
    """
    Возвращает:
      caption: str
      media_list: list[InputMediaPhoto] или []
      first_photo_url: str | None
    """
    caption = _extract_text(item).strip()
    urls = _extract_media_urls(item)

    media_list: list[InputMediaPhoto] = []
    first_photo_url = urls[0] if urls else None

    if urls:
        # если несколько фото — готовим медиагруппу
        for i, url in enumerate(urls):
            if i == 0:
                # подпись ставим на первый элемент (Telegram покажет её под альбомом)
                media_list.append(InputMediaPhoto(media=url, caption=caption or None, parse_mode="HTML"))
            else:
                media_list.append(InputMediaPhoto(media=url))
    # если фото нет — media_list будет пустой, значит отправим просто текст
    return caption, media_list, first_photo_url


# ---------- Уведомления админам ----------
async def notify_admins_preview(bot: Bot, when_dt: datetime, caption: str, first_photo: str | None):
    if not ADMINS:
        return
    header = f"🕒 Через {PREVIEW_MIN} мин. автопост ({when_dt:%Y-%m-%d %H:%M} {when_dt.tzname()})\n\n"
    text = header + (caption or "— без текста —")
    for admin_id in ADMINS:
        try:
            if first_photo:
                # Пытаемся прислать фото + подпись
                await bot.send_photo(admin_id, first_photo, caption=text, parse_mode="HTML", disable_notification=True)
            else:
                await bot.send_message(admin_id, text, parse_mode="HTML", disable_web_page_preview=True, disable_notification=True)
        except Exception as e:
            log.warning(f"preview to admin {admin_id} failed: {e}")


async def notify_admins_published_copy(bot: Bot, channel_id, sent_result):
    """
    Копируем опубликованный пост админам.
    Если альбом — копируем первый элемент (с подписью).
    """
    if not ADMINS:
        return

    # sent_result может быть list[Message] (media_group) или Message
    if isinstance(sent_result, list) and sent_result:
        msg_id = sent_result[0].message_id
    else:
        msg_id = getattr(sent_result, "message_id", None)

    if not msg_id:
        return

    for admin_id in ADMINS:
        try:
            await bot.copy_message(chat_id=admin_id, from_chat_id=CHANNEL_ID, message_id=msg_id)
        except Exception as e:
            log.warning(f"copy to admin {admin_id} failed: {e}")


# ---------- Время и слоты ----------
def next_slot(now_dt: datetime) -> datetime:
    """
    Возвращает ближайший datetime публикации в TZ tz.
    """
    # кандидаты сегодня
    today = now_dt.date()
    candidates = [tz.localize(datetime.combine(today, t)) for t in SLOT_TIMES]
    for dtc in candidates:
        if dtc > now_dt:
            return dtc
    # иначе — первый слот завтрашнего дня
    tomorrow = today + timedelta(days=1)
    return tz.localize(datetime.combine(tomorrow, SLOT_TIMES[0]))


# ---------- Публикация ----------
async def publish_item(bot: Bot, item) -> object | list[object] | None:
    """
    Публикует запись в канал.
    Возвращает объект(ы) Message, пригодные для copy_message.
    """
    caption, media, _ = build_caption_and_media(item)

    # если есть 2+ фото — публикуем альбом
    if len(media) >= 2:
        try:
            sent = await bot.send_media_group(CHANNEL_ID, media=media)
            return sent
        except Exception as e:
            log.warning(f"send_media_group failed: {e}")

    # если одно фото — send_photo
    if len(media) == 1:
        try:
            sent = await bot.send_photo(CHANNEL_ID, media[0].media, caption=caption or None, parse_mode="HTML")
            return sent
        except Exception as e:
            log.warning(f"send_photo failed: {e}")

    # иначе — просто текст
    try:
        sent = await bot.send_message(CHANNEL_ID, caption or " ", parse_mode="HTML", disable_web_page_preview=False)
        return sent
    except Exception as e:
        log.error(f"send_message failed: {e}")
        return None


# ---------- Основной цикл ----------
async def run_scheduler():
    if not BOT_TOKEN or not CHANNEL_ID:
        raise RuntimeError("BOT_TOKEN / CHANNEL_ID не заданы")

    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))

    # гарантируем, что таблицы БД созданы
    await init_db()

    # Лог конфигурации
    log.info(f"Scheduler TZ={TZ_NAME}, times={','.join([t.strftime('%H:%M') for t in SLOT_TIMES])}")

    while True:
        now_dt = datetime.now(tz)
        post_dt = next_slot(now_dt)
        eta_sec = (post_dt - now_dt).total_seconds()
        log.info(f"Следующий пост через {eta_sec/3600:.2f} часов ({post_dt:%Y-%m-%d %H:%M %Z})")

        # ---- PREVIEW ----
        # если есть время на превью — подсмотрим самого «кандидата» и отправим админам
        if eta_sec > PREVIEW_MIN * 60:
            peek = await get_oldest()
            if peek:
                cap, media_list, first_photo = build_caption_and_media(peek)
                try:
                    await notify_admins_preview(bot, post_dt, cap, first_photo)
                except Exception as e:
                    log.warning(f"notify preview failed: {e}")
            else:
                # очередь пуста — предупредим админов
                for admin_id in ADMINS:
                    try:
                        await bot.send_message(admin_id, f"ℹ️ Очередь пуста — публикация в {post_dt:%H:%M} пропустится.")
                    except Exception:
                        pass
            # Спим до момента постинга, минус «остаток» уже потратили на превью.
            await asyncio.sleep(eta_sec - PREVIEW_MIN * 60)
        else:
            # Прямо спим до слота
            await asyncio.sleep(max(0, eta_sec))

        # ---- ПУБЛИКАЦИЯ ----
        try:
            item = await get_oldest()
            if not item:
                log.info("Очередь пуста — нечего публиковать.")
                continue

            # Публикация
            sent = await publish_item(bot, item)
            if sent:
                # Скопировать опубликованное админам
                try:
                    await notify_admins_published_copy(bot, CHANNEL_ID, sent)
                except Exception as e:
                    log.warning(f"notify copy failed: {e}")

                # Удалить текущий элемент (опубликованный)
                try:
                    # id пытаемся достать максимально совместимо
                    item_id = None
                    if isinstance(item, dict):
                        for k in ("id", "_id", "rowid"):
                            if k in item:
                                item_id = item[k]
                                break
                    if item_id is not None:
                        await delete_by_id(item_id)
                except Exception as e:
                    log.warning(f"delete current failed: {e}")

                # Удалить похожие
                try:
                    similar_ids = await find_similar_ids(item)
                    if similar_ids:
                        await bulk_delete(similar_ids)
                except Exception as e:
                    log.warning(f"bulk_delete similar failed: {e}")
            else:
                log.error("Не удалось опубликовать пост.")
        except Exception as e:
            log.exception(f"Публикация упала: {e}")
            # ждём минуту чтобы не «крутиться» в ошибке
            await asyncio.sleep(60)


async def main():
    await run_scheduler()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
