# main.py
import asyncio
import os
import re
from typing import Optional, Tuple, List, Dict

from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode, ChatType
from aiogram.filters import Command
from aiogram.types import (
    Message,
    InputMediaPhoto,
)

# =========================
# ENV & базовая инициализация
# =========================
TOKEN = os.getenv("TOKEN", "").strip()
if not TOKEN or ":" not in TOKEN:
    raise RuntimeError("ENV TOKEN пуст или имеет неверный формат. Задайте корректный токен бота.")

def _parse_admins(env_val: str) -> List[int]:
    if not env_val:
        return []
    out = []
    for part in env_val.split(","):
        p = part.strip()
        if not p:
            continue
        try:
            out.append(int(p))
        except ValueError:
            pass
    return out

ADMINS: List[int] = _parse_admins(os.getenv("ADMINS", ""))
try:
    CHANNEL_ID = int(os.getenv("CHANNEL_ID", "-1000000000000"))
except ValueError:
    CHANNEL_ID = -1000000000000  # заглушка, чтобы не падать

TZ = os.getenv("TZ", "Europe/Moscow")

bot = Bot(TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()


# =========================
# Глобальные структуры
# =========================
# Очередь постов: [{"items":[{"type":"photo","file_id":"..."}], "caption":"...", "src": (chat_id, msg_id) or None}]
QUEUE: List[Dict] = []

# Буфер альбомов по пользователю (когда просто пересылают альбом — без реплая)
ALBUM_BUFFER: Dict[int, Dict] = {}
# Индекс по media_group_id → весь собранный альбом (чтобы /add_post в ответ на ЛЮБУЮ часть)
MEDIA_GROUPS: Dict[str, Dict] = {}

# Очистка старых записей в буферах (таймаут в секундах)
ALBUM_TTL = 120  # 2 минуты


# =========================
# Вспомогательные функции
# =========================
def build_caption(raw: str) -> str:
    """
    Приводим текст к единому виду:
    - Оставляем исходные строки (чуть чистим пробелы)
    - Обязательно добавляем две неизменные строки внизу (альбом и покупка)
    Без эмодзи, как просили.
    """
    raw = (raw or "").strip()

    # простейшие подчистки мусора и двойных пробелов
    cleaned = re.sub(r"[ \t]+", " ", raw)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()

    # неизменяемые ссылки (во всех постах)
    album_line = "Общий альбом: https://vk.com/market-222108341?screen=group&section=album_26"
    buy_line = "Покупка/вопросы: @layoutplacebuy"

    # не дублируем, если уже есть
    parts = [cleaned] if cleaned else []
    if album_line not in cleaned:
        parts.append(album_line)
    if buy_line not in cleaned:
        parts.append(buy_line)

    final = "\n\n".join([p for p in parts if p]).strip()
    return final or (album_line + "\n\n" + buy_line)


def _is_admin(uid: int) -> bool:
    return uid in ADMINS


def _src_tuple(msg: Message) -> Optional[Tuple[int, int]]:
    """
    Пытаемся вытащить исходник канального поста, чтобы потом удалить дубликат.
    Работает, если автор НЕ скрыт (forward_from_chat доступен и type == 'channel').
    """
    try:
        if msg.forward_from_chat and msg.forward_from_chat.type == ChatType.CHANNEL:
            return (msg.forward_from_chat.id, msg.forward_from_message_id)
    except Exception:
        pass
    return None


def _extract_single_from_message(msg: Message) -> Tuple[List[Dict], str]:
    """
    Извлекаем одиночное фото (если есть) и подпись.
    """
    items: List[Dict] = []
    caption = msg.caption or msg.text or ""

    if msg.photo:
        items.append({"type": "photo", "file_id": msg.photo[-1].file_id})

    return items, caption


def _get_ready_album_from_buffer(user_id: int) -> Optional[Tuple[List[Dict], str]]:
    """
    Достаём готовый альбом из пользовательского буфера, если он «свежий».
    """
    buf = ALBUM_BUFFER.get(user_id)
    if not buf:
        return None
    loop_ts = asyncio.get_running_loop().time()
    if loop_ts - buf.get("ts", 0) > ALBUM_TTL:
        ALBUM_BUFFER.pop(user_id, None)
        return None
    items = buf.get("items") or []
    if not items:
        return None
    caption = buf.get("caption") or ""
    return items, caption


def _merge_album_piece(user_id: int, msg: Message):
    """
    Сливаем очередной кусок альбома в буфер по пользователю И в индекс по media_group_id.
    Благодаря этому /add_post можно сделать реплаем на любую часть альбома.
    """
    mg_id = msg.media_group_id
    if not mg_id:
        return

    # ---- буфер по пользователю ----
    u_buf = ALBUM_BUFFER.get(user_id)
    if not u_buf or u_buf.get("mg_id") != mg_id:
        ALBUM_BUFFER[user_id] = {
            "mg_id": mg_id,
            "items": [],
            "caption": msg.caption or "",
            "ts": asyncio.get_running_loop().time(),
        }
        u_buf = ALBUM_BUFFER[user_id]

    if msg.photo:
        u_buf["items"].append({"type": "photo", "file_id": msg.photo[-1].file_id})
    if msg.caption and not u_buf.get("caption"):
        u_buf["caption"] = msg.caption
    u_buf["ts"] = asyncio.get_running_loop().time()

    # ---- индекс по media_group_id ----
    g = MEDIA_GROUPS.get(mg_id)
    if not g:
        MEDIA_GROUPS[mg_id] = {
            "items": [],
            "caption": msg.caption or "",
            "ts": asyncio.get_running_loop().time(),
        }
        g = MEDIA_GROUPS[mg_id]

    if msg.photo:
        g["items"].append({"type": "photo", "file_id": msg.photo[-1].file_id})
    if msg.caption and not g.get("caption"):
        g["caption"] = msg.caption
    g["ts"] = asyncio.get_running_loop().time()


async def _post_to_channel(task: Dict) -> bool:
    """
    Постинг в канал. Возвращает True при успехе.
    task = {"items":[...], "caption":"...", "src": (chat_id, msg_id) or None}
    """
    items = task.get("items") or []
    caption = task.get("caption") or ""

    if not items and not caption:
        return False

    # мультимедиа альбом
    if len(items) > 1:
        media: List[InputMediaPhoto] = []
        for i, it in enumerate(items):
            if it["type"] == "photo":
                if i == 0:
                    media.append(InputMediaPhoto(media=it["file_id"], caption=caption, parse_mode=ParseMode.HTML))
                else:
                    media.append(InputMediaPhoto(media=it["file_id"]))
        await bot.send_media_group(chat_id=CHANNEL_ID, media=media)
        return True

    # одиночка (фото с подписью или просто текст)
    if items and items[0]["type"] == "photo":
        await bot.send_photo(chat_id=CHANNEL_ID, photo=items[0]["file_id"], caption=caption)
        return True

    # чисто текст
    await bot.send_message(chat_id=CHANNEL_ID, text=caption, disable_web_page_preview=True)
    return True


async def _maybe_delete_original(src: Optional[Tuple[int, int]]):
    """
    Если знаем исходный канал и msg_id — пробуем удалить оригинал.
    Не всегда возможно (скрытый автор, чужой канал, нет прав).
    """
    if not src:
        return
    chat_id, msg_id = src
    try:
        await bot.delete_message(chat_id=chat_id, message_id=msg_id)
    except Exception as e:
        # Молча проглатываем — удаление не критично для работы
        print(f"Warn: can't delete original {chat_id}/{msg_id}: {e}")


# =========================
# Хендлеры
# =========================
@dp.message(Command("start"))
async def cmd_start(m: Message):
    help_text = (
        "Привет! Я помогу публиковать объявления в канал.\n\n"
        "<b>Основные команды:</b>\n"
        "• <b>/add_post</b> — добавить в очередь:\n"
        "    └ Реплай на часть альбома → возьму весь альбом целиком\n"
        "    └ Или просто после пересылки альбома (без реплая) — из буфера\n"
        "    └ Одиночное фото/текст тоже поддерживается\n"
        "• <b>/queue</b> — показать размер очереди\n"
        "• <b>/post_oldest</b> — запостить самый старый из очереди\n"
        "• <b>/clear_queue</b> — очистить очередь (только админы)\n\n"
        "Формат подписи приводится к единому виду и внизу <i>всегда</i> добавляются:\n"
        "«Общий альбом» и «Покупка/вопросы»."
    )
    await m.answer(help_text, disable_web_page_preview=True)


# Собираем части альбома в буферы (по пользователю и по media_group_id)
@dp.message(F.media_group_id != None, F.content_type.in_({"photo"}))
async def on_any_album_piece(m: Message):
    _merge_album_piece(m.from_user.id, m)


@dp.message(Command("add_post"))
async def cmd_add_post(m: Message):
    """
    Добавляет пост в очередь.
    Приоритет:
      A) если это реплай на часть альбома -> берём весь альбом по media_group_id из MEDIA_GROUPS
      B) иначе, если недавно пересылали альбом -> берём из ALBUM_BUFFER по user_id
      C) иначе берём одиночное сообщение (реплай или текущее)
    Сохраняем source (если возможно), чтобы потом удалить оригинал.
    """
    user_id = m.from_user.id

    # --- A) Реплай на часть альбома? ---
    src_msg = m.reply_to_message
    if src_msg and src_msg.media_group_id:
        mg_id = src_msg.media_group_id
        g = MEDIA_GROUPS.get(mg_id)
        # не старше ALBUM_TTL
        if g and (asyncio.get_running_loop().time() - g.get("ts", 0) <= ALBUM_TTL) and g.get("items"):
            items = list(g["items"])
            caption = g.get("caption") or ""
            final_caption = build_caption(caption)
            src = _src_tuple(src_msg)  # попытка сохранить источник для удаления
            QUEUE.append({"items": items, "caption": final_caption, "src": src})
            await m.answer("✅ Альбом (по реплаю) добавлен в очередь.")
            return
        # если индекс не найден — попробуем fallback на пользовательский буфер ниже

    # --- B) Пробуем взять готовый альбом из пользовательского буфера ---
    ready = _get_ready_album_from_buffer(user_id)
    if ready:
        items, caption = ready
        src = _src_tuple(m.reply_to_message or m)  # возможно, пересылали прямо сейчас
    else:
        # --- C) одиночное (реплай или текущее) ---
        src_msg = m.reply_to_message or m
        items, caption = _extract_single_from_message(src_msg)
        src = _src_tuple(src_msg)

    if not items and not caption:
        await m.answer("❌ Не нашёл ни фото/альбома, ни текста. Перешли пост и снова /add_post.")
        return

    final_caption = build_caption(caption)
    QUEUE.append({"items": items, "caption": final_caption, "src": src})
    await m.answer("✅ Пост добавлен в очередь.")


@dp.message(Command("queue"))
async def cmd_queue(m: Message):
    await m.answer(f"В очереди: {len(QUEUE)}.")


@dp.message(Command("clear_queue"))
async def cmd_clear(m: Message):
    if not _is_admin(m.from_user.id):
        return
    QUEUE.clear()
    await m.answer("🧹 Очередь очищена.")


@dp.message(Command("post_oldest"))
async def cmd_post_oldest(m: Message):
    """
    Публикует самый старый пост из очереди, затем пытается удалить исходник (если он известен).
    """
    if not QUEUE:
        await m.answer("Очередь пуста.")
        return

    task = QUEUE.pop(0)
    ok = await _post_to_channel(task)
    if not ok:
        await m.answer("Не удалось отправить пост.")
        return

    # попытка удалить оригинал
    await _maybe_delete_original(task.get("src"))
    await m.answer("✅ Опубликовано.")


# =========================
# Точка входа
# =========================
async def run_bot():
    print("Starting bot instance...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(run_bot())
