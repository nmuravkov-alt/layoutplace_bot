# main.py
import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import List

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ChatType
from aiogram.filters import Command, CommandObject
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

from storage.db import (
    init_db,
    enqueue as db_enqueue,
    get_oldest,
    delete_by_id,
    find_similar_ids,
    bulk_delete,
    plan_cancel,
    plan_get,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("layoutplace_bot")

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()  # @username или -100...
ADMINS_RAW = os.getenv("ADMINS", "").strip()      # id через запятую
TZ = os.getenv("TZ", "Europe/Moscow")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is empty")

def _parse_admins(raw: str) -> List[int]:
    ids: List[int] = []
    for p in (raw or "").replace(";", ",").split(","):
        p = p.strip()
        if not p:
            continue
        try:
            ids.append(int(p))
        except ValueError:
            pass
    return ids

ADMINS = _parse_admins(ADMINS_RAW)

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())

# ------------------------ утилиты ------------------------

def _now_str() -> str:
    try:
        import pytz, datetime as _dt
        tz = pytz.timezone(TZ)
        return _dt.datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

async def _send_to_admins(text: str, reply_markup=None):
    for admin_id in ADMINS:
        try:
            await bot.send_message(admin_id, text, reply_markup=reply_markup, disable_web_page_preview=True)
        except Exception as e:
            log.warning("send admin %s failed: %s", admin_id, e)

async def _send_to_channel(text: str):
    await bot.send_message(CHANNEL_ID, text, disable_web_page_preview=False)

# ------------------------ команды ------------------------

@dp.message(Command("start"))
async def cmd_start(m: Message):
    help_text = (
        "Готов к работе.\n\n"
        "<b>Команды</b>:\n"
        "/myid — показать твой Telegram ID\n"
        "/enqueue <текст> — положить объявление в очередь\n"
        "/queue — показать размер очереди\n"
        "/post_oldest — опубликовать самое старое и удалить похожие\n"
        "/now — текущее время сервера\n"
        "/plans — показать запланированные (для отладки)\n"
    )
    await m.answer(help_text)

@dp.message(Command("myid"))
async def cmd_myid(m: Message):
    await m.answer(f"Твой Telegram ID: <code>{m.from_user.id}</code>")

def _is_admin(uid: int) -> bool:
    return uid in ADMINS

@dp.message(Command("now"))
async def cmd_now(m: Message):
    await m.answer(f"Серверное время: <b>{_now_str()}</b>")

@dp.message(Command("enqueue"))
async def cmd_enqueue(m: Message, command: CommandObject):
    if not _is_admin(m.from_user.id):
        return await m.answer("Нет прав.")
    text = (command.args or "").strip()
    if not text:
        return await m.answer("Использование:\n/enqueue <текст объявления>")
    import time
    ad_id = db_enqueue(text, int(time.time()))
    await m.answer(f"Добавлено в очередь (id={ad_id}).")

@dp.message(Command("queue"))
async def cmd_queue(m: Message):
    # просто считаем приблизительно
    from storage.db import _cx
    with _cx() as cx:
        cnt = cx.execute("SELECT COUNT(*) FROM ads").fetchone()[0]
    await m.answer(f"В очереди объявлений: <b>{cnt}</b>")

@dp.message(Command("post_oldest"))
async def cmd_post_oldest(m: Message):
    if not _is_admin(m.from_user.id):
        return await m.answer("Нет прав.")
    ad = get_oldest()
    if not ad:
        return await m.answer("Очередь пуста.")
    ad_id, text = ad
    await _send_to_channel(text)
    # удаляем сам пост и похожие
    sims = find_similar_ids(ad_id)
    delete_by_id(ad_id)
    bulk_delete(sims)
    await m.answer(f"Опубликовано.\nУдалено похожих: {len(sims)}")

@dp.message(Command("plans"))
async def cmd_plans(m: Message):
    if not _is_admin(m.from_user.id):
        return await m.answer("Нет прав.")
    from storage.db import _cx
    with _cx() as cx:
        rows = cx.execute(
            "SELECT token, ad_id, run_at, status FROM planned ORDER BY run_at ASC LIMIT 10"
        ).fetchall()
    if not rows:
        return await m.answer("Запланированных записей нет.")
    import datetime as _dt
    items = []
    for token, ad_id, run_at, status in rows:
        when = datetime.fromtimestamp(run_at).strftime("%Y-%m-%d %H:%M:%S")
        items.append(f"• <code>{token}</code> — ad:{ad_id} — {when} — <b>{status}</b>")
    await m.answer("\n".join(items))

# ------------------------ callback: отмена превью ------------------------

@dp.callback_query(F.data.startswith("cancel:"))
async def cb_cancel(q: CallbackQuery):
    if not _is_admin(q.from_user.id):
        return await q.answer("Нет прав", show_alert=True)

    token = q.data.split(":", 1)[1]
    ok = plan_cancel(token)
    plan = plan_get(token)
    if ok:
        await q.answer("Пост отменён")
        await q.message.edit_reply_markup(reply_markup=None)
        if plan:
            await _send_to_admins(
                f"❌ Отменён пост по превью.\n"
                f"token: <code>{token}</code>\n"
                f"ad_id: <b>{plan['ad_id']}</b>\n"
            )
    else:
        await q.answer("Не удалось отменить (возможно, уже отменён/не найден)", show_alert=True)

# ------------------------ run ------------------------

async def main():
    await init_db()
    log.info("Бот запущен для %s (TZ %s)", CHANNEL_ID, TZ)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
