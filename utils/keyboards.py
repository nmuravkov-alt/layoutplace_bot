from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

def post_actions_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Опубликовать", callback_data="approve")],[InlineKeyboardButton(text="🗑️ Удалить", callback_data="reject")]])

def cta_kb(text: str, url: str):
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=text, url=url)]])
