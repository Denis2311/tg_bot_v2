#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import asyncio
import logging
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder
from deep_translator import GoogleTranslator
from db import init_db, save_request, get_user_requests, get_request_by_id

# === НАСТРОЙКИ ЛОГИРОВАНИЯ ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# === НАСТРОЙКИ ===
BOT_TOKEN = "8563519693:AAEZL3guplbdJJ700ZLDWu0GtI705jTSN7E"
MAIN_CHAT_ID = -1003345325031

TOPIC_IDS = {
    "global": 3,
    "russia_sng": 4,
    "china": 5
}

# === ЛОКАЛИЗАЦИЯ ===
MESSAGES = {
    "start_choose_lang": "👋 Добро пожаловать! Пожалуйста, выберите язык.\n\n"
                         "👋 Welcome! Please choose your language.\n\n"
                         "👋 欢迎！请选择语言。",
    "lang_selected": {
        "ru": "🇷🇺 Выбран русский язык.",
        "en": "🇺🇸 English selected.",
        "zh": "🇨🇳 中文已选择。"
    },
    "choose_server": {
        "ru": "🌍 Выберите сервер для демо-запроса:",
        "en": "🌍 Choose a server for the demo request:",
        "zh": "🌍 请选择演示服务器："
    },
    "ask_server_version": {
        "ru": "📦 Выберите версию сервера:",
        "en": "📦 Choose server version:",
        "zh": "📦 请选择服务器版本："
    },
    "ask_area": {
        "ru": "📐 Выберите размер игровой площадки:",
        "en": "📐 Choose game area size:",
        "zh": "📐 请选择游戏区域尺寸"
    },
    "ask_vr_device": {
        "ru": "👓 Выберите VR-шлем:",
        "en": "👓 Choose VR headset:",
        "zh": "👓 请选择 VR 头显："
    },
    "ask_partner_contact": {
        "ru": "📎 Хотите добавить контактные данные партнёра?",
        "en": "📎 Would you like to add partner contact details?",
        "zh": "📎 是否要添加合作伙伴联系信息？"
    },
    "partner_contact_yes": {
        "ru": "✅ Да, добавить",
        "en": "✅ Yes, add",
        "zh": "✅ 是，添加"
    },
    "partner_contact_no": {
        "ru": "❌ Нет, пропустить",
        "en": "❌ No, skip",
        "zh": "❌ 否，跳过"
    },
    "ask_partner_name": {
        "ru": "👤 Введите имя партнёра:",
        "en": "👤 Enter partner name:",
        "zh": "👤 请输入合作伙伴姓名："
    },
    "ask_partner_phone": {
        "ru": "📱 Введите номер телефона партнёра:",
        "en": "📱 Enter partner phone number:",
        "zh": "📱 请输入合作伙伴电话号码："
    },
    "ask_partner_email": {
        "ru": "📧 Введите email партнёра:",
        "en": "📧 Enter partner email:",
        "zh": "📧 请输入合作伙伴电子邮件："
    },
    "ask_partner_crm": {
        "ru": "🔗 Введите ссылку на CRM партнёра:",
        "en": "🔗 Enter partner CRM link:",
        "zh": "🔗 请输入合作伙伴CRM链接："
    },
    "ask_city": {
        "ru": "🌍 Укажите страну / город:",
        "en": "🌍 Enter country / city:",
        "zh": "🌍 请输入国家 / 城市："
    },
    "ask_duration": {
        "ru": "⏳ Укажите срок действия демо игры:",
        "en": "⏳ Enter demo validity period:",
        "zh": "⏳ 请输入演示有效期："
    },
    "ask_comment": {
        "ru": "✏️ Добавить комментарий (опционально):",
        "en": "✏️ Add a comment (optional):",
        "zh": "✏️ 添加评论（可选）："
    },
    "enter_comment": {
        "ru": "Введите комментарий:",
        "en": "Enter comment:",
        "zh": "请输入评论："
    },
    "send_without_comment": {
        "ru": "✅ Отправить без комментария",
        "en": "✅ Send without comment",
        "zh": "✅ 发送，无需评论"
    },
    "success_with_link": {
        "ru": "✅ Запрос успешно оформлен и отправлен в раздел <a href='{link}'>[перейти к запросу]</a>",
        "en": "✅ Request successfully submitted and sent to section <a href='{link}'>[go to request]</a>",
        "zh": "✅ 请求已成功提交并发送至分区 <a href='{link}'>[跳转到请求]</a>"
    },
    "buttons": {
        "lang": {
            "ru": {"lang_ru": "🇷🇺 Русский", "lang_en": "🇺🇸 English", "lang_zh": "🇨 中文"},
            "en": {"lang_ru": "🇷🇺 Russian", "lang_en": "🇺 English", "lang_zh": "🇨🇳 Chinese"},
            "zh": {"lang_ru": "🇷 俄语", "lang_en": "🇺🇸 English", "lang_zh": "🇨 中文"}
        },
        "server": {
            "ru": {"server_usd": "🇺🇸 Сервер USD", "server_eud": "🇪🇺 Сервер EUD", "server_rud": "🇷🇺 Сервер RUD", "server_chd": "🇨🇳 Сервер CHD"},
            "en": {"server_usd": "🇺🇸 Server USD", "server_eud": "🇪 Server EUD", "server_rud": "🇷 Server RUD", "server_chd": "🇨🇳 Server CHD"},
            "zh": {"server_usd": "🇺🇸 服务器 USD", "server_eud": "🇪🇺 服务器 EUD", "server_rud": "🇷 服务器 RUD", "server_chd": "🇨🇳 服务器 CHD"}
        },
        "server_version": {
            "ru": {"ver_1272": "📦 1.2.7.2", "ver_1281": "🚀 1.2.8.1"},
            "en": {"ver_1272": "📦 1.2.7.2", "ver_1281": "🚀 1.2.8.1"},
            "zh": {"ver_1272": "📦 1.2.7.2", "ver_1281": "🚀 1.2.8.1"}
        },
        "vr_device": {
            "ru": {"vr_quest2": "🔵 Meta Quest 2", "vr_quest3": "🔵 Meta Quest 3/3s", "vr_pico4": "🟣 Pico 4", "vr_pico4ultra": "🟣 Pico 4 Ultra/Ultra Enterprise"},
            "en": {"vr_quest2": "🔵 Meta Quest 2", "vr_quest3": "🔵 Meta Quest 3/3s", "vr_pico4": "🟣 Pico 4", "vr_pico4ultra": "🟣 Pico 4 Ultra/Ultra Enterprise"},
            "zh": {"vr_quest2": "🔵 Meta Quest 2", "vr_quest3": "🔵 Meta Quest 3/3s", "vr_pico4": "🟣 Pico 4", "vr_pico4ultra": "🟣 Pico 4 Ultra/Ultra Enterprise"}
        },
        "duration": {
            "ru": {"dur_1": "1 день", "dur_3": "3 дня", "dur_5": "5 дней", "dur_7": "7 дней", "dur_10": "10 дней", "dur_14": "14 дней"},
            "en": {"dur_1": "1 day", "dur_3": "3 days", "dur_5": "5 days", "dur_7": "7 days", "dur_10": "10 days", "dur_14": "14 days"},
            "zh": {"dur_1": "1 天", "dur_3": "3 天", "dur_5": "5 天", "dur_7": "7 天", "dur_10": "10 天", "dur_14": "14 天"}
        },
        "comment": {
            "ru": "✏️ Добавить комментарий",
            "en": "✏️ Add comment",
            "zh": "✏️ 添加评论"
        },
        "back": {
            "ru": "⬅️ Назад",
            "en": "⬅️ Back",
            "zh": "⬅️ 返回"
        }
    }
}

AREA_SIZES_GLOBAL = ["4x8", "6x6", "8x8", "9x6", "10x7", "10x10", "10x12", "10x15"]
AREA_SIZES_CHD = ["4x8", "6x6", "7x15", "8x8", "8x12", "9x6", "10x7", "10x10", "10x12", "10x15"]

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)


class Form(StatesGroup):
    language = State()
    server_type = State()
    server_version = State()
    area_size = State()
    vr_device = State()
    partner_contact = State()
    partner_name = State()
    partner_phone = State()
    partner_email = State()
    partner_crm = State()
    city = State()
    duration = State()
    comment = State()


# === ПЕРЕВОД ===
def translate_to_russian(text: str, source_lang: str) -> str:
    """Безопасный перевод с обработкой ошибок"""
    if not text or source_lang == "ru":
        return text
    try:
        src = 'zh-CN' if source_lang == "zh" else 'en'
        translator = GoogleTranslator(source=src, target='ru')
        return translator.translate(text)
    except Exception as e:
        logger.error(f"Ошибка перевода '{text}': {e}")
        return text  # Возвращаем оригинал при ошибке


# === КЛАВИАТУРЫ ===
def get_lang_keyboard(lang_code):
    b = MESSAGES["buttons"]["lang"][lang_code]
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=b["lang_ru"], callback_data="lang_ru")],
        [types.InlineKeyboardButton(text=b["lang_en"], callback_data="lang_en")],
        [types.InlineKeyboardButton(text=b["lang_zh"], callback_data="lang_zh")]
    ])


def get_server_keyboard(lang_code):
    b = MESSAGES["buttons"]["server"][lang_code]
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=b["server_usd"], callback_data="server_usd")],
        [types.InlineKeyboardButton(text=b["server_eud"], callback_data="server_eud")],
        [types.InlineKeyboardButton(text=b["server_rud"], callback_data="server_rud")],
        [types.InlineKeyboardButton(text=b["server_chd"], callback_data="server_chd")],
        [types.InlineKeyboardButton(text=MESSAGES["buttons"]["back"][lang_code], callback_data="back")]
    ])


def get_version_keyboard(lang_code):
    b = MESSAGES["buttons"]["server_version"][lang_code]
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=b["ver_1272"], callback_data="ver_1272")],
        [types.InlineKeyboardButton(text=b["ver_1281"], callback_data="ver_1281")],
        [types.InlineKeyboardButton(text=MESSAGES["buttons"]["back"][lang_code], callback_data="back")]
    ])


def get_area_keyboard(lang_code, server_type):
    sizes = AREA_SIZES_CHD if server_type == "CHD" else AREA_SIZES_GLOBAL
    buttons = [[types.InlineKeyboardButton(text=size, callback_data=f"area_{size}")] for size in sizes]
    buttons.append([types.InlineKeyboardButton(text=MESSAGES["buttons"]["back"][lang_code], callback_data="back")])
    return types.InlineKeyboardMarkup(inline_keyboard=buttons)


def get_vr_keyboard(lang_code):
    b = MESSAGES["buttons"]["vr_device"][lang_code]
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=b["vr_quest2"], callback_data="vr_quest2")],
        [types.InlineKeyboardButton(text=b["vr_quest3"], callback_data="vr_quest3")],
        [types.InlineKeyboardButton(text=b["vr_pico4"], callback_data="vr_pico4")],
        [types.InlineKeyboardButton(text=b["vr_pico4ultra"], callback_data="vr_pico4ultra")],
        [types.InlineKeyboardButton(text=MESSAGES["buttons"]["back"][lang_code], callback_data="back")]
    ])


def get_partner_keyboard(lang_code):
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=MESSAGES["partner_contact_yes"][lang_code], callback_data="partner_yes")],
        [types.InlineKeyboardButton(text=MESSAGES["partner_contact_no"][lang_code], callback_data="partner_no")],
        [types.InlineKeyboardButton(text=MESSAGES["buttons"]["back"][lang_code], callback_data="back")]
    ])


def get_duration_keyboard(lang_code):
    b = MESSAGES["buttons"]["duration"][lang_code]
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=b["dur_1"], callback_data="dur_1")],
        [types.InlineKeyboardButton(text=b["dur_3"], callback_data="dur_3")],
        [types.InlineKeyboardButton(text=b["dur_5"], callback_data="dur_5")],
        [types.InlineKeyboardButton(text=b["dur_7"], callback_data="dur_7")],
        [types.InlineKeyboardButton(text=b["dur_10"], callback_data="dur_10")],
        [types.InlineKeyboardButton(text=b["dur_14"], callback_data="dur_14")],
        [types.InlineKeyboardButton(text=MESSAGES["buttons"]["back"][lang_code], callback_data="back")]
    ])


def get_comment_keyboard(lang_code):
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=MESSAGES["buttons"]["comment"][lang_code], callback_data="add_comment")],
        [types.InlineKeyboardButton(text=MESSAGES["send_without_comment"][lang_code], callback_data="send_without_comment")],
        [types.InlineKeyboardButton(text=MESSAGES["buttons"]["back"][lang_code], callback_data="back")]
    ])


def back_keyboard(lang_code):
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=MESSAGES["buttons"]["back"][lang_code], callback_data="back")]
    ])


# === ОБРАБОТЧИКИ ===
@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    logger.info(f"Команда /start от пользователя {message.from_user.id}")
    
    if message.chat.type != "private":
        bot_info = await bot.get_me()
        builder = InlineKeyboardBuilder()
        msg_text = (
            "🤖 Заполнение формы доступно только в личном чате с ботом.\n\n"
            "🤖 Form submission is only available in a private chat with the bot.\n\n"
            "🤖 表单填写仅限与机器人私聊。"
        )
        builder.button(text="Contact the bot", url=f"https://t.me/{bot_info.username}")
        await message.answer(msg_text, reply_markup=builder.as_markup(), disable_web_page_preview=True)
        return

    await state.clear()
    await message.answer(MESSAGES["start_choose_lang"], reply_markup=get_lang_keyboard("ru"))
    await state.set_state(Form.language)


@dp.callback_query(lambda c: c.data.startswith("lang_"))
async def process_language(callback: types.CallbackQuery, state: FSMContext):
    lang_map = {"lang_ru": "ru", "lang_en": "en", "lang_zh": "zh"}
    lang_code = lang_map.get(callback.data)
    if not lang_code:
        logger.warning(f"Неверный выбор языка: {callback.data}")
        await callback.answer("Ошибка выбора языка", show_alert=True)
        return
    await state.update_data(language=lang_code)
    await callback.message.edit_text(MESSAGES["choose_server"][lang_code], reply_markup=get_server_keyboard(lang_code))
    await state.set_state(Form.server_type)
    await callback.answer()


@dp.callback_query(lambda c: c.data.startswith("server_"))
async def process_server_type(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    lang_code = data.get("language", "en")
    server_map = {
        "server_usd": ("USD", TOPIC_IDS["global"]),
        "server_eud": ("EUD", TOPIC_IDS["global"]),
        "server_rud": ("RUD", TOPIC_IDS["russia_sng"]),
        "server_chd": ("CHD", TOPIC_IDS["china"]),
    }
    server_info = server_map.get(callback.data)
    if not server_info:
        logger.warning(f"Неверный выбор сервера: {callback.data}")
        await callback.answer("Ошибка выбора сервера", show_alert=True)
        return
    server_type, topic_id = server_info
    await state.update_data(server_type=server_type, topic_id=topic_id)
    await callback.message.edit_text(MESSAGES["ask_server_version"][lang_code], reply_markup=get_version_keyboard(lang_code))
    await state.set_state(Form.server_version)
    await callback.answer()


@dp.callback_query(lambda c: c.data.startswith("ver_"))
async def process_server_version(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    lang_code = data.get("language", "en")
    version = {"ver_1272": "1.2.7.2", "ver_1281": "1.2.8.1"}.get(callback.data)
    if not version:
        logger.warning(f"Неверный выбор версии: {callback.data}")
        await callback.answer("Ошибка выбора версии", show_alert=True)
        return
    await state.update_data(server_version=version)
    server_type = data.get("server_type")
    await callback.message.edit_text(MESSAGES["ask_area"][lang_code], reply_markup=get_area_keyboard(lang_code, server_type))
    await state.set_state(Form.area_size)
    await callback.answer()


@dp.callback_query(lambda c: c.data.startswith("area_"))
async def process_area_size(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    lang_code = data.get("language", "en")
    area_size = callback.data.replace("area_", "")
    await state.update_data(area_size=area_size)
    await callback.message.edit_text(MESSAGES["ask_vr_device"][lang_code], reply_markup=get_vr_keyboard(lang_code))
    await state.set_state(Form.vr_device)
    await callback.answer()


@dp.callback_query(lambda c: c.data.startswith("vr_"))
async def process_vr_device(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    lang_code = data.get("language", "en")
    vr_map = {
        "vr_quest2": "Meta Quest 2",
        "vr_quest3": "Meta Quest 3/3s",
        "vr_pico4": "Pico 4",
        "vr_pico4ultra": "Pico 4 Ultra/Ultra Enterprise"
    }
    vr_device = vr_map.get(callback.data)
    if not vr_device:
        logger.warning(f"Неверный выбор VR: {callback.data}")
        await callback.answer("Ошибка выбора VR", show_alert=True)
        return
    await state.update_data(vr_device=vr_device)
    await callback.message.edit_text(MESSAGES["ask_partner_contact"][lang_code], reply_markup=get_partner_keyboard(lang_code))
    await state.set_state(Form.partner_contact)
    await callback.answer()


@dp.callback_query(lambda c: c.data == "partner_yes")
async def partner_yes(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    lang_code = data.get("language", "en")
    await callback.message.edit_text(MESSAGES["ask_partner_name"][lang_code], reply_markup=back_keyboard(lang_code))
    await state.set_state(Form.partner_name)
    await callback.answer()


@dp.callback_query(lambda c: c.data == "partner_no")
async def partner_no(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(partner_name=None, partner_phone=None, partner_email=None, partner_crm=None)
    data = await state.get_data()
    lang_code = data.get("language", "en")
    await callback.message.edit_text(MESSAGES["ask_city"][lang_code], reply_markup=back_keyboard(lang_code))
    await state.set_state(Form.city)
    await callback.answer()


@dp.message(Form.partner_name)
async def process_partner_name(message: types.Message, state: FSMContext):
    await state.update_data(partner_name=message.text.strip() or None)
    data = await state.get_data()
    lang_code = data.get("language", "en")
    await message.answer(MESSAGES["ask_partner_phone"][lang_code], reply_markup=back_keyboard(lang_code))
    await state.set_state(Form.partner_phone)


@dp.message(Form.partner_phone)
async def process_partner_phone(message: types.Message, state: FSMContext):
    await state.update_data(partner_phone=message.text.strip() or None)
    data = await state.get_data()
    lang_code = data.get("language", "en")
    await message.answer(MESSAGES["ask_partner_email"][lang_code], reply_markup=back_keyboard(lang_code))
    await state.set_state(Form.partner_email)


@dp.message(Form.partner_email)
async def process_partner_email(message: types.Message, state: FSMContext):
    await state.update_data(partner_email=message.text.strip() or None)
    data = await state.get_data()
    lang_code = data.get("language", "en")
    await message.answer(MESSAGES["ask_partner_crm"][lang_code], reply_markup=back_keyboard(lang_code))
    await state.set_state(Form.partner_crm)


@dp.message(Form.partner_crm)
async def process_partner_crm(message: types.Message, state: FSMContext):
    await state.update_data(partner_crm=message.text.strip() or None)
    data = await state.get_data()
    lang_code = data.get("language", "en")
    await message.answer(MESSAGES["ask_city"][lang_code], reply_markup=back_keyboard(lang_code))
    await state.set_state(Form.city)


@dp.message(Form.city)
async def process_city(message: types.Message, state: FSMContext):
    await state.update_data(city=message.text.strip())
    data = await state.get_data()
    lang_code = data.get("language", "en")
    await message.answer(MESSAGES["ask_duration"][lang_code], reply_markup=get_duration_keyboard(lang_code))
    await state.set_state(Form.duration)


@dp.callback_query(lambda c: c.data.startswith("dur_"))
async def process_duration(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    lang_code = data.get("language", "en")
    duration_map = {
        "dur_1": "1", "dur_3": "3", "dur_5": "5",
        "dur_7": "7", "dur_10": "10", "dur_14": "14"
    }
    duration = duration_map.get(callback.data)
    if not duration:
        logger.warning(f"Неверный выбор срока: {callback.data}")
        await callback.answer("Ошибка выбора срока", show_alert=True)
        return
    await state.update_data(duration=duration)
    await callback.message.edit_text(MESSAGES["ask_comment"][lang_code], reply_markup=get_comment_keyboard(lang_code))
    await state.set_state(Form.comment)
    await callback.answer()


@dp.callback_query(lambda c: c.data == "add_comment")
async def ask_comment(callback: types.CallbackQuery, state: FSMContext):
    lang_code = (await state.get_data()).get("language", "en")
    await callback.message.edit_text(MESSAGES["enter_comment"][lang_code], reply_markup=back_keyboard(lang_code))
    await state.set_state(Form.comment)
    await callback.answer()


@dp.callback_query(lambda c: c.data == "send_without_comment")
async def send_without_comment(callback: types.CallbackQuery, state: FSMContext):
    """Обработка отправки без комментария — КРИТИЧЕСКИ ВАЖНО: await перед finalize_request"""
    logger.info(f"Пользователь {callback.from_user.id} выбрал отправку без комментария")
    await state.update_data(comment=None)
    try:
        # ✅ Убираем клавиатуру
        await callback.message.edit_reply_markup(reply_markup=None)
        # ✅ КРИТИЧЕСКИ: await перед finalize_request!
        await finalize_request(callback, state)
    except Exception as e:
        logger.error(f"Ошибка в send_without_comment: {e}", exc_info=True)
        await callback.answer("Произошла ошибка. Попробуйте снова.", show_alert=True)
    await callback.answer()


@dp.message(Form.comment)
async def process_comment(message: types.Message, state: FSMContext):
    """Обработка ввода комментария"""
    logger.info(f"Пользователь {message.from_user.id} ввёл комментарий")
    await state.update_data(comment=message.text.strip())
    await finalize_request(message, state)


# === ФИНАЛИЗАЦИЯ ЗАПРОСА ===
async def finalize_request(event, state: FSMContext):
    """Финализация запроса с полной обработкой ошибок"""
    data = await state.get_data()
    lang_code = data.get("language", "en")
    
    try:
        # Определяем пользователя и тип события
        is_callback = isinstance(event, types.CallbackQuery)
        user = event.from_user if is_callback else event.from_user
        user_id = user.id
        username = user.username
        first_name = user.first_name
        last_name = user.last_name

        server_type = data.get("server_type")
        server_version = data.get("server_version")
        vr_device = data.get("vr_device")
        area_size = data.get("area_size")
        city = data.get("city")
        duration = data.get("duration")
        topic_id = data.get("topic_id")
        comment = data.get("comment")
        partner_name = data.get("partner_name")
        partner_phone = data.get("partner_phone")
        partner_email = data.get("partner_email")
        partner_crm = data.get("partner_crm")

        # ✅ Безопасный перевод с обработкой ошибок
        try:
            city_ru = translate_to_russian(city, lang_code) if city else ""
            comment_ru = translate_to_russian(comment, lang_code) if comment else None
        except Exception as e:
            logger.error(f"Ошибка перевода: {e}")
            city_ru = city or ""
            comment_ru = comment

        # Формируем сообщение
        final_msg = (
            f"Прошу включить {server_type} сервер, для страны / города: {city_ru}.\n"
            f"Игровая зона с размером {area_size} метров.\n"
            f"Версия сервера: {server_version}.\n"
            f"Партнер использует {vr_device}.\n"
            f"Срок демо показа: {duration} дня(ей).\n"
        )

        partner_lines = []
        if partner_name:
            partner_lines.append(f"Контакт (Имя): {partner_name}")
        if partner_phone:
            partner_lines.append(f"Номер телефона: {partner_phone}")
        if partner_email:
            partner_lines.append(f"Email: {partner_email}")
        if partner_crm:
            partner_lines.append(f"Ссылка на CRM: {partner_crm}")
        if partner_lines:
            final_msg += "\n" + "\n".join(partner_lines) + "\n"

        if comment_ru:
            final_msg += f"\n💬 Комментарий: {comment_ru}"

        user_info = f"\n\n👤 Запрос отправлен пользователем: {first_name}"
        if last_name:
            user_info += f" {last_name}"
        if username:
            user_info += f" (@{username})"
        user_info += f" (ID: {user_id})"
        if lang_code == "zh":
            user_info += " (на китайском языке)"
        elif lang_code == "en":
            user_info += " (на английском языке)"

        final_msg += user_info

        if lang_code in ["en", "zh"]:
            final_msg += "\n\n🌐 Сообщение автоматически переведено на русский"

        # ✅ Отправка в топик с обработкой ошибок
        try:
            sent_message = await bot.send_message(
                chat_id=MAIN_CHAT_ID, 
                text=final_msg, 
                message_thread_id=topic_id
            )
            msg_id = sent_message.message_id
            chat_id_short = str(MAIN_CHAT_ID).replace("-100", "")
            link = f"https://t.me/c/{chat_id_short}/{msg_id}?thread={topic_id}"
            logger.info(f"Сообщение отправлено в чат, ссылка: {link}")
        except Exception as e:
            logger.error(f"Ошибка отправки в чат: {e}", exc_info=True)
            link = "#"
            msg_id = "unknown"

        # ✅ Сохранение в БД с обработкой ошибок
        try:
            expires_at = datetime.now() + timedelta(days=int(duration))
            request_data = {
                "user_id": user_id,
                "language": lang_code,
                "server_type": server_type,
                "area_size": area_size,
                "vr_device": vr_device,
                "duration": int(duration),
                "city": city,
                "topic_id": topic_id
            }
            save_request(request_data, link, expires_at.strftime("%Y-%m-%d %H:%M:%S"))
            logger.info(f"Запрос сохранён в БД для пользователя {user_id}")
        except Exception as e:
            logger.error(f"Ошибка сохранения в БД: {e}", exc_info=True)

        # ✅ ОТПРАВКА ФИНАЛЬНОГО СООБЩЕНИЯ ПОЛЬЗОВАТЕЛЮ
        success_msg = MESSAGES["success_with_link"][lang_code].format(link=link)
        
        try:
            if is_callback:
                # Для callback - отправляем НОВОЕ сообщение пользователю
                await bot.send_message(
                    chat_id=event.from_user.id,
                    text=success_msg,
                    parse_mode="HTML"
                )
                # ✅ Удаляем сообщение с кнопками, чтобы не дублировать
                await callback.message.delete()
            else:
                # Для message - отвечаем на сообщение
                await event.answer(
                    success_msg,
                    parse_mode="HTML"
                )
        except Exception as e:
            logger.error(f"Ошибка отправки подтверждения: {e}", exc_info=True)
            # Пробуем отправить без parse_mode как запасной вариант
            try:
                plain_msg = f"✅ Запрос успешно оформлен! Ссылка: {link}"
                if is_callback:
                    await bot.send_message(chat_id=event.from_user.id, text=plain_msg)
                else:
                    await event.answer(plain_msg)
            except Exception as e2:
                logger.error(f"Не удалось отправить подтверждение даже в упрощённом виде: {e2}")

        # ✅ Очищаем состояние
        await state.clear()
        logger.info(f"Запрос успешно завершён для пользователя {user_id}")
        
    except Exception as e:
        logger.error(f"Критическая ошибка в finalize_request: {e}", exc_info=True)
        # Пробуем очистить состояние даже при ошибке
        try:
            await state.clear()
        except:
            pass
        # Уведомляем пользователя
        try:
            error_msg = "❌ Произошла ошибка при обработке запроса. Попробуйте снова /start"
            if isinstance(event, types.CallbackQuery):
                await bot.send_message(chat_id=event.from_user.id, text=error_msg)
            else:
                await event.answer(error_msg)
        except:
            pass


# === ОБРАБОТКА КНОПКИ НАЗАД ===
@dp.callback_query(lambda c: c.data == "back")
async def process_back(callback: types.CallbackQuery, state: FSMContext):
    logger.info(f"Пользователь {callback.from_user.id} нажал кнопку 'Назад'")
    current_state = await state.get_state()
    data = await state.get_data()
    lang_code = data.get("language", "ru")
    server_type = data.get("server_type")

    if current_state == Form.server_version:
        await callback.message.edit_text(MESSAGES["choose_server"][lang_code], reply_markup=get_server_keyboard(lang_code))
        await state.set_state(Form.server_type)
    elif current_state == Form.area_size:
        await callback.message.edit_text(MESSAGES["ask_server_version"][lang_code], reply_markup=get_version_keyboard(lang_code))
        await state.set_state(Form.server_version)
    elif current_state == Form.vr_device:
        await callback.message.edit_text(MESSAGES["ask_area"][lang_code], reply_markup=get_area_keyboard(lang_code, server_type))
        await state.set_state(Form.area_size)
    elif current_state == Form.partner_contact:
        await callback.message.edit_text(MESSAGES["ask_vr_device"][lang_code], reply_markup=get_vr_keyboard(lang_code))
        await state.set_state(Form.vr_device)
    elif current_state == Form.partner_name:
        await callback.message.edit_text(MESSAGES["ask_partner_contact"][lang_code], reply_markup=get_partner_keyboard(lang_code))
        await state.set_state(Form.partner_contact)
    elif current_state == Form.partner_phone:
        await callback.message.edit_text(MESSAGES["ask_partner_name"][lang_code], reply_markup=back_keyboard(lang_code))
        await state.set_state(Form.partner_name)
    elif current_state == Form.partner_email:
        await callback.message.edit_text(MESSAGES["ask_partner_phone"][lang_code], reply_markup=back_keyboard(lang_code))
        await state.set_state(Form.partner_phone)
    elif current_state == Form.partner_crm:
        await callback.message.edit_text(MESSAGES["ask_partner_email"][lang_code], reply_markup=back_keyboard(lang_code))
        await state.set_state(Form.partner_email)
    elif current_state == Form.city:
        if data.get("partner_name") is not None:
            await callback.message.edit_text(MESSAGES["ask_partner_crm"][lang_code], reply_markup=back_keyboard(lang_code))
            await state.set_state(Form.partner_crm)
        else:
            await callback.message.edit_text(MESSAGES["ask_partner_contact"][lang_code], reply_markup=get_partner_keyboard(lang_code))
            await state.set_state(Form.partner_contact)
    elif current_state == Form.duration:
        await callback.message.edit_text(MESSAGES["ask_city"][lang_code], reply_markup=back_keyboard(lang_code))
        await state.set_state(Form.city)
    elif current_state == Form.comment:
        await callback.message.edit_text(MESSAGES["ask_duration"][lang_code], reply_markup=get_duration_keyboard(lang_code))
        await state.set_state(Form.duration)
    elif current_state == Form.server_type:
        await callback.message.edit_text(MESSAGES["start_choose_lang"], reply_markup=get_lang_keyboard("ru"))
        await state.set_state(Form.language)
    else:
        # Если состояние неизвестно — начинаем заново
        await callback.message.edit_text(MESSAGES["start_choose_lang"], reply_markup=get_lang_keyboard("ru"))
        await state.set_state(Form.language)

    await callback.answer()


# === ОБРАБОТКА НЕИЗВЕСТНЫХ СООБЩЕНИЙ ===
@dp.message()
async def handle_unknown_state(message: types.Message, state: FSMContext):
    """Обработка сообщений, когда бот в неожиданном состоянии"""
    current_state = await state.get_state()
    if current_state:
        logger.warning(f"Получено сообщение в состоянии {current_state} от {message.from_user.id}")
        await message.answer(
            "🔄 Похоже, произошла ошибка. Начните заново: /start",
            reply_markup=types.ReplyKeyboardRemove()
        )
        await state.clear()


# === HEALTH CHECK ===
@dp.message(Command("ping"))
async def cmd_ping(message: types.Message):
    """Проверка работоспособности бота"""
    await message.answer("🟢 Бот работает нормально!")
    logger.info(f"Health check от пользователя {message.from_user.id}")


# === MAIN ===
async def main():
    logger.info("Запуск бота...")
    init_db()
    logger.info("База данных инициализирована")
    await dp.start_polling(bot)


if __name__ == "__main__":
    logger.info("=== Бот запущен ===")
    asyncio.run(main())
