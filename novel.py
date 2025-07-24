import telegram.error
from telegram import (
    Bot,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputFile,
    InputMediaAnimation,
    InputMediaAudio,
    InputMediaDocument,
    InputMediaPhoto,
    InputMediaVideo,
    InlineQueryResultArticle,
    InputTextMessageContent,
    Message,
    Update,
    CallbackQuery,    
)
from telegram import WebAppInfo
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, TelegramError, TimedOut 
from telegram.ext import (
    Application,
    CallbackContext,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    InlineQueryHandler,
    MessageHandler,
    filters,
)
from telegram.helpers import escape, mention_html

import datetime
import asyncio
import colorsys
import copy
import html
import json
import logging
import math
import os
import random
import re
import tempfile
import time
from asyncio import create_task, sleep
from collections import defaultdict
from datetime import datetime
from datetime import timezone, timedelta
import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
import uuid
from uuid import uuid4

import firebase_admin
from firebase_admin import credentials, db


import networkx as nx

import graphviz as gv

from background import keep_alive

from google import genai
from google.genai import types
from google.genai.types import (
    FunctionDeclaration,
    GenerateContentConfig,
    GoogleSearch,
    Part,
    Retrieval,
    SafetySetting,
    Tool,
)


GOOGLE_API_KEY = "AIzaSyCJ9lom_jgT-SUHGG-UYrrcpuWn7s8081g"

client = genai.Client(api_key=GOOGLE_API_KEY)
# Настройка логирования для отладки
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logging.getLogger("httpx").setLevel(logging.WARNING) # Уменьшает спам от http запросов
logger = logging.getLogger(__name__)

# --- Константы ---
BOT_TOKEN = "7553491252:AAFwKa2WzZ6wKMVUIGt18oxCGPNqvSo5oRA"  # <-- ЗАМЕНИ НА СВОЙ ТОКЕН БОТА

def init_firebase():
    """
    Инициализирует приложение Firebase, если оно еще не было инициализировано.
    """
    try:
        firebase_admin.get_app()
    except ValueError:
        cred = credentials.Certificate('/etc/secrets/firebase-key.json')
        firebase_admin.initialize_app(cred, {
            'databaseURL': 'https://otlzhka-default-rtdb.europe-west1.firebasedatabase.app/'
        })

# Состояния для ConversationHandler (создание истории)
# Существующие состояния + новое
ASK_TITLE, ADD_CONTENT, ASK_CONTINUE_TEXT, ASK_BRANCH_TEXT, EDIT_STORY_MAP, \
ASK_LINK_TEXT, SELECT_LINK_TARGET, SELECT_CHOICE_TO_EDIT, AWAITING_NEW_CHOICE_TEXT, \
ASK_NEW_BRANCH_NAME, REORDER_CHOICE_SELECT_ITEM, REORDER_CHOICE_SELECT_POSITION, NEURAL_INPUT, COOP_ADD_USER, COOP_DELETE_USER, ADMIN_UPLOAD = range(16) # Добавлено ASK_NEW_BRANCH_NAME
EDIT_FRAGMENT_DATA = "edit_fragment_data"
# --- Функции для работы с данными ---

MAKE_PUBLIC_PREFIX = "mk_pub_"
MAKE_PRIVATE_PREFIX = "mk_priv_"
DOWNLOAD_STORY_PREFIX = "dl_story_"
# Добавьте эти константы вместе с другими определениями состояний
# (например, в начало вашего файла)
REORDER_CHOICE_SELECT_ITEM = "RE_C_S_I"
REORDER_CHOICE_SELECT_POSITION = "RE_C_S_P"

# Префиксы для callback_data для удобства
REORDER_CHOICE_ITEM_PREFIX = "re_i_"
REORDER_CHOICE_POSITION_PREFIX = "re_p_"
REORDER_CHOICE_CANCEL = "re_c_c"
REORDER_CHOICES_START_PREFIX = "re_c_s_"

ENABLE_NEURO_MODE_PREFIX = 'e_neuro_'
DISABLE_NEURO_MODE_PREFIX = 'd_neuro_'

active_votes = {}

DEFAULT_FILE_ID = "AgACAgIAAxkBAAIii2goP0dta_zNlsSNOwTaejMUOrfZAAJ58zEbN2RASXcAAfln8-X2ygEAAwIAA3gAAzYE" # Ваш ID файла по умолчанию
VOTE_THRESHOLDS = [1, 2, 3, 5, 7, 10, 15, 20, 35, 60, 100] # Пороги для голосования

# Эта константа нужна для handle_single_choice_selection, если она используется.
# Если эта логика не нужна, можно убрать и ее, и обработчик.
#SINGLE_CHOICE_DELAY_SECONDS = 5 # Пример значения в секундах

# --- Вспомогательная функция для отображения фрагмента (будет вызываться из разных мест) ---
MEDIA_TYPES = {"photo", "video", "animation", "audio"}





# Замените на ваш ID администратора
ADMIN_USER_ID = 6217936347
logger = logging.getLogger(__name__)

async def delete_inline_stories(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("У вас нет прав на выполнение этой команды.")
        return

    try:
        
        ref = db.reference('story_settings')
        all_stories = ref.get()

        if not all_stories:
            await update.message.reply_text("Нет активных историй для удаления.")
            return

        now = datetime.datetime.now(timezone.utc)
        deleted_count = 0

        for inline_message_id, story_data in all_stories.items():
            launch_info = story_data.get("launch_time")
            
            # Удаляем истории без ключа launch_time или без iso_timestamp_utc
            if not launch_info or "iso_timestamp_utc" not in launch_info:
                logger.info(f"Удаление истории {inline_message_id}: отсутствует launch_time или iso_timestamp_utc")
                db.reference(f'story_settings/{inline_message_id}').delete()
                deleted_count += 1
                continue

            # Пытаемся разобрать timestamp
            timestamp_str = launch_info.get("iso_timestamp_utc")
            try:
                launch_time = datetime.datetime.fromisoformat(timestamp_str)
                if launch_time.tzinfo is None:
                    launch_time = launch_time.replace(tzinfo=timezone.utc)
            except Exception as e:
                logger.warning(f"Некорректный формат времени для {inline_message_id}: {e}, запись будет удалена.")
                db.reference(f'story_settings/{inline_message_id}').delete()
                deleted_count += 1
                continue

            # Проверяем, устарела ли история
            if now - launch_time >= timedelta(weeks=2):
                logger.info(f"Удаление истории {inline_message_id}, дата запуска: {timestamp_str}")
                db.reference(f'story_settings/{inline_message_id}').delete()
                deleted_count += 1

        await update.message.reply_text(f"Удалено устаревших или некорректных историй: {deleted_count}")

    except Exception as e:
        logger.error(f"Ошибка при удалении историй: {e}")
        await update.message.reply_text("Произошла ошибка при удалении.")


COLLECTING_MEDIA = 101

# Настройка логирования, чтобы видеть ошибки
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


async def startsend_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Начинает процесс сбора медиа для рассылки."""
    # Инициализируем список для хранения сообщений в user_data
    # user_data предпочтительнее, т.к. сборка привязана к конкретному админу
    context.user_data['media_for_sending'] = []
    
    await update.message.reply_text(
        "✅ **Режим сбора медиа активирован.**\n\n"
        "Присылайте мне фото, видео, документы или текст. "
        "Они будут добавлены в очередь на рассылку.\n\n"
        "Когда закончите, используйте команду:\n"
        "`/endsend user_id1,user_id2,...`\n\n"
        "Для отмены используйте /cancelsend.",
        parse_mode='Markdown'
    )
    
    return COLLECTING_MEDIA


async def collect_media_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Собирает любое присланное сообщение (медиа или текст) в список."""
    if 'media_for_sending' not in context.user_data:
        # На случай, если что-то пошло не так
        context.user_data['media_for_sending'] = []
        
    # Просто сохраняем всё сообщение. Это самый надежный способ
    # сохранить и file_id, и caption, и media_group_id.
    context.user_data['media_for_sending'].append(update.message)
    
    count = len(context.user_data['media_for_sending'])
    await update.message.reply_text(f"📥 Добавлено в рассылку. Всего в очереди: {count}.")
    
    # Остаемся в том же состоянии, ожидая новых сообщений
    return COLLECTING_MEDIA


async def endsend_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Завершает сбор и рассылает собранный контент."""
    user_ids_str = " ".join(context.args)
    if not user_ids_str:
        await update.message.reply_text(
            "⚠️ Вы не указали ID пользователей для рассылки.\n"
            "Пример: `/nsend 12345678, 87654321`",
            parse_mode='Markdown'
        )
        return COLLECTING_MEDIA # Остаемся в состоянии сбора

    media_to_send = context.user_data.get('media_for_sending')
    if not media_to_send:
        await update.message.reply_text("⚠️ Очередь рассылки пуста. Нечего отправлять.")
        # Выходим из состояния
        context.user_data.pop('media_for_sending', None)
        return ConversationHandler.END
        
    try:
        user_ids = [int(uid.strip()) for uid in user_ids_str.split(',')]
    except ValueError:
        await update.message.reply_text("❌ Ошибка: ID пользователей должны быть целыми числами, разделенными запятой.")
        return COLLECTING_MEDIA

    await update.message.reply_text(f"🚀 Начинаю рассылку для {len(user_ids)} пользователей...")

    successful_sends = 0
    failed_sends = 0

    # Группируем сообщения по media_group_id
    grouped_messages = defaultdict(list)
    single_messages = []

    for msg in media_to_send:
        if msg.media_group_id:
            grouped_messages[msg.media_group_id].append(msg)
        else:
            single_messages.append(msg)
            
    # Объединяем все в один упорядоченный список для отправки
    ordered_send_list = []
    processed_group_ids = set()

    # Сначала добавляем одиночные сообщения и группы в том порядке, в котором они были добавлены
    for msg in media_to_send:
        if msg.media_group_id:
            if msg.media_group_id not in processed_group_ids:
                # Сортируем сообщения внутри группы по времени отправки
                group = sorted(grouped_messages[msg.media_group_id], key=lambda m: m.message_id)
                ordered_send_list.append(group)
                processed_group_ids.add(msg.media_group_id)
        else:
            ordered_send_list.append(msg)


    # Основной цикл рассылки
    for user_id in user_ids:
        try:
            for item in ordered_send_list:
                # СЛУЧАЙ 1: Это медиагруппа (представлена как список)
                if isinstance(item, list):
                    # Логика сборки медиагруппы, как в вашем `nsend`
                    media_group_list = []
                    caption = None
                    caption_entities = None
                    # Находим подпись в группе
                    for msg in item:
                        if msg.caption:
                            caption = msg.caption
                            caption_entities = msg.caption_entities
                            break
                    
                    is_first = True
                    for msg in item:
                        # Формируем InputMedia... объекты
                        current_caption = caption if is_first else None
                        current_entities = caption_entities if is_first else None
                        
                        if msg.photo:
                            media_group_list.append(InputMediaPhoto(media=msg.photo[-1].file_id, caption=current_caption, caption_entities=current_entities))
                        elif msg.video:
                            media_group_list.append(InputMediaVideo(media=msg.video.file_id, caption=current_caption, caption_entities=current_entities))
                        elif msg.document:
                            media_group_list.append(InputMediaDocument(media=msg.document.file_id, caption=current_caption, caption_entities=current_entities))
                        elif msg.audio:
                             media_group_list.append(InputMediaAudio(media=msg.audio.file_id, caption=current_caption, caption_entities=current_entities))
                        is_first = False

                    if media_group_list:
                         await context.bot.send_media_group(chat_id=user_id, media=media_group_list)

                # СЛУЧАЙ 2: Это обычное сообщение
                else:
                    await context.bot.copy_message(
                        chat_id=user_id,
                        from_chat_id=item.chat_id,
                        message_id=item.message_id
                    )
                await asyncio.sleep(0.1) # Пауза между отправками, чтобы не попасть в лимиты
            
            successful_sends += 1
        except Exception as e:
            logger.error(f"Не удалось отправить рассылку пользователю {user_id}: {e}")
            failed_sends += 1

    await update.message.reply_text(
        f"✅ **Рассылка завершена!**\n\n"
        f"Успешно отправлено: {successful_sends} пользователям\n"
        f"Не удалось отправить: {failed_sends} пользователям"
    )

    # Очищаем данные и выходим из состояния
    context.user_data.pop('media_for_sending', None)
    return ConversationHandler.END


async def cancelsend_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отменяет процесс сбора медиа и очищает очередь."""
    context.user_data.pop('media_for_sending', None)
    await update.message.reply_text("❌ Сбор медиа отменен. Очередь очищена.")
    return ConversationHandler.END





async def admin_upload_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Пожалуйста, отправьте JSON-файл для загрузки в Firebase.")
    return ADMIN_UPLOAD
def convert_choices_in_story(data):
    """
    Рекурсивно обходит структуру и преобразует все поля 'choices' из dict в list[{"text": ..., "target": ...}]
    """
    if isinstance(data, dict):
        for key, value in data.items():
            if key == "choices" and isinstance(value, dict):
                data[key] = [{"text": k, "target": v} for k, v in value.items()]
            else:
                convert_choices_in_story(value)
    elif isinstance(data, list):
        for item in data:
            convert_choices_in_story(item)
    return data

async def handle_admin_json_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    document = update.message.document
    if not document or not document.file_name.endswith('.json'):
        await update.message.reply_text("Это не JSON-файл. Пожалуйста, отправьте корректный файл.")
        return ADMIN_UPLOAD

    file = await document.get_file()
    tmp_dir = Path(tempfile.gettempdir())
    file_path = tmp_dir / f"{file.file_id}.json"

    await file.download_to_drive(str(file_path))

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # --- Добавить owner_id ко всем историям ---
        users_story = data.get("users_story", {})
        for user_id, stories in users_story.items():
            for story_id, story_data in stories.items():
                if isinstance(story_data, dict):
                    story_data["owner_id"] = user_id

        # --- Найти повторяющиеся story_id в users_story ---
        story_to_users = defaultdict(list)
        for user_id, stories in users_story.items():
            for story_id in stories:
                story_to_users[story_id].append(user_id)

        repeated_stories = {sid: uids for sid, uids in story_to_users.items() if len(uids) > 1}

        html_result = ""
        if repeated_stories:
            html_result = "<b>Повторяющиеся story_id в users_story:</b>\n\n"
            for sid, user_ids in repeated_stories.items():
                html_result += f"<code>{sid}</code> — используется у пользователей: {', '.join(user_ids)}\n"
        else:
            html_result = "Повторяющихся story_id не найдено."

        # --- Преобразовать choices (ваша логика) ---
        converted_data = convert_choices_in_story(data)

        converted_path = tmp_dir / f"converted_{file.file_id}.json"
        with open(converted_path, 'w', encoding='utf-8') as f:
            json.dump(converted_data, f, ensure_ascii=False, indent=2)

        with open(converted_path, 'rb') as f:
            await update.message.reply_document(
                document=InputFile(f, filename=f"converted_{document.file_name}"),
                caption="Вот JSON с обновлёнными choices и owner_id."
            )

        await update.message.reply_html(html_result)

        return ConversationHandler.END

    except Exception as e:
        await update.message.reply_text(f"Ошибка при обработке: {e}")
        return ADMIN_UPLOAD


def load_story_settings(inline_message_id: str) -> dict:
    """
    Загружает конкретные настройки истории по ключу inline_message_id из 'story_settings'.
    """
    try:
        
        if not firebase_admin._DEFAULT_APP_NAME:
            logger.error("Firebase приложение не инициализировано. Невозможно загрузить story_settings.")
            return {}

        ref = db.reference(f'story_settings/{inline_message_id}')
        data = ref.get()

        if data is None:
            logger.info(f"Настройки story_settings для {inline_message_id} отсутствуют. Возвращён пустой словарь.")
            return {}

        if not isinstance(data, dict):
            logger.warning(f"Настройки story_settings/{inline_message_id} имеют некорректный формат.")
            return {}

        return data
    except firebase_admin.exceptions.FirebaseError as e:
        logger.error(f"Ошибка Firebase при загрузке story_settings/{inline_message_id}: {e}")
        return {}
    except Exception as e:
        logger.error(f"Неожиданная ошибка при загрузке story_settings/{inline_message_id}: {e}")
        return {}

def load_all_user_stories(user_id_str: str) -> dict:
    """
    Загружает все истории пользователя по user_id_str.
    Не ищет среди других пользователей и не проверяет coop_edit.
    """
    try:
        
        if not firebase_admin._DEFAULT_APP_NAME:
            logger.error("Firebase приложение не инициализировано.")
            return {}

        ref = db.reference(f'users_story/{user_id_str}')
        data = ref.get()

        if data is not None and isinstance(data, dict):
            return data
        else:
            logger.info(f"У пользователя {user_id_str} не найдено историй.")
            return {}

    except firebase_admin.exceptions.FirebaseError as e:
        logger.error(f"Ошибка Firebase при загрузке историй пользователя {user_id_str}: {e}")
        return {}
    except Exception as e:
        logger.error(f"Неожиданная ошибка при загрузке историй пользователя {user_id_str}: {e}")
        return {}



def load_all_coop_stories_with_user(user_id_str: str) -> dict:
    """
    Загружает все истории всех пользователей, в которых user_id_str есть в списке coop_edit.
    """
    try:
        
        if not firebase_admin._DEFAULT_APP_NAME:
            logger.error("Firebase приложение не инициализировано.")
            return {}

        ref = db.reference('users_story')
        all_users_data = ref.get()

        if not all_users_data:
            return {}

        result = {}
        for other_user_id, stories in all_users_data.items():
            if not isinstance(stories, dict):
                continue
            for story_id, story_data in stories.items():
                coop_list = story_data.get("coop_edit", [])
                if user_id_str in coop_list:
                    result[story_id] = story_data

        return result

    except firebase_admin.exceptions.FirebaseError as e:
        logger.error(f"Ошибка Firebase при загрузке coop-историй пользователя {user_id_str}: {e}")
        return {}
    except Exception as e:
        logger.error(f"Неожиданная ошибка при загрузке coop-историй: {e}")
        return {}


def load_story_by_id_fallback(story_id: str) -> dict:
    """
    Загружает историю по её story_id, даже если пользователь не является её владельцем или coop-редактором.
    Используется для публичного или fallback-доступа (например, только для чтения).
    """
    logger.info(f"История {story_id}.")    
    try:
        
        if not firebase_admin._DEFAULT_APP_NAME:
            logger.error("Firebase приложение не инициализировано.")
            return {}

        all_users_ref = db.reference('users_story')
        all_stories = all_users_ref.get()

        if not all_stories or not isinstance(all_stories, dict):
            logger.warning("Не удалось загрузить список всех историй пользователей.")
            return {}

        for user_id, stories in all_stories.items():
            if not isinstance(stories, dict):
                continue
            story_data = stories.get(story_id)
            if story_data:
                logger.info(f"История {story_id} найдена у пользователя {user_id} через fallback-доступ.")
                return story_data

        logger.info(f"История {story_id} не найдена ни у одного пользователя.")
        return {}

    except firebase_admin.exceptions.FirebaseError as e:
        logger.error(f"Ошибка Firebase при fallback-загрузке истории {story_id}: {e}")
        return {}
    except Exception as e:
        logger.error(f"Неожиданная ошибка при fallback-загрузке истории {story_id}: {e}")
        return {}


def load_user_story(user_id_str: str, story_id: str) -> dict:
    """
    Загружает историю пользователя по user_id_str и story_id.
    Если не находит напрямую, ищет среди всех историй других пользователей с coop_edit доступом.
    """
    try:
        
        if not firebase_admin._DEFAULT_APP_NAME:
            logger.error("Firebase приложение не инициализировано.")
            return {}

        # Попытка загрузить напрямую
        ref = db.reference(f'users_story/{user_id_str}/{story_id}')
        data = ref.get()

        if data is not None and isinstance(data, dict):
            return data

        # Если не нашли — ищем среди всех
        logger.info(f"История {story_id} не найдена у пользователя {user_id_str}. Пытаемся найти владельца...")

        all_users_ref = db.reference('users_story')
        all_stories = all_users_ref.get()

        if not all_stories or not isinstance(all_stories, dict):
            logger.warning("Не удалось загрузить список всех историй пользователей.")
            return {}

        for possible_owner_id, stories in all_stories.items():
            if not isinstance(stories, dict):
                continue
            story_data = stories.get(story_id)
            if story_data:
                try:
                    actual_owner = get_owner_id_or_raise(int(user_id_str), story_id, story_data)
                    logger.info(f"История {story_id} найдена у пользователя {actual_owner} с правом coop_edit для {user_id_str}.")
                    return story_data
                except PermissionError:
                    continue

        logger.info(f"История {story_id} недоступна для пользователя {user_id_str} ни напрямую, ни по coop_edit.")
        return {}

    except firebase_admin.exceptions.FirebaseError as e:
        logger.error(f"Ошибка Firebase при загрузке истории {story_id}: {e}")
        return {}
    except Exception as e:
        logger.error(f"Неожиданная ошибка при загрузке истории {story_id}: {e}")
        return {}


def load_data() -> dict:
    """
    Загружает данные из Firebase Realtime Database и гарантирует наличие
    ключей 'users_story' и 'story_settings' в возвращаемом словаре.
    """
    try:
        
        if not firebase_admin._DEFAULT_APP_NAME: # Проверка, инициализировано ли приложение Firebase
            logger.error("Firebase приложение не инициализировано. Невозможно загрузить данные.")
            return {"users_story": {}, "story_settings": {}}

        ref = db.reference('/')
        data = ref.get()

        if data is None:
            logger.info("База данных Firebase пуста или нет данных в корне. Возвращена пустая структура.")
            return {"users_story": {}, "story_settings": {}}

        if not isinstance(data, dict):
            logger.error(f"Данные в корне Firebase не являются словарем (тип: {type(data)}). Возвращена пустая структура.")
            return {"users_story": {}, "story_settings": {}}

        # Обеспечение наличия ключей в возвращаемом словаре
        if "users_story" not in data or not isinstance(data.get("users_story"), dict):
            logger.warning("Ключ 'users_story' отсутствует в Firebase или имеет неверный тип. Инициализирован пустым словарем в возвращаемых данных.")
            data["users_story"] = {}
        if "story_settings" not in data or not isinstance(data.get("story_settings"), dict):
            logger.warning("Ключ 'story_settings' отсутствует в Firebase или имеет неверный тип. Инициализирован пустым словарем в возвращаемых данных.")
            data["story_settings"] = {}
        
        # logger.debug("Данные успешно загружены из Firebase.") # Можно включить для детального логирования
        return data
    except firebase_admin.exceptions.FirebaseError as e:
        logger.error(f"Ошибка Firebase при чтении данных: {e}. Возвращена пустая структура.")
        return {"users_story": {}, "story_settings": {}}
    except Exception as e: # Другие возможные ошибки (сетевые и т.д.)
        logger.error(f"Неожиданная ошибка при загрузке данных из Firebase: {e}. Возвращена пустая структура.")
        return {"users_story": {}, "story_settings": {}}

def save_story_data(user_id_str: str, story_id: str, story_content: dict):
    """
    Сохраняет данные конкретной истории для конкретного пользователя
    в Firebase Realtime Database по пути 'users_story/{user_id_str}/{story_id}'.
    Предварительно загружает актуальные данные, чтобы избежать перезаписи
    изменений, внесённых параллельно другим пользователем.
    """
    try:
        
        if not firebase_admin._DEFAULT_APP_NAME:
            logger.error("Firebase приложение не инициализировано. Невозможно сохранить данные истории.")
            return

        ref = db.reference(f'users_story/{user_id_str}/{story_id}')
        
        # Загрузка актуальных данных перед сохранением
        current_data = ref.get()
        if current_data is None:
            current_data = {}

        # Обновление данных — можно заменить на более интеллектуальное слияние
        current_data.update(story_content)

        # Сохраняем объединённые данные
        ref.set(current_data)

        logger.info(f"Актуализированные данные для истории {story_id} пользователя {user_id_str} сохранены в Firebase.")
    except firebase_admin.exceptions.FirebaseError as e:
        logger.error(f"Ошибка Firebase при сохранении данных истории {story_id} для пользователя {user_id_str}: {e}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка при сохранении данных истории в Firebase: {e}")

def save_current_story_from_context(context: ContextTypes.DEFAULT_TYPE):
    """
    Извлекает данные текущей истории из user_data и сохраняет их в Firebase,
    используя save_story_data_firebase.
    """
    if 'user_id_str' in context.user_data and \
       'story_id' in context.user_data and \
       'current_story' in context.user_data:

        user_id = context.user_data['user_id_str']
        story_id = context.user_data['story_id']
        story_data = context.user_data['current_story']
        save_story_data(user_id, story_id, story_data)
    else:
        logger.warning("Попытка сохранить текущую историю из контекста, но не все данные найдены в context.user_data (user_id_str, story_id, current_story).")


def save_story_bookmark(user_id_str: str, story_id: str, bookmark_data: dict):
    """
    Сохраняет одну заметку для истории.
    Использует push() для генерации уникального ID.
    bookmark_data должен содержать 'text' и 'position'.
    """
    try:
        # Генерируем новый ключ для заметки
        ref = db.reference(f'users_story/{user_id_str}/{story_id}/bookmarks').push()
        # Сохраняем данные заметки
        ref.set(bookmark_data)
        logger.info(f"Заметка для истории {story_id} сохранена с ID: {ref.key}")
        # Возвращаем ID и данные для отправки клиенту
        return { "id": ref.key, **bookmark_data }
    except Exception as e:
        logger.error(f"Ошибка при сохранении заметки для истории {story_id}: {e}")
        return None

def load_story_bookmarks(user_id_str: str, story_id: str):
    """
    Загружает все заметки для указанной истории.
    """
    try:
        ref = db.reference(f'users_story/{user_id_str}/{story_id}/bookmarks')
        bookmarks = ref.get()
        return bookmarks if bookmarks else {}
    except Exception as e:
        logger.error(f"Ошибка при загрузке заметок для истории {story_id}: {e}")
        return None


def update_story_bookmark(user_id_str: str, story_id: str, note_id: str, new_text: str):
    """
    Обновляет текст существующей заметки.
    """
    try:
        # Указываем путь прямо к текстовому полю заметки
        ref = db.reference(f'users_story/{user_id_str}/{story_id}/bookmarks/{note_id}/text')
        ref.set(new_text)
        logger.info(f"Текст заметки {note_id} для истории {story_id} обновлен.")
        return True
    except Exception as e:
        logger.error(f"Ошибка при обновлении заметки {note_id} для истории {story_id}: {e}")
        return False

def delete_story_bookmark(user_id_str: str, story_id: str, note_id: str):
    """
    Удаляет заметку по ее ID.
    """
    try:
        # Указываем путь к корню удаляемой заметки
        ref = db.reference(f'users_story/{user_id_str}/{story_id}/bookmarks/{note_id}')
        ref.delete()
        logger.info(f"Заметка {note_id} для истории {story_id} удалена.")
        return True
    except Exception as e:
        logger.error(f"Ошибка при удалении заметки {note_id} для истории {story_id}: {e}")
        return False


def get_owner_id_or_raise(user_id: int, story_id: str, story_data: dict) -> str:
    """
    Проверяет, имеет ли пользователь доступ к данной истории.
    Возвращает user_id владельца истории (строкой) или вызывает PermissionError.
    Если story_id == "000", доступ всегда разрешён.
    """
    if story_id == "000":
        return str(user_id)

    if not story_data or not isinstance(story_data, dict):
        raise PermissionError(f"История {story_id} не найдена или повреждена.")

    owner_id_raw = story_data.get("owner_id")
    if owner_id_raw is None:
        raise PermissionError(f"История {story_id} не содержит информации о владельце.")

    try:
        owner_id = int(owner_id_raw)
    except ValueError:
        raise PermissionError(f"owner_id имеет неверный формат: {owner_id_raw}")

    coop_list_raw = story_data.get("coop_edit", [])
    coop_list = [int(uid) for uid in coop_list_raw if isinstance(uid, (str, int)) and str(uid).isdigit()]

    logging.info(f"owner_id: {owner_id}")    
    logging.info(f"user_id: {user_id}")

    if user_id == owner_id or user_id in coop_list:
        return str(owner_id)

    raise PermissionError(f"Пользователь {user_id} не имеет доступа к истории {story_id}")



def get_owner_id(story_id: str, story_data: dict) -> str:
    """
    Возвращает user_id владельца истории (строкой), без проверки прав доступа.
    Если story_id == "000", возвращает "000" как владельца.
    """
    if story_id == "000":
        return "000"

    if not story_data or not isinstance(story_data, dict):
        raise ValueError(f"История {story_id} не найдена или повреждена.")

    owner_id_raw = story_data.get("owner_id")
    if owner_id_raw is None:
        raise ValueError(f"История {story_id} не содержит информации о владельце.")

    try:
        return str(int(owner_id_raw))
    except ValueError:
        raise ValueError(f"owner_id имеет неверный формат: {owner_id_raw}")


async def delete_story_confirmed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Подтверждает и удаляет историю из Firebase.
    user_id_str и story_id извлекаются из context.user_data['delete_candidate'].
    Предполагается, что user_id_str является владельцем истории.
    """
    
    if not firebase_admin._DEFAULT_APP_NAME:
        logger.error("Firebase приложение не инициализировано. Невозможно удалить историю.")
        if update.callback_query:
            await update.callback_query.answer("Ошибка: Сервис базы данных недоступен.", show_alert=True)
        return

    query = update.callback_query
    user_id_owner, story_id_to_delete = context.user_data.get('delete_candidate', (None, None))

    if not user_id_owner or not story_id_to_delete:
        logger.warning("Ключ 'delete_candidate' не найден или не содержит полных данных в context.user_data при попытке удаления.")
        if query:
            await query.answer("Ошибка: данные для удаления истории не найдены в сессии.", show_alert=True)
        return

    story_ref = db.reference(f'users_story/{user_id_owner}/{story_id_to_delete}')

    try:
        if story_ref.get() is None:
            logger.info(f"Попытка удалить несуществующую историю: users_story/{user_id_owner}/{story_id_to_delete}")
            if query:
                await query.answer("История не найдена или уже удалена.", show_alert=True)
        else:
            story_ref.delete()
            logger.info(f"История {story_id_to_delete} пользователя {user_id_owner} удалена из Firebase.")
            if query:
                await query.answer("История удалена.", show_alert=True)
            # Показываем обновлённый список историй
            await view_stories_list(update, context)

    except firebase_admin.exceptions.FirebaseError as e:
        logger.error(f"Ошибка Firebase при удалении истории {story_id_to_delete} для пользователя {user_id_owner}: {e}")
        if query:
            await query.answer("Ошибка Firebase при удалении истории.", show_alert=True)
    except Exception as e:
        logger.error(f"Неожиданная ошибка при удалении истории {story_id_to_delete} (владелец {user_id_owner}): {e}")
        if query:
            await query.answer("Неожиданная ошибка при удалении истории.", show_alert=True)




def save_story_state_to_firebase(inline_message_id: str, story_state_data: dict):
    """
    Сохраняет полное состояние истории/голосования в Firebase.
    Добавляет или обновляет время запуска.
    """
    
    if not inline_message_id:
        logger.error("save_story_state_to_firebase: inline_message_id is required.")
        return

    ref = db.reference(f'story_settings/{inline_message_id}') # Замените db.reference реальной функцией
    
    # Получаем существующие данные, чтобы не перезаписать launch_time если оно уже есть
    existing_data = ref.get() or {}
    existing_data.update(story_state_data)
    
    if 'launch_time' not in existing_data and 'launch_time' not in story_state_data :
        now_utc = datetime.datetime.utcnow()
        story_state_data['launch_time'] = {
            'year': now_utc.year,
            'day': now_utc.day, # День месяца
            'hour': now_utc.hour,
            'minute': now_utc.minute,
            'iso_timestamp_utc': now_utc.isoformat() # Для удобства
        }
        logger.info(f"Setting initial launch_time for {inline_message_id}")
    elif 'launch_time' in existing_data and 'launch_time' not in story_state_data:
        story_state_data['launch_time'] = existing_data['launch_time'] # Сохраняем существующее время

    # Конвертируем set в list для Firebase (JSON)
    if 'poll_details' in story_state_data and story_state_data['poll_details']:
        poll_details = story_state_data['poll_details']
    
        if 'votes' in poll_details:
            votes_data = poll_details['votes']
    
            if isinstance(votes_data, dict):
                # Конвертируем dict с None в [] и ключи в строки
                story_state_data['poll_details']['votes'] = {
                    str(idx): list(user_set or [])  # None → []
                    for idx, user_set in votes_data.items()
                }
    
            elif isinstance(votes_data, list):
                # Превращаем список в словарь, гарантируя отсутствие None
                story_state_data['poll_details']['votes'] = {
                    str(i): list(v) if isinstance(v, list) else []
                    for i, v in enumerate(votes_data)
                }
    
        if 'voted_users' in poll_details and isinstance(poll_details['voted_users'], set):
            poll_details['voted_users'] = list(poll_details['voted_users'])

    logger.info(f"Saving to Firebase for {inline_message_id}: {story_state_data}")
    ref.set(story_state_data)


def update_user_attributes(inline_message_id: str, user_attributes: dict):
    """
    Обновляет только поле 'user_attributes' в story_settings/{inline_message_id}, 
    не затрагивая другие поля.
    """
    
    if not inline_message_id:
        logger.error("update_user_attributes: inline_message_id is required.")
        return

    ref = db.reference(f'story_settings/{inline_message_id}/user_attributes')
    
    try:
        logger.info(f"Updating user_attributes for {inline_message_id}: {user_attributes}")
        ref.set(user_attributes)
    except Exception as e:
        logger.error(f"Failed to update user_attributes for {inline_message_id}: {e}")

def save_story_data_to_file(all_data: dict) -> bool:
    """
    Сохраняет все предоставленные данные в корень Firebase Realtime Database
    и возвращает True в случае успеха, False в случае ошибки.
    Аналогично save_all_data_firebase, но с булевым возвратом.
    """
    try:
        
        if not firebase_admin._DEFAULT_APP_NAME:
            logger.error("Firebase приложение не инициализировано. Невозможно сохранить данные (и вернуть статус).")
            return False
            
        ref = db.reference('/')
        ref.set(all_data)
        logger.info("Все данные успешно сохранены в Firebase (с возвратом статуса).")
        return True
    except firebase_admin.exceptions.FirebaseError as e:
        logger.error(f"Ошибка Firebase при сохранении всех данных (с возвратом статуса): {e}")
        return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка при сохранении всех данных в Firebase (с возвратом статуса): {e}")
        return False


def load_story_state_from_firebase(inline_message_id: str) -> dict | None:
    """
    Загружает состояние истории/голосования из Firebase.
    Конвертирует данные из JSON-совместимых форматов обратно в нужные типы (например, list в set).
    """
    
    if not inline_message_id:
        logger.error("load_story_state_from_firebase: inline_message_id is required.")
        return None
        
    ref = db.reference(f'story_settings/{inline_message_id}') # Замените db.reference реальной функцией
    story_state = ref.get()
    if story_state:
        logger.info(f"Loaded from Firebase for {inline_message_id}: {story_state}")
        # Конвертируем обратно list в set для votes и voted_users
        if 'poll_details' in story_state and story_state['poll_details']:
            if 'votes' in story_state['poll_details']:
                votes_raw = story_state['poll_details']['votes']
                if isinstance(votes_raw, dict):
                    story_state['poll_details']['votes'] = {
                        idx_str: set(user_list)
                        for idx_str, user_list in votes_raw.items()
                    }
                else:
                    logger.warning(f"'votes' expected to be dict, got {type(votes_raw)}: {votes_raw}")
            if 'voted_users' in story_state['poll_details']:
                 story_state['poll_details']['voted_users'] = set(story_state['poll_details']['voted_users'])
        return story_state
    return None



def save_node_positions(user_id_str: str, story_id: str, positions: dict):
    """
    Сохраняет позиции узлов для карты конкретной истории.
    Использует путь 'story_maps/{user_id_str}/{story_id}'.
    Метод .set() полностью перезаписывает данные, что является
    корректным поведением для сохранения полного набора координат.
    """
    try:
        
        ref = db.reference(f'story_maps/{user_id_str}/{story_id}')
        ref.set(positions)
        logger.info(f"Позиции узлов для истории {story_id} сохранены.")
    except Exception as e:
        logger.error(f"Ошибка при сохранении позиций узлов для истории {story_id}: {e}")

def load_node_positions(user_id_str: str, story_id: str):
    """
    Загружает сохраненные позиции узлов для карты конкретной истории.
    """
    try:
        
        ref = db.reference(f'story_maps/{user_id_str}/{story_id}')
        positions = ref.get()
        return positions if positions else None
    except Exception as e:
        logger.error(f"Ошибка при загрузке позиций узлов для истории {story_id}: {e}")
        return None


#===============================================================        


import logging
import datetime # Для времени запуска
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, InputMediaVideo, InputMediaAnimation, InputMediaAudio
from telegram.ext import CallbackContext



def check_choice_requirements(effects: list, user_attributes: dict) -> list[str]:
    errors = []

    for effect in effects:
        stat = effect.get("stat")
        value = effect.get("value")
        effect_type, op, number = _parse_effect_value(value)

        if effect_type == "check":
            if stat not in user_attributes:
                errors.append(f"{stat}: отсутствует")
                continue

            current_value = user_attributes[stat]

            if op == ">" and not (current_value > number):
                errors.append(f"{stat}: требуется > {number}, текущее значение {current_value}")
            elif op == "<" and not (current_value < number):
                errors.append(f"{stat}: требуется < {number}, текущее значение {current_value}")
            elif op == "=" and not (current_value == number):
                errors.append(f"{stat}: требуется = {number}, текущее значение {current_value}")

    return errors


async def process_choice_effects_to_user_attributes(
    inline_message_id: str,
    user_id: int,
    effects_list: List[Dict[str, Any]],
    query: Optional[CallbackQuery] = None,
    context: Optional[CallbackContext] = None
) -> Tuple[bool, str, bool, Dict[str, Any]]:
    """
    Обрабатывает эффекты: set/modify, в том числе с диапазонами и модификаторами.
    """
    story_state = load_story_state_from_firebase(inline_message_id)
    user_attr = story_state.get("user_attributes", {})
    temp_user_attr = dict(user_attr)

    success_alert_parts = []
    failure_reasons = []

    for effect in effects_list:
        stat_name = effect.get("stat")
        value_str = effect.get("value", "")
        hide_effect = effect.get("hide", False)
        modifiers = effect.get("modifiers")

        action_type, op_char, parsed_value = _parse_effect_value(value_str)

        if action_type == "invalid" or not stat_name or parsed_value is None:
            logger.warning(f"Пропущен некорректный эффект: {effect}")
            continue

        # Генерируем числовое значение, если диапазон
        if action_type in ("set_range", "modify_range"):
            min_val, max_val = parsed_value
            numeric_val = _get_random_value_from_range(min_val, max_val, modifiers)
            action_type = "set" if action_type == "set_range" else "modify"
        else:
            numeric_val = parsed_value

        current_value = temp_user_attr.get(stat_name, 0)
        try:
            val_for_calc = int(current_value)
        except (ValueError, TypeError):
            logger.warning(f"Стат {stat_name} имеет нечисловое значение '{current_value}', принимаем 0.")
            val_for_calc = 0

        if action_type == "check":
            check_passed = (
                (op_char == '>' and val_for_calc > numeric_val) or
                (op_char == '<' and val_for_calc < numeric_val) or
                (op_char == '=' and val_for_calc == numeric_val)
            )
            if not check_passed:
                reason = f"Проверка не пройдена: {stat_name} {op_char}{numeric_val} (у вас: {val_for_calc})"
                if len(reason) > 200:
                    reason = reason[:197] + "..."
                failure_reasons.append(reason)
                break  # Цепочка прерывается

        elif action_type == "set":
            temp_user_attr[stat_name] = numeric_val
            if not hide_effect:
                success_alert_parts.append(f"▫️Атрибут {stat_name} установлен на {numeric_val}")

        elif action_type == "modify":
            new_val = val_for_calc + numeric_val if op_char == '+' else val_for_calc - numeric_val
            temp_user_attr[stat_name] = new_val
            if not hide_effect:
                word = "увеличен" if op_char == '+' else "уменьшен"
                success_alert_parts.append(f"▫️Атрибут {stat_name} {word} на {abs(numeric_val)}")

    # Формируем результат
    final_alert_text = "\n".join(success_alert_parts)

    if failure_reasons:
        failure_alert = "\n".join(failure_reasons)
        full_alert = f"{final_alert_text}\n\n⚠️ {failure_alert}" if final_alert_text else f"⚠️ {failure_alert}"
        return False, full_alert.strip(), True, story_state

    # Сохраняем изменения
    story_state["user_attributes"] = temp_user_attr
    save_story_state_to_firebase(inline_message_id, story_state)
    if context and inline_message_id:
        context.bot_data.setdefault(inline_message_id, {})["user_attributes"] = temp_user_attr

    return True, final_alert_text.strip(), False, story_state


def clean_caption(text: str) -> str:
    """Удаляет конструкции вида ((+2)) и [[-4]] из текста."""
    if not text:
        return ""
    cleaned = re.sub(r'\(\([+-]?\d+\)\)', '', text)
    cleaned = re.sub(r'\[\[[+-]?\d+\]\]', '', cleaned)
    return cleaned.strip()


def replace_attributes_in_text(text: str, user_attributes: dict) -> str:
    def replace_match(match):
        key = match.group(1)
        if key in user_attributes:
            return f"{key}: {user_attributes[key]}"
        else:
            return f"Атрибут ({key}) отсутствует"
    return re.sub(r"\{\{(.*?)\}\}", replace_match, text)

def deserialize_votes_from_db(votes_data) -> dict:
    """
    Безопасно конвертирует данные о голосах из Firebase (любого формата)
    в чистый словарь {индекс: множество}.
    """
    if not votes_data:
        return {}

    clean_votes = {}
    
    # Обработка нового/правильного формата: {"0": [123], "1": [456]}

    if isinstance(votes_data, dict):
        for key, user_ids in votes_data.items():
            try:
                idx = int(key)
                logger.debug(f"Обработка голосов: ключ={key}, значение={user_ids}")
                if user_ids and isinstance(user_ids, (list, set)):
                    clean_votes[idx] = {uid for uid in user_ids if uid is not None}
                else:
                    clean_votes[idx] = set()
            except (ValueError, TypeError):
                continue
        return clean_votes

    # Обработка старого/испорченного формата: [[123], null, [null, 456]]
    if isinstance(votes_data, list):
        for idx, user_ids in enumerate(votes_data):
            if user_ids and isinstance(user_ids, (list, set)):
                clean_votes[idx] = {uid for uid in user_ids if uid is not None}
            else:
                clean_votes[idx] = set()
        return clean_votes

    return {}


async def display_fragment_for_interaction(context: CallbackContext, inline_message_id: str, target_user_id_str: str, story_id: str, fragment_id: str):
    """
    Отображает фрагмент истории, обрабатывая текст, медиа и варианты выбора (голосование).
    Логика полностью сохранена, логи переработаны для ясности и краткости.
    """
    log_prefix = f"[{inline_message_id}][{story_id}][{fragment_id}]"
    logger.info(f"{log_prefix} Отображение фрагмента для пользователя {target_user_id_str}.")

    # 1. Загрузка и проверка определений истории и фрагмента
    all_data = load_data()
    story_definition = None
    for user_key, user_stories in all_data.get("users_story", {}).items():
        if story_id in user_stories:
            story_definition = user_stories[story_id]
            break

    if not story_definition:
        logger.warning(f"{log_prefix} ОШИБКА: Определение истории не найдено.")
        try:
            await context.bot.edit_message_text(inline_message_id=inline_message_id, text="История не найдена.")
        except Exception as e:
            logger.error(f"{log_prefix} Не удалось отправить сообщение об ошибке 'история не найдена': {e}")
        return

    fragment = story_definition.get("fragments", {}).get(fragment_id)
    if not fragment:
        logger.warning(f"{log_prefix} ОШИБКА: Определение фрагмента не найдено.")
        try:
            await context.bot.edit_message_text(inline_message_id=inline_message_id, text="Фрагмент не найден.")
        except Exception as e:
            logger.error(f"{log_prefix} Не удалось отправить сообщение об ошибке 'фрагмент не найден': {e}")
        return

    # 2. Инициализация переменных и состояния
    user_attributes = {}
    required_votes_for_poll = None
    current_poll_data_from_firebase = None

    # Загружаем сохраненное состояние из Firebase
    story_state_from_firebase = load_story_state_from_firebase(inline_message_id)
    logger.info(f"===== Данные загружены из firebase: {story_state_from_firebase} ")
    if story_state_from_firebase:
        logger.info(f"{log_prefix} Загружено состояние из Firebase.")
        user_attributes = story_state_from_firebase.get("user_attributes", {})
        story_id = story_state_from_firebase.get("story_id", story_id)
        target_user_id_str = story_state_from_firebase.get("target_user_id", target_user_id_str)
        required_votes_for_poll = story_state_from_firebase.get("required_votes_to_win")

        # Если в Firebase есть данные о голосовании для текущего фрагмента, восстанавливаем их
        if "poll_details" in story_state_from_firebase and story_state_from_firebase.get("current_fragment_id") == fragment_id:
            poll_details_fb = story_state_from_firebase["poll_details"]
            votes = deserialize_votes_from_db(poll_details_fb.get("votes"))
            
            voted_users_list = poll_details_fb.get("voted_users", [])
            
            current_poll_data_from_firebase = {
                "type": "poll",
                "target_user_id": story_state_from_firebase["target_user_id"],
                "story_id": story_state_from_firebase["story_id"],
                "current_fragment_id": story_state_from_firebase["current_fragment_id"],
                "choices_data": poll_details_fb.get("choices_data", []),
                "votes": votes,
                "voted_users": set(voted_users_list),
                "required_votes_to_win": story_state_from_firebase["required_votes_to_win"],
                "user_attributes": user_attributes,
            }
            context.bot_data[inline_message_id] = current_poll_data_from_firebase
            logger.info(f"===== Данные голосования восстановлены из Firebase в `context.bot_data`: {current_poll_data_from_firebase} ")

    # Проверяем, есть ли свежие данные в bot_data (например, после установки порога голосов)
    if inline_message_id in context.bot_data:
        bot_data_entry = context.bot_data[inline_message_id]
        if bot_data_entry.get("type") == "poll_setup_pending_display":
            required_votes_for_poll = bot_data_entry.get("required_votes")
            logger.info(f"{log_prefix} Установлен порог голосов из `poll_setup_pending_display`: {required_votes_for_poll}.")
        if "user_attributes" in bot_data_entry:
            user_attributes = bot_data_entry["user_attributes"]
            logger.info(f"{log_prefix} Атрибуты пользователя обновлены из `context.bot_data`.")
    
    logger.info(f"{log_prefix} Итоговые атрибуты для проверок: {user_attributes}")

    # 3. Подготовка контента сообщения
    raw_caption = fragment.get("text", "")
    caption = clean_caption(replace_attributes_in_text(raw_caption, user_attributes))[:1000]
    
    choices = fragment.get("choices", [])
    media = fragment.get("media", [])
    keyboard = []
    reply_markup = None

    # Критическая проверка: есть выборы, но не установлен порог голосов
    if len(choices) > 1 and required_votes_for_poll is None:
        logger.error(f"{log_prefix} КРИТИЧЕСКАЯ ОШИБКА: Порог голосов не установлен, но у фрагмента есть варианты выбора.")
        try:
            await context.bot.edit_message_text(inline_message_id=inline_message_id, text="Ошибка конфигурации голосования: порог не установлен.")
        except Exception as e:
            logger.error(f"{log_prefix} Не удалось отредактировать сообщение об ошибке порога: {e}")
        return

    # Логика для сохранения медиа между фрагментами
    app_data = context.application.bot_data.setdefault("fragments", {})
    previous_fragment = app_data.get(inline_message_id, {}).get("last_fragment")
    if media and isinstance(media, list): media = media[:1]
    if not media and previous_fragment:
        old_media = previous_fragment.get("media", [])
        if len(old_media) == 1 and old_media[0].get("type") == "photo":
            media = [{"type": "photo", "file_id": DEFAULT_FILE_ID}]
    fragment["media"] = media
    app_data.setdefault(inline_message_id, {})
    app_data[inline_message_id]["last_fragment"] = {"id": fragment_id, "media": media}

    # 4. Логика обработки вариантов выбора (голосования)
    if len(choices) > 0:
        # --- НАЧАЛО ИСПРАВЛЕННОГО БЛОКА ---
    
        poll_data_to_use = context.bot_data.get(inline_message_id)
    
        # Шаг 1: Проверяем, актуальны ли данные в оперативной памяти (RAM)
        is_ram_data_valid = (
            poll_data_to_use and
            poll_data_to_use.get("type") == "poll" and
            poll_data_to_use.get("current_fragment_id") == fragment_id
        )
    
        if is_ram_data_valid:
            logger.info(f"{log_prefix} Используются актуальные данные голосования из `context.bot_data` (RAM).")
        else:
            logger.info(f"{log_prefix} Данные в RAM отсутствуют или устарели. Попытка загрузки из Firebase.")
            poll_data_to_use = None # Сбрасываем неактуальные данные
    
            # Шаг 2: Если в RAM данных нет, пытаемся загрузить из Firebase
            if story_state_from_firebase and "poll_details" in story_state_from_firebase:
                poll_details_fb = story_state_from_firebase["poll_details"]
                votes = deserialize_votes_from_db(poll_details_fb.get("votes"))
                voted_users_list = poll_details_fb.get("voted_users", [])
    
                poll_data_to_use = {
                    "type": "poll",
                    "target_user_id": story_state_from_firebase["target_user_id"],
                    "story_id": story_state_from_firebase["story_id"],
                    "current_fragment_id": story_state_from_firebase["current_fragment_id"],
                    "choices_data": poll_details_fb.get("choices_data", []),
                    "votes": votes,
                    "voted_users": set(voted_users_list),
                    "required_votes_to_win": story_state_from_firebase["required_votes_to_win"],
                    "user_attributes": user_attributes,
                }
                logger.info(f"{log_prefix} Данные голосования успешно восстановлены из Firebase.")
            
            # Шаг 3: Если нигде данных нет, создаем новое голосование
            if not poll_data_to_use:
                logger.info(f"{log_prefix} Данные не найдены. Создается новое голосование.")
                poll_data_to_use = {
                    "type": "poll", "target_user_id": target_user_id_str, "story_id": story_id,
                    "current_fragment_id": fragment_id, "choices_data": [],
                    "votes": {idx: set() for idx in range(len(choices))}, "voted_users": set(),
                    "required_votes_to_win": required_votes_for_poll, "user_attributes": user_attributes
                }
                for idx, choice in enumerate(choices):
                    poll_data_to_use["choices_data"].append({
                        "text": choice["text"], "next_fragment_id": choice["target"],
                        "effects": choice.get("effects", [])
                    })
    
        # Шаг 4: Сохраняем итоговое, авторитетное состояние в RAM для последующих операций
        context.bot_data[inline_message_id] = poll_data_to_use
        
        # Формирование клавиатуры на основе проверок
        show_vote_counts = required_votes_for_poll > 1
        for idx, choice_d in enumerate(poll_data_to_use["choices_data"]):
            is_choice_available, must_hide, missing_stats = True, False, []

            for effect in choice_d.get("effects", []):
                action_type, op, num_req = _parse_effect_value(str(effect.get("value", "")))
                if action_type != "check": continue
                
                stat_name = effect.get("stat")
                if not stat_name or num_req is None: continue

                user_stat_val = user_attributes.get(stat_name)
                try: user_stat_num = int(user_stat_val)
                except (ValueError, TypeError): user_stat_num = None

                check_passed = False
                if user_stat_num is not None:
                    if op == '>': check_passed = user_stat_num > num_req
                    elif op == '<': check_passed = user_stat_num < num_req
                    elif op == '=': check_passed = user_stat_num == num_req
                
                if not check_passed:
                    is_choice_available = False
                    if effect.get("hide", False):
                        logger.info(f"{log_prefix} Выбор '{choice_d['text']}' скрыт из-за провала проверки стата '{stat_name}'.")
                        must_hide = True
                        break
                    else:
                        missing_stats.append(stat_name)
            
            if must_hide: continue

            button_text = choice_d["text"]
            if is_choice_available:
                if show_vote_counts:
                    num_votes = len(poll_data_to_use["votes"].get(idx, set()))
                    button_text = f"({num_votes}/{required_votes_for_poll}) {button_text}"
            else:
                reason = f"не хватает: {', '.join(missing_stats)}" if missing_stats else "недоступно"
                button_text = f"{button_text} ({reason})"
            
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"vote_{inline_message_id}_{idx}")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)

        # Сохранение актуального состояния голосования в Firebase
        firebase_save_data = {
            "story_id": poll_data_to_use["story_id"],
            "target_user_id": poll_data_to_use["target_user_id"],
            "current_fragment_id": poll_data_to_use["current_fragment_id"],
            "required_votes_to_win": poll_data_to_use["required_votes_to_win"],
            "user_attributes": user_attributes, # Используем атрибуты, определенные ранее в функции
            "poll_details": {
                "choices_data": poll_data_to_use["choices_data"],
                "votes": {str(k): list(v) for k, v in poll_data_to_use["votes"].items()},
                "voted_users": list(poll_data_to_use["voted_users"]),
            },
        }
        logger.info(f"{firebase_save_data} ===== Сохранение состояния (с данными голосования) в Firebase.")
        save_story_state_to_firebase(inline_message_id, firebase_save_data)
    
    else: # Нет вариантов выбора (финальный фрагмент или переход)
        caption += "\n\n(История завершена)"
        if story_state_from_firebase:
            update_required = False
            if "poll_details" in story_state_from_firebase:
                story_state_from_firebase.pop("poll_details", None)
                logger.info(f"{log_prefix} Фрагмент без выбора. Удалены данные о голосовании из Firebase.")
                update_required = True
            if story_state_from_firebase.get("current_fragment_id") != fragment_id:
                story_state_from_firebase["current_fragment_id"] = fragment_id
                update_required = True
            
            if update_required:
                logger.info(f"{log_prefix} Обновление финального состояния в Firebase.")
                save_story_state_to_firebase(inline_message_id, story_state_from_firebase)

    # 5. Отправка (редактирование) сообщения пользователю
    try:
        has_media = media and isinstance(media, list) and media[0].get("file_id")
        logger.info(f"{log_prefix} Попытка обновить сообщение: медиа={'да' if has_media else 'нет'}, клавиатура={'да' if reply_markup else 'нет'}.")
        
        if has_media:
            media_item = media[0]
            file_id, media_type = media_item.get("file_id"), media_item.get("type")
            input_media = None
            if media_type == "photo": input_media = InputMediaPhoto(media=file_id, caption=caption, parse_mode='HTML')
            elif media_type == "video": input_media = InputMediaVideo(media=file_id, caption=caption, parse_mode='HTML')
            elif media_type == "animation": input_media = InputMediaAnimation(media=file_id, caption=caption, parse_mode='HTML')
            elif media_type == "audio": input_media = InputMediaAudio(media=file_id, caption=caption, parse_mode='HTML')
            
            if input_media:
                await context.bot.edit_message_media(inline_message_id=inline_message_id, media=input_media, reply_markup=reply_markup)
                return
        
        await context.bot.edit_message_text(inline_message_id=inline_message_id, text=caption, reply_markup=reply_markup, parse_mode='HTML')

    except Exception as e:
        logger.error(f"{log_prefix} КРИТИЧЕСКАЯ ОШИБКА при обновлении сообщения: {e}")



async def handle_inline_play(update: Update, context: CallbackContext):
    query = update.callback_query
    if not query or not query.data or not query.inline_message_id:
        logger.warning("handle_inline_play: Invalid query object.")
        return

    try:
        parts = query.data.split("_", 3)
        if len(parts) != 4 or not parts[0].startswith("inlineplay"):
            await query.answer("Неверный формат данных для inlineplay.", show_alert=True)
            logger.warning(f"Invalid callback_data format for inlineplay: {query.data}")
            return

        _, target_user_id_str, story_id, fragment_id = parts
        sender_user_id = str(query.from_user.id)

        if sender_user_id != target_user_id_str:
            await query.answer("Эта кнопка предназначена для другого пользователя.", show_alert=True)
            return
        
        await query.answer() 

        keyboard_layout = [VOTE_THRESHOLDS[i:i + 5] for i in range(0, len(VOTE_THRESHOLDS), 5)]
        keyboard = []
        for row_thresholds in keyboard_layout:
            keyboard_row = []
            for threshold in row_thresholds:
                keyboard_row.append(InlineKeyboardButton(
                    str(threshold),
                    callback_data=f"setthreshold_{query.inline_message_id}_{threshold}"             
                ))

            keyboard.append(keyboard_row)
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Сохраняем информацию, что ожидается выбор порога, чтобы display_fragment_for_interaction её не затер
        # Это предварительная установка, которая будет дополнена в handle_set_vote_threshold
        context.bot_data[query.inline_message_id] = {
            "type": "threshold_selection", # Флаг, что мы на этапе выбора порога
            "target_user_id_str": target_user_id_str,
            "story_id": story_id,
            "fragment_id": fragment_id
        }
        
        await context.bot.edit_message_text(
            inline_message_id=query.inline_message_id,
            text="Пожалуйста, выберите необходимое количество голосов для перехода к следующему фрагменту:",
            reply_markup=reply_markup
        )
    except Exception as e:
        logger.error(f"Ошибка в handle_inline_play при обработке {query.data}: {e}", exc_info=True)
        if query and not query.answer:
            try:
                await query.answer("Произошла ошибка при подготовке к игре.")
            except Exception: pass


async def handle_set_vote_threshold(update: Update, context: CallbackContext):
    query = update.callback_query
    if not query or not query.data or not query.inline_message_id:
        logger.info("⛔️ Нет данных в callback_query (query.data или inline_message_id отсутствуют)")
        return

    try:
        base_str, threshold_str = query.data.rsplit("_", 1)
        if not base_str.startswith("setthreshold_"):
            logger.info("⚠️ Неверный формат данных в query.data")
            await query.answer("Неверный формат данных.", show_alert=True)
            return

        cb_inline_message_id = base_str[len("setthreshold_"):]
        chosen_threshold = int(threshold_str)
        sender_user_id = str(query.from_user.id)

        logger.info(f"🔧 Установка порога: {chosen_threshold} голосов | message_id: {cb_inline_message_id}")

        # Получаем сохранённые данные по истории
        initial_setup_data = context.bot_data.get(cb_inline_message_id)
        if not initial_setup_data or initial_setup_data.get("type") != "threshold_selection":
            logger.info(f"❗️Данные сессии не найдены или истекли: {cb_inline_message_id}")
            await query.answer("Сессия настройки истекла или не найдена.", show_alert=True)
            return

        target_user_id_str = initial_setup_data["target_user_id_str"]
        story_id = initial_setup_data["story_id"]
        initial_fragment_id = initial_setup_data["fragment_id"]

        if sender_user_id != target_user_id_str:
            logger.info(f"⛔️ Пользователь {sender_user_id} пытался установить порог, но не является инициатором {target_user_id_str}")
            await query.answer("Только инициатор истории может установить порог.", show_alert=True)
            return

        if cb_inline_message_id != query.inline_message_id:
            logger.info(f"⚠️ Несовпадение inline_message_id: ожидался {cb_inline_message_id}, получен {query.inline_message_id}")
            await query.answer("Ошибка идентификатора сообщения.", show_alert=True)
            return

        await query.answer(f"Порог в {chosen_threshold} голосов установлен!")

        # Подготовка и сохранение состояния истории
        story_initial_state = {
            "story_id": story_id,
            "target_user_id": target_user_id_str,
            "current_fragment_id": initial_fragment_id,
            "required_votes_to_win": chosen_threshold,
        }

        save_story_state_to_firebase(query.inline_message_id, story_initial_state)
        logger.info(f"✅ Порог установлен и сохранён: {chosen_threshold} голосов | История: {story_id}, Фрагмент: {initial_fragment_id}")

        # Обновляем bot_data для следующего этапа
        context.bot_data[query.inline_message_id] = {
            "type": "poll_setup_pending_display",
            "required_votes": chosen_threshold,
        }

        await display_fragment_for_interaction(
            context,
            query.inline_message_id,
            target_user_id_str,
            story_id,
            initial_fragment_id
        )
        logger.info(f"📤 Фрагмент отправлен для голосования: {initial_fragment_id}")

    except ValueError:
        logger.info("❌ Ошибка преобразования порога в int")
        await query.answer("Неверное значение порога.", show_alert=True)
    except Exception as e:
        logger.info(f"💥 Ошибка в handle_set_vote_threshold: {e}", exc_info=True)
        if query and hasattr(query, 'answer') and not query.answered:
            try:
                await query.answer("Произошла ошибка при установке порога.")
            except Exception:
                pass


async def end_poll_and_proceed(context: CallbackContext, inline_message_id: str, winning_choice_idx: int, poll_data: dict):
    logger.info(f"[{inline_message_id}] Завершение голосования. Победил вариант #{winning_choice_idx}")

    target_user_id = poll_data["target_user_id"]
    story_id = poll_data["story_id"]
    current_fragment_id = poll_data.get("current_fragment_id")

    # Удаляем данные опроса из оперативной памяти
    context.bot_data.pop(inline_message_id, None)

    choices_data = poll_data["choices_data"]
    next_fragment_id_to_display = choices_data[winning_choice_idx]["next_fragment_id"]
    winner_text_choice = choices_data[winning_choice_idx]["text"]
    num_votes_for_winner = len(poll_data["votes"].get(winning_choice_idx, set()))
    required_votes_to_win = poll_data.get("required_votes_to_win", 1)

    winning_effects = choices_data[winning_choice_idx].get("effects", [])
    alert_text = ""
    proceed = True
    needs_retry = False
    story_state = None

    if winning_effects:
        logger.info(f"[{inline_message_id}] Применение эффектов варианта: {winner_text_choice!r}")
        try:
            proceed, alert_text, needs_retry, updated_story_state = await process_choice_effects_to_user_attributes(
                inline_message_id=inline_message_id,
                user_id=int(target_user_id),
                effects_list=winning_effects,
                context=context
            )
            story_state = updated_story_state

            if story_state and "user_attributes" in story_state:
                context.bot_data.setdefault(inline_message_id, {})["user_attributes"] = story_state["user_attributes"]
            logger.info(f"[{inline_message_id}] Эффекты успешно применены")
        except Exception as e:
            logger.info(f"[{inline_message_id}] ❌ Ошибка при применении эффектов: {e}")
            alert_text = "Произошла внутренняя ошибка при применении эффектов."
            proceed = False
            needs_retry = False

    safe_winner_text_choice = html.escape(winner_text_choice)
    safe_alert_text = html.escape(alert_text) if alert_text else None

    winner_message_text = f"Голосование завершено!\nВыбран вариант: \"{safe_winner_text_choice}\""
    if required_votes_to_win > 1:
        winner_message_text += f" ({num_votes_for_winner} голосов)."
    if safe_alert_text:
        winner_message_text += f"\n\n{safe_alert_text}"

    if not proceed and needs_retry:
        logger.info(f"[{inline_message_id}] Проверка эффектов не пройдена, требуется повтор фрагмента {current_fragment_id}")
        try:
            if story_state:
                story_state.pop("poll_details", None)
                save_story_state_to_firebase(inline_message_id, story_state)

            delay_seconds = 10
            winner_message_text += f"\n\n<i>Продолжение через {delay_seconds} секунд...</i>"
            await context.bot.edit_message_text(
                inline_message_id=inline_message_id,
                text=winner_message_text,
                reply_markup=None,
                parse_mode='HTML'
            )
            await asyncio.sleep(delay_seconds)
            await display_fragment_for_interaction(
                context, inline_message_id, str(target_user_id), story_id, current_fragment_id
            )
        except Exception as e:
            logger.info(f"[{inline_message_id}] ❌ Ошибка при повторе фрагмента: {e}")
        return

    # Если все прошло успешно или повтор не требуется
    logger.info(f"[{inline_message_id}] Голосование успешно завершено, переход к следующему фрагменту")

    if not story_state:
        story_state = load_story_state_from_firebase(inline_message_id)
        logger.info(f"[{inline_message_id}] Загружено состояние из Firebase")

    if story_state:
        story_state.pop("poll_details", None)
        story_state["current_fragment_id"] = next_fragment_id_to_display
        save_story_state_to_firebase(inline_message_id, story_state)
        logger.info(f"[{inline_message_id}] Состояние обновлено и сохранено")

    try:
        if required_votes_to_win > 1 or winning_effects:
            delay_seconds = 5
            winner_message_text += f"\n\n<i>Продолжение через {delay_seconds} секунд...</i>"
            await context.bot.edit_message_text(
                inline_message_id=inline_message_id,
                text=winner_message_text,
                reply_markup=None,
                parse_mode='HTML'
            )
            await asyncio.sleep(delay_seconds)
    except Exception as e:
        logger.info(f"[{inline_message_id}] ❌ Ошибка при показе финального сообщения: {e}")

    if proceed and next_fragment_id_to_display:
        logger.info(f"[{inline_message_id}] Отображение следующего фрагмента: {next_fragment_id_to_display}")
        await display_fragment_for_interaction(context, inline_message_id, target_user_id, story_id, next_fragment_id_to_display)
    elif proceed:
        logger.info(f"[{inline_message_id}] История завершена — нет следующего фрагмента")
        final_text = winner_message_text + "\n\nИстория завершена."
        try:
            await context.bot.edit_message_text(
                inline_message_id=inline_message_id,
                text=final_text,
                reply_markup=None
            )
            db.reference(f'story_settings/{inline_message_id}').delete()
            logger.info(f"[{inline_message_id}] Очистка настроек истории завершена")
        except Exception as e:
            logger.info(f"[{inline_message_id}] ❌ Ошибка при завершении истории: {e}")

async def handle_poll_vote(update: Update, context: CallbackContext):
    query = update.callback_query
    if not query or not query.data or not query.inline_message_id:
        return

    try:
        parts = query.data.rsplit("_", 1)
        if len(parts) != 2:
            await query.answer("Ошибка формата.", show_alert=True)
            logger.info("Прервано: некорректный формат callback_data.")
            return

        callback_prefix_and_msg_id = parts[0]
        choice_idx_str = parts[1]

        vote_parts = callback_prefix_and_msg_id.split("_", 1)
        if len(vote_parts) != 2 or vote_parts[0] != "vote":
            await query.answer("Ошибка формата callback (prefix).", show_alert=True)
            logger.info("Прервано: неверный префикс callback_data.")
            return

        inline_msg_id_from_cb = vote_parts[1]
        if inline_msg_id_from_cb != query.inline_message_id:
            await query.answer("Ошибка идентификатора.", show_alert=True)
            logger.info(f"Прервано: inline_message_id не совпадает. Из callback: {inline_msg_id_from_cb}, из запроса: {query.inline_message_id}")
            return

        choice_idx = int(choice_idx_str)
        user_id = query.from_user.id

        poll_data = context.bot_data.get(query.inline_message_id)

        if not poll_data or poll_data.get("type") != "poll":
            logger.info(f"Данные голосования для {query.inline_message_id} не найдены в памяти. Загружаем из Firebase.")
            story_state_from_firebase = load_story_state_from_firebase(query.inline_message_id)

            if not story_state_from_firebase or "poll_details" not in story_state_from_firebase:
                await query.answer("Голосование не найдено, завершено или неактуально.", show_alert=True)
                logger.info(f"Firebase: голосование {query.inline_message_id} не найдено или завершено.")
                return

            current_fragment_id_fb = story_state_from_firebase.get("current_fragment_id")
            poll_details_fb = story_state_from_firebase["poll_details"]

            votes_raw = poll_details_fb.get("votes")
            votes_dict = deserialize_votes_from_db(votes_raw)

            all_voted_in_votes = set(uid for vote_list in votes_dict.values() for uid in vote_list)
            voted_users_list = poll_details_fb.get("voted_users", [])
            cleaned_voted_users = [uid for uid in voted_users_list if uid in all_voted_in_votes]

            rehydrated_poll_data = {
                "type": "poll",
                "target_user_id": story_state_from_firebase["target_user_id"],
                "story_id": story_state_from_firebase["story_id"],
                "current_fragment_id": current_fragment_id_fb,
                "choices_data": poll_details_fb.get("choices_data", []),
                "votes": votes_dict,
                "voted_users": set(cleaned_voted_users),
                "required_votes_to_win": story_state_from_firebase["required_votes_to_win"],
                "user_attributes": story_state_from_firebase.get("user_attributes", {}),
            }

            if poll_data and current_fragment_id_fb != poll_data.get("current_fragment_id"):
                await query.answer("Голосование не найдено, завершено или неактуально.", show_alert=True)
                logger.info("Прервано: fragment_id из Firebase не совпадает с текущим.")
                return

            context.bot_data[query.inline_message_id] = rehydrated_poll_data
            poll_data = rehydrated_poll_data
            logger.info(f"Голосование восстановлено из Firebase: {query.inline_message_id}")

        votes_dict = poll_data.get("votes", {})
        voted_users_set = poll_data.get("voted_users", set())
        all_voted_in_votes = set(uid for vote_list in votes_dict.values() for uid in vote_list)
        cleaned_voted_users = voted_users_set.intersection(all_voted_in_votes)
        poll_data["voted_users"] = cleaned_voted_users

        if not poll_data or poll_data.get("type") != "poll":
            await query.answer("Голосование не найдено или завершено.", show_alert=True)
            logger.info("Прервано: poll_data отсутствует или некорректно.")
            return

        if user_id in poll_data["voted_users"]:
            await query.answer("Вы уже голосовали.", show_alert=True)
            logger.info(f"Пользователь {user_id} уже голосовал.")
            return

        if choice_idx < 0 or choice_idx >= len(poll_data["choices_data"]):
            await query.answer("Некорректный вариант выбора.", show_alert=True)
            logger.info(f"Некорректный индекс выбора: {choice_idx}")
            return

        votes = poll_data.get("votes")
        if not isinstance(votes, dict):
            if isinstance(votes, list):
                votes = {
                    idx: set(user_ids if isinstance(user_ids, list) else [user_ids])
                    for idx, user_ids in enumerate(votes)
                }
            else:
                votes = {}
        else:
            votes = {
                idx: set(user_ids if isinstance(user_ids, (list, set)) else [user_ids])
                for idx, user_ids in votes.items()
            }

        poll_data["votes"] = votes
        poll_data["votes"].setdefault(choice_idx, set()).add(user_id)

        if not isinstance(poll_data.get("voted_users"), set):
            poll_data["voted_users"] = set(poll_data.get("voted_users", []))
        poll_data["voted_users"].add(user_id)

        firebase_save_data = {
            "story_id": poll_data["story_id"],
            "target_user_id": poll_data["target_user_id"],
            "current_fragment_id": poll_data["current_fragment_id"],
            "required_votes_to_win": poll_data["required_votes_to_win"],
            "poll_details": {
                "choices_data": poll_data["choices_data"],
                "votes": poll_data["votes"],
                "voted_users": poll_data["voted_users"]
            },
            "user_attributes": poll_data["user_attributes"],
        }

        save_story_state_to_firebase(query.inline_message_id, firebase_save_data)
        logger.info(f"Голос принят: {query.inline_message_id}, выбор {choice_idx}, пользователь {user_id}")

        required_votes_to_win = poll_data["required_votes_to_win"]
        num_votes_for_current_choice = len(poll_data["votes"][choice_idx])

        if num_votes_for_current_choice >= required_votes_to_win:
            await query.answer(f"Голос принят! Вариант набрал {required_votes_to_win} голосов!", show_alert=False)
            logger.info(f"Голосование завершено: {query.inline_message_id}, выбор {choice_idx}")
            await end_poll_and_proceed(context, query.inline_message_id, choice_idx, poll_data)
            return

        await query.answer("Ваш голос принят!")
        await display_fragment_for_interaction(
            context,
            inline_message_id=query.inline_message_id,
            target_user_id_str=poll_data["target_user_id"],
            story_id=poll_data["story_id"],
            fragment_id=poll_data["current_fragment_id"]
        )

    except ValueError:
        await query.answer("Неверный выбор (ошибка значения).", show_alert=True)
        logger.info(f"Ошибка преобразования choice_idx: {query.data}")
    except Exception as e:
        logger.info(f"Ошибка в handle_poll_vote: {e}")
        if query and hasattr(query, 'answer') and not query.answered:
            try:
                await query.answer("Ошибка при голосовании.")
            except Exception:
                pass

def is_possible_story_id(text: str) -> bool:
    return bool(re.fullmatch(r'[0-9a-f]{10}', text.lower()))

async def inlinequery(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query_text = update.inline_query.query.strip()
    results = []
    user_id = str(update.inline_query.from_user.id)
    stories_to_show = {}
    
    def format_story_text(story_id: str, story_data: dict) -> str:
        title = story_data.get("title", "Без названия")
        neural = story_data.get("neural", False)
        author = story_data.get("author", "")
        clean_author = html.escape(author)
        clean_title = html.escape(title)        
        lines = [f"📖 <b>История:</b> «{clean_caption(clean_title)}»"]
        if author:
            lines.append(f"✍️ <b>Автор:</b> {clean_caption(clean_author)}{' (нейроистория)' if neural else ''}")
        lines.append(f"🆔 <b>ID:</b> <code>{story_id}</code>")
        lines.append("\n<i>Нажмите кнопку ниже, чтобы настроить и запустить историю в этом чате.</i>")
        return "\n".join(lines)

    if not query_text:
        # Показываем все истории текущего пользователя
        user_stories_ref = db.reference(f'users_story/{user_id}')
        stories_to_show = user_stories_ref.get() or {}
    else:
        query_text_lower = query_text.lower()
        is_id_search = is_possible_story_id(query_text_lower)

        if is_id_search:
            # Поиск по всем историям всех пользователей только по ID
            all_users_data = db.reference('users_story').get() or {}
            for uid, user_stories_dict in all_users_data.items():
                if query_text_lower in user_stories_dict:
                    stories_to_show[query_text_lower] = user_stories_dict[query_text_lower]
                    break  # Нашли — достаточно
        else:
            # Поиск по заголовкам только среди историй текущего пользователя
            user_stories = db.reference(f'users_story/{user_id}').get() or {}
            for story_id_key, story_content in user_stories.items():
                title = story_content.get("title", "").lower()
                if query_text_lower in title:
                    stories_to_show[story_id_key] = story_content

    for story_id, story_data in stories_to_show.items():
        # Определяем владельца (нужно только если поиск был по ID)
        owner_user_id_for_story = user_id
        if is_possible_story_id(query_text):
            all_users_data = db.reference('users_story').get() or {}
            for uid, user_stories_dict in all_users_data.items():
                if story_id in user_stories_dict:
                    owner_user_id_for_story = uid
                    break

        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("▶️ Настроить и играть здесь", callback_data=f"inlineplay_{owner_user_id_for_story}_{story_id}_main_1")],
            [InlineKeyboardButton("▶️ Открыть в чате с ботом", url=f"https://t.me/{context.bot.username}?start={story_id}")]
        ])
        results.append(InlineQueryResultArticle(
            id=str(uuid4()),
            title=f"История: {story_data.get('title', 'Без названия')}",
            description=f"Автор: {story_data.get('author', 'Неизвестен')}",
            input_message_content=InputTextMessageContent(format_story_text(story_id, story_data), parse_mode="HTML"),
            reply_markup=buttons
        ))
        if len(results) >= 49:
            break

    await update.inline_query.answer(results, cache_time=15)




#==========================================================================


# --- кооп ---

def build_coop_edit_keyboard(user_id_str: str, story_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Добавить пользователя", callback_data=f"coop_add_{user_id_str}_{story_id}")],
        [InlineKeyboardButton("➖ Удалить пользователя", callback_data=f"coop_remove_{user_id_str}_{story_id}")],
        [InlineKeyboardButton("❌ Закрыть это окно", callback_data="delete_this_message")]
    ])




async def handle_coop_add(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    parts = query.data.split('_')
    user_id_str, story_id = parts[2], parts[3]
    context.user_data['coop_add_target'] = (user_id_str, story_id)

    cancel_button = InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_coop_add")]
    ])

    await query.message.reply_text(
        "📩 Пожалуйста, перешлите сообщение пользователя, которого хотите добавить, или отправьте его ID.\n\n"
        "Будьте осторожны давая доступ.",
        reply_markup=cancel_button,
        parse_mode=ParseMode.HTML
    )
    return COOP_ADD_USER



async def receive_coop_user_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id_str, story_id = context.user_data.get('coop_add_target', (None, None))
    if not user_id_str or not story_id:
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад к списку историй", callback_data="view_stories")]])
        await update.message.reply_text("Ошибка контекста. Повторите команду.", reply_markup=keyboard)
        return EDIT_STORY_MAP

    new_user_id = None
    message = update.message
    logger.info(f"message: {message}") 
    if message:
        if getattr(message, 'forward_from', None):
            new_user_id = str(message.forward_from.id)
        elif getattr(message, 'forward_origin', None) and getattr(message.forward_origin, 'sender_user', None):
            new_user_id = str(message.forward_origin.sender_user.id)
        else:
            text = message.text
            if text and text.isdigit():
                new_user_id = text

    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад к списку историй", callback_data="view_stories")]])

    if new_user_id:
        story_data = load_user_story(user_id_str, story_id)
        if not story_data:
            await update.message.reply_text(
                "❌ Не удалось загрузить историю. Возможно, она была удалена.",
                reply_markup=keyboard
            )
            return EDIT_STORY_MAP

        coop_list = story_data.setdefault("coop_edit", [])
        if new_user_id not in coop_list:
            coop_list.append(new_user_id)
            save_story_data(user_id_str, story_id, story_data)
            await update.message.reply_text(
                f"✅ Пользователь с ID <code>{new_user_id}</code> добавлен для совместного редактирования.",
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML
            )
            return EDIT_STORY_MAP
        else:
            await update.message.reply_text(
                f"ℹ️ Пользователь с ID <code>{new_user_id}</code> уже имеет доступ.",
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML
            )
            return EDIT_STORY_MAP
    else:
        await update.message.reply_text(
            "❌ Не удалось определить ID пользователя. Возможно он отключил пересылку сообщений. Либо попросите этого пользователя на время включить её, либо отправьте его ID вручную.",
            reply_markup=keyboard
        )
        return COOP_ADD_USER


async def cancel_coop_add(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if query:
        await query.answer()
        await query.message.delete()
        await query.message.chat.send_message("❌ Добавление/удаление пользователя отменено.")
    else:
        await update.message.reply_text("❌ Добавление/удаление пользователя отменено.")
    return EDIT_STORY_MAP

async def show_coop_edit_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    parts = query.data.split('_')
    user_id_str, story_id = parts[3], parts[4]
    reply_markup = build_coop_edit_keyboard(user_id_str, story_id)

    await query.message.reply_text(
        text="Что вы хотите сделать? \n\n ВНИМАНИЕ!!! В боте пока не реализована система ограничений редактирования для сооватворов. И в ближайшее время она не появится точно, поскольку это довольно сложно для реализации. Поэтому добавляйте только тех людей в которых вы уверены что они ничего вам специально не испортят, не удалят и тд",
        reply_markup=reply_markup
    )




async def handle_coop_remove(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    parts = query.data.split('_')
    user_id_str, story_id = parts[2], parts[3]
    context.user_data['coop_remove_target'] = (user_id_str, story_id)

    cancel_button = InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_coop_add")]
    ])

    await query.message.reply_text(
        "🗑 Пожалуйста, перешлите сообщение пользователя, которого хотите удалить, или отправьте его ID.\n\n",
        reply_markup=cancel_button,
        parse_mode=ParseMode.HTML
    )
    return COOP_DELETE_USER




async def receive_coop_user_id_to_remove(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id_str, story_id = context.user_data.get('coop_remove_target', (None, None))
    if not user_id_str or not story_id:
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад к списку историй", callback_data="view_stories")]])
        await update.message.reply_text("Ошибка контекста. Повторите команду.", reply_markup=keyboard)
        return EDIT_STORY_MAP

    remove_user_id = None
    message = update.message
    if message:
        if getattr(message, 'forward_from', None):
            remove_user_id = str(message.forward_from.id)
        elif getattr(message, 'forward_origin', None) and getattr(message.forward_origin, 'sender_user', None):
            remove_user_id = str(message.forward_origin.sender_user.id)
        else:
            text = message.text
            if text and text.isdigit():
                remove_user_id = text

    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад к списку историй", callback_data="view_stories")]])

    if remove_user_id:
        story_data = load_user_story(user_id_str, story_id)

        coop_list = story_data.setdefault("coop_edit", [])
        if remove_user_id in coop_list:
            coop_list.remove(remove_user_id)
            save_story_data(user_id_str, story_id, story_data)
            await update.message.reply_text(
                f"✅ Пользователь с ID <code>{remove_user_id}</code> удалён из совместного редактирования.",
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML
            )
            return EDIT_STORY_MAP
        else:
            await update.message.reply_text(
                f"ℹ️ Пользователь с ID <code>{remove_user_id}</code> не найден в списке.",
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML
            )
            return EDIT_STORY_MAP
    else:
        await update.message.reply_text(
            "❌ Не удалось определить ID пользователя. Возможно он отключил пересылку сообщений. Либо попросите этого пользователя на время включить её, либо отправьте его ID вручную.",
            reply_markup=keyboard
        )
        return COOP_DELETE_USER



#==========================================================================


# --- Основные обработчики ---


# Убедитесь, что эти импорты и функции/переменные доступны в области видимости start
# from your_data_logic_file import load_data # Ваша функция для загрузки данных
# from your_story_player_file import render_fragment, active_timers # Ваша функция для отображения фрагмента и active_timers

# Настройка логгера (если еще не настроен глобально)
logger = logging.getLogger(__name__)
# Пример базовой конфигурации логгера, если необходимо
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# ПРЕДПОЛАГАЕТСЯ, ЧТО ЭТИ ФУНКЦИИ И ПЕРЕМЕННЫЕ ОПРЕДЕЛЕНЫ ГДЕ-ТО ЕЩЕ И ИМПОРТИРОВАНЫ:
# def load_data(): ...
# async def render_fragment(context, user_id, story_id, fragment_id, message, story): ...
# active_timers: Dict[str, asyncio.Task] = {} (если render_fragment его использует глобально)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает команды в личке и группах: запускает истории по ID или показывает меню."""

    user_id_str = str(update.effective_user.id)
    message_text = update.message.text.strip() if update.message and update.message.text else ""
    chat_type = update.effective_chat.type if update.effective_chat else "private"

    # Групповой чат: реагировать только на foxstart или ID истории
    # 👇 ДОПОЛНЕНИЕ: если личка и просто текст — считаем его аргументом
    if chat_type == "private" and not context.args and message_text:
        context.args = [message_text]

    # Групповой чат: реагировать только на foxstart или ID истории
    if chat_type != "private":
        # Загрузка всех историй для проверки ID
        all_data = load_data()
        users_story = all_data.get("users_story", {})

        # Проверка: текст == foxstart
        if message_text.lower().startswith("foxstart"):
            keyboard = [
                [InlineKeyboardButton("🌟Посмотреть общие истории", callback_data='public_stories')]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await update.effective_message.reply_text(
                'Добро пожаловать в бот для создания и прохождения визуальных новелл! Выберите действие:',
                reply_markup=reply_markup
            )
            return

        # Проверка: это ID истории?
        for uid, stories in users_story.items():
            if message_text in stories:
                story_data = stories[message_text]
                title = story_data.get("title", "Без названия")
                neural = story_data.get("neural", False)
                author = story_data.get("author", "неизвестен")

                info = f"📖 История: «{title}»\n✍️ Автор: {author}"
                if neural:
                    info += " (нейроистория)"

                suffix = f"{user_id_str}_{message_text}_main_1"
                callback_data = f"nstartstory_{suffix}"

                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("▶️ Открыть", callback_data=callback_data)]
                ])
                await update.effective_message.reply_text(
                    f"🎮 Обнаружена история.\n\n{info}\n\nНажмите кнопку ниже, чтобы вызвать своё персональное сообщение для прохождения в нём этой истории:\n\n<i>(обратите внимание что если автор вызовет эту историю через @FoxNovel_bot то можно настроить совместное прохождение в одном окне, тогда выборы будут делаться по голосам)</i>",
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML
                )
                return
        return  # Ни foxstart, ни ID — игнорируем
    else:
        # Приватный чат — любые сообщения могут быть ID истории
        if not context.args and message_text:
            context.args = [message_text]

    # Запуск истории, если аргументы переданы
    if context.args:
        story_id_to_start = context.args[0]
        logger.info(f"Пользователь {user_id_str} пытается запустить историю {story_id_to_start} через /start.")

        all_data = load_data()
        users_story = all_data.get("users_story", {})

        story_data = None
        story_owner_id = None

        for uid, stories in users_story.items():
            if story_id_to_start in stories:
                story_data = stories[story_id_to_start]
                story_owner_id = uid
                break

        if story_data:
            if story_data.get("fragments"):
                if "main_1" in story_data["fragments"]:
                    first_fragment_id = "main_1"
                else:
                    first_fragment_id = next(iter(story_data["fragments"]), None)
                if first_fragment_id:
                    context.user_data.clear()
                    # ▼▼▼ НАЧАЛО ИСПРАВЛЕНИЯ ▼▼▼
                    # Инициализируем и сохраняем начальный прогресс пользователя.
                    # Это "сообщит" боту, что пользователь официально начал историю с этого фрагмента.
                    initial_progress = {
                        "fragment_id": first_fragment_id,
                        "current_effects": {}  # Начинаем с пустыми эффектами
                    }
                    # Убедитесь, что у вас есть функция save_user_story_progress
                    # и она импортирована в этот файл.
                    save_user_story_progress(story_id_to_start, int(user_id_str), initial_progress)
                    logger.info(f"Пользователь {user_id_str} запускает историю {story_id_to_start}. Начальный прогресс для фрагмента {first_fragment_id} сохранен.")
                    # ▲▲▲ КОНЕЦ ИСПРАВЛЕНИЯ ▲▲▲

                    placeholder_message = await update.effective_message.reply_text("⏳ Загрузка истории...")

                    first_fragment_data = story_data["fragments"].get(first_fragment_id, {})
                    fragment_text_content = first_fragment_data.get("text", "")

                    base_text_for_display = re.split(r"(\[\[[-+]\d+\]\]|\(\([-+]\d+\)\))", fragment_text_content, 1)[0].strip()
                    edit_steps = parse_timed_edits(fragment_text_content)

                    await render_fragment(
                        context=context,
                        user_id=user_id_str,                        
                        owner_id=int(story_owner_id),
                        story_id=story_id_to_start,
                        fragment_id=first_fragment_id,
                        message_to_update=placeholder_message,
                        story_data=story_data,
                        chat_id=update.effective_chat.id,
                        current_auto_path=[],
                        base_text_for_display=base_text_for_display,
                        edit_steps_for_text=edit_steps
                    )
                    return
                else:
                    await update.effective_message.reply_text(f"История '{story_id_to_start}' пуста.")
            else:
                await update.effective_message.reply_text(f"История '{story_id_to_start}' не содержит фрагментов.")
        else:
            logger.info(f"История с ID {story_id_to_start} не найдена.")

            # Если приватный чат — покажем меню
            if chat_type == "private":
                keyboard = [
                    [InlineKeyboardButton("🌠Создать историю", callback_data='create_story_start')],
                    [InlineKeyboardButton("✏️Управление моими историями", callback_data='view_stories')],
                    [InlineKeyboardButton("🌟Посмотреть общие истории", callback_data='public_stories')],
                    [InlineKeyboardButton("📔Простое обучение", callback_data='play_000_001_main_1')],
                    [InlineKeyboardButton("📚Продвинутое обучение", callback_data='play_000_002_main_1')],                    
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)

                await update.effective_message.reply_text(
                    '🌠Добро пожаловать в бот для создания и прохождения визуальных новелл!\n\n'
                    'Если у вас есть ID истории — отправьте его, и она начнётся.\nИли создайте свою, или откройте публичные:',
                    reply_markup=reply_markup
                )
                return
            else:
                return  # В группе — просто молча игнорируем













# --- НОВАЯ ФУНКЦИЯ RESTART ---
def _remove_task_from_context(task: asyncio.Task, user_data: Dict[str, Any]):
    """Вспомогательная функция для удаления задачи из user_data."""
    user_tasks_set = user_data.get('user_tasks')
    if isinstance(user_tasks_set, set):
        user_tasks_set.discard(task)

async def restart(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Полностью очищает состояние пользователя, отменяет его фоновые задачи и возвращает в главное меню."""
    user_id = update.effective_user.id
    logger.info(f"Пользователь {user_id} вызвал restart (через команду или fallback). Отмена активных задач и очистка user_data.")

    # Отменяем все активные асинхронные задачи пользователя
    if 'user_tasks' in context.user_data and isinstance(context.user_data['user_tasks'], set):
        active_tasks_for_user: Set[asyncio.Task] = context.user_data['user_tasks']
        if active_tasks_for_user:
            logger.info(f"Пользователь {user_id}: отменяются {len(active_tasks_for_user)} фоновых задач.")
            # Итерируемся по копии множества, так как отмена может вызвать колбэки, модифицирующие множество
            for task in list(active_tasks_for_user):
                if not task.done():
                    task.cancel()
                    # Можно добавить небольшое ожидание здесь, чтобы задача успела обработать отмену,
                    # но это может замедлить рестарт. Обычно просто task.cancel() достаточно.
                    # logger.info(f"Задача '{task.get_name()}' для пользователя {user_id} помечена для отмены.")
    
    # Очищаем временные данные пользователя
    # Это также удалит 'user_tasks', если он не был удален ранее.
    context.user_data.clear()

    # Отправляем подтверждение и главное меню (дублируем логику start)
    keyboard = [
        [InlineKeyboardButton("🌠Создать историю", callback_data='create_story_start')],
        [InlineKeyboardButton("✏️Управление моими историями", callback_data='view_stories')],
        [InlineKeyboardButton("🌟Посмотреть общие истории", callback_data='public_stories')],
        [InlineKeyboardButton("📔Простое обучение", callback_data='play_000_001_main_1')],
        [InlineKeyboardButton("📚Продвинутое обучение", callback_data='play_000_002_main_1')],   
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    message_text = (
        'Бот перезапущен и вы успешно вернулись в главное меню 🦊\n\n'
        'Отправьте id истории для её запуска, если у вас есть id.\n\n'
        'Либо создайте свою собственную историю или посмотрите общие:'
    )

    if update.callback_query:
        try:
            await update.callback_query.edit_message_text(
                text=message_text,
                reply_markup=reply_markup
            )
            await update.callback_query.answer("Бот перезапущен")
        except Exception as e:
            logger.warning(f"Не удалось отредактировать сообщение при рестарте из callback: {e}. Отправка нового.")
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=message_text,
                reply_markup=reply_markup
            )
            await update.callback_query.answer("Бот перезапущен")
    elif update.message:
        await update.message.reply_text(
            message_text,
            reply_markup=reply_markup
        )

    return ConversationHandler.END


async def delete_message_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        # Удаляем сообщение с кнопкой
        await query.message.delete()
    except Exception:
        pass  # Кнопка могла быть уже удалена

    # Удаляем все предпросмотренные сообщения
    message_ids = context.user_data.get("preview_message_ids", [])
    logger.info(f"message_ids : {message_ids}")
    for msg_id in message_ids:
        try:
            await context.bot.delete_message(
                chat_id=query.message.chat_id,
                message_id=msg_id
            )
        except Exception as e:
            print(f"Ошибка при удалении сообщения {msg_id}: {e}")
    
    # Очищаем список
    context.user_data["preview_message_ids"] = []

    #keyboard = [
        #[InlineKeyboardButton("🗑 Удалить", callback_data="delete_this_message")]
    #]












#==========================================================================
PROTECTED_FRAGMENT_ID = "main_1"

#УДАЛЕНИЕ ВЕТОК И ФРАГМЕНТОВ

async def handle_delete_fragment_execute(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int | None:
    """Выполняет удаление фрагмента и его потомков."""
    query = update.callback_query
    await query.answer("Удаление...")

    try:
        callback_data = query.data
        logger.info(f"callback_data: {callback_data}")
        parts = callback_data.split('_')
        logger.info(f"parts: {parts}")
        # Ожидаемый формат: dfre_STORYID_FRAGMENTID (префикс DELETE_FRAGMENT_EXECUTE_PREFIX)
        if len(parts) < 3 or parts[0] != DELETE_FRAGMENT_EXECUTE_PREFIX: # Используем ваш префикс
            raise ValueError("Invalid callback data format for execute")

        user_id_str = str(update.effective_user.id) # Получаем user_id из update
        story_id = parts[1]
        target_fragment_id = "_".join(parts[2:]) # Корректно для ID с '_'
        if target_fragment_id == PROTECTED_FRAGMENT_ID:
            await query.edit_message_text(f"Фрагмент <code>{PROTECTED_FRAGMENT_ID}</code> не может быть удален, так как это начальный фрагмент истории, это нарушит логику бота и история станет недостпна.")
            # Возможно, потребуется обновить отображение карты или списка здесь
            # await show_fragment_list_or_map(update, context, user_id_str, story_id) # Если есть такая функция
            return EDIT_STORY_MAP # Или другое подходящее состояние
        # Проверка прав пользователя (user_id из callback должен совпадать с user_id автора истории, если такая логика есть)
        # В вашем коде user_id не передается в callback_data для dfre, он берется из update.effective_user.id
        # Это нормально, если владелец истории определяется по user_id_str, связанному с story_data.


        story_data = load_user_story(user_id_str, story_id)
        user_id = int(user_id_str)
        owner_id_str = get_owner_id_or_raise(user_id, story_id, story_data)

        if not story_data or "fragments" not in story_data:
            await query.edit_message_text("Ошибка: История или фрагменты не найдены.")
            return EDIT_STORY_MAP

        all_fragments = story_data["fragments"]

        if target_fragment_id not in all_fragments:
            await query.edit_message_text(f"Фрагмент {target_fragment_id} уже удален или не существует.")
            # Попытка обновить вид
            # await show_fragment_list_or_map(update, context, user_id_str, story_id)
            return EDIT_STORY_MAP

        # --- Находим фрагменты для удаления ---
        # Шаг 1: Получаем полное логическое поддерево с помощью новой функции
        full_deletion_tree = find_descendant_fragments(all_fragments, target_fragment_id)

        if not full_deletion_tree : # target_fragment_id должен быть в full_deletion_tree, если он существует
            await query.edit_message_text(f"Ошибка: Не удалось определить поддерево для удаления для {target_fragment_id}.")
            return EDIT_STORY_MAP

        # Шаг 2: Поиск внешних ссылок на фрагменты в поддереве
        fragments_preview_for_deletion, externally_referenced_in_subtree, descendants_to_list, bypass_reachable = get_fragments_for_deletion_preview(
                    all_fragments=all_fragments,
                    target_fragment_id=target_fragment_id,
                    potential_full_subtree=full_deletion_tree,
                    protected_fragment_id=PROTECTED_FRAGMENT_ID,
                )

        # Шаг 3: Определяем окончательный список fragments_to_delete.
        fragments_to_delete = fragments_preview_for_deletion

        if not fragments_to_delete or target_fragment_id not in fragments_to_delete:
            await query.edit_message_text(f"Фрагмент {target_fragment_id} не удалось подготовить к удалению (возможно, он защищен или уже удален).")
            return EDIT_STORY_MAP

        deleted_count = 0
        for frag_id in list(fragments_to_delete):
            if frag_id == PROTECTED_FRAGMENT_ID: # Дополнительная защита
                logger.warning(f"Попытка удалить {PROTECTED_FRAGMENT_ID} на этапе фактического удаления! Пропускаем.")
                continue
            if frag_id in all_fragments:
                del all_fragments[frag_id]
                deleted_count += 1
                logger.info(f"Удален фрагмент {frag_id}...")


        
        # --- ОЧЕНЬ ВАЖНО: Очистка ссылок в родительских (оставшихся) фрагментах ---
        fragments_to_delete_set = set(fragments_to_delete) # для быстрой проверки
        for frag_id, fragment_content in list(all_fragments.items()):
            if "choices" in fragment_content:
                choices = fragment_content.get("choices", [])
                new_choices = [c for c in choices if c["target"] not in fragments_to_delete_set]
                if len(new_choices) != len(choices):
                    all_fragments[frag_id]["choices"] = new_choices
        
        # --- Сохраняем изменения ---
        if 'current_story' in context.user_data and context.user_data.get('story_id') == story_id:
            context.user_data['current_story'] = story_data

        save_story_data(owner_id_str, story_id, story_data)

        # --- Генерация и отправка обновленной карты ---
        total_fragments_after_delete = len(all_fragments)
        # Убедитесь, что generate_story_map корректно работает с новыми ID
        image_path = generate_story_map(story_id, story_data) # Без выделения

        try:
            message_text = f"Удалено {deleted_count} фрагментов (начиная с <code>{target_fragment_id}</code>)."
            reply_markup_map_button = InlineKeyboardMarkup([[
                InlineKeyboardButton("🔄 Обновить список/карту", callback_data=f"edit_story_{owner_id_str}_{story_id}")
            ]])

            # Удаляем старое сообщение с кнопками подтверждения, если оно от бота
            if query.message.from_user.is_bot:
                 await query.message.delete()
            
            if image_path and os.path.exists(image_path):
                with open(image_path, 'rb') as file:
                    if total_fragments_after_delete > 20:
                        await context.bot.send_message(
                            chat_id=query.message.chat_id, # query.message.chat_id (или update.effective_chat.id)
                            text=message_text + " Слишком много фрагментов — карта отправлена отдельным файлом.",
                            reply_markup=reply_markup_map_button,
                            parse_mode=ParseMode.HTML
                        )
                        await context.bot.send_document(
                            chat_id=query.message.chat_id,
                            document=file,
                            filename=os.path.basename(image_path)
                        )
                    else:
                        await context.bot.send_photo(
                            chat_id=query.message.chat_id,
                            photo=file,
                            caption=message_text,
                            reply_markup=reply_markup_map_button,
                            parse_mode=ParseMode.HTML
                        )
                os.remove(image_path)
            else: # Если карта не сгенерировалась или не нашлась
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text=message_text + " Не удалось сгенерировать карту.",
                    reply_markup=reply_markup_map_button,
                    parse_mode=ParseMode.HTML
                )
        except Exception as e:
            logger.error(f"Ошибка при отправке обновлённой карты: {e}")
            # Сообщение об удалении уже отправлено или будет отправлено без карты
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=message_text + " Произошла ошибка при отправке карты.",
                reply_markup=reply_markup_map_button,
                parse_mode=ParseMode.HTML
            )
        
        return EDIT_STORY_MAP # Возвращаемся к карте/списку

    except ValueError as ve: # Ловим конкретную ошибку парсинга callback_data
        logger.warning(f"Ошибка обработки callback_data при удалении: {ve}")
        await query.edit_message_text(f"Ошибка данных для удаления: {ve}")
        return EDIT_STORY_MAP
    except Exception as e:
        logger.error(f"Ошибка в handle_delete_fragment_execute: {e}", exc_info=True)
        try: # Попытка отправить сообщение об ошибке, если query.edit_message_text не сработает
            await query.edit_message_text("Произошла критическая ошибка при удалении фрагмента.")
        except Exception:
            await context.bot.send_message(update.effective_chat.id, "Произошла критическая ошибка при удалении фрагмента.")
        return EDIT_STORY_MAP # Попытка безопасного возврата


# --- Префиксы для callback_data ---
DELETE_FRAGMENT_CONFIRM_PREFIX = "dfr"
DELETE_FRAGMENT_EXECUTE_PREFIX = "dfre"
CANCEL_DELETE_PREFIX = "cancel_delete_" # Можно использовать существующий edit_story_map_...


def find_descendant_fragments(all_fragments: dict, start_node_id: str) -> set[str]:
    """
    Находит сам целевой фрагмент и всех его логических потомков,
    следуя по связям 'choices'. Возвращает set идентификаторов фрагментов.
    """
    if start_node_id not in all_fragments:
        return set()

    descendants = set()
    queue = [start_node_id]
    # Множество для отслеживания уже посещенных узлов в текущем обходе,
    # чтобы избежать зацикливания и повторной обработки.
    visited_in_traversal = set()

    while queue:
        current_fid = queue.pop(0)

        if current_fid in visited_in_traversal:
            continue
        visited_in_traversal.add(current_fid)
        descendants.add(current_fid) # Добавляем текущий узел в потомки

        fragment_content = all_fragments.get(current_fid, {})
        choices = fragment_content.get("choices", [])

        for choice in choices:
            choice_target_id = choice.get("target")
            if choice_target_id in all_fragments and choice_target_id not in visited_in_traversal:
                queue.append(choice_target_id)

    return descendants


async def safe_edit_or_resend(query, context, text, reply_markup=None, parse_mode=None):
    try:
        await query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode=parse_mode
        )
    except BadRequest as e:
        # Попытка редактирования не удалась — удаляем и отправляем новое
        try:
            await query.message.delete()
        except Exception as del_err:
            logger.warning(f"Не удалось удалить сообщение: {del_err}")
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=text,
            reply_markup=reply_markup,
            parse_mode=parse_mode
        )

def can_reach_without_target(all_fragments, start_id, target_ids, forbidden_id):
    """
    Проверяет, можно ли из start_id дойти до любого из target_ids, минуя forbidden_id.
    """
    stack = [start_id]
    visited = set()

    while stack:
        current = stack.pop()
        if current in visited or current == forbidden_id:
            continue
        visited.add(current)

        if current in target_ids:
            return True  # Дошли до одной из целей, не проходя через forbidden_id

        for choice in all_fragments.get(current, {}).get("choices", []):
            target = choice.get("target")
            if target and target not in visited:
                stack.append(target)

    return False




async def handle_delete_fragment_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int | None:
    query = update.callback_query
    await query.answer()

    try:
        callback_data = query.data
        logger.info(f"callback_data: {callback_data}")

        pattern = r"^dfr_([a-zA-Z0-9]{10})_(.+)$"
        match = re.match(pattern, callback_data)

        if not match:
            logger.warning("Invalid callback data format for confirm delete")
            await safe_edit_or_resend(query, context, "Ошибка: Неверный формат данных для подтверждения удаления.")
            return EDIT_STORY_MAP 

        story_id = match.group(1)
        target_fragment_id = match.group(2)
        requesting_user_id_str = str(update.effective_user.id) # Определяем до первой проверки PROTECTED_FRAGMENT_ID

        if target_fragment_id == PROTECTED_FRAGMENT_ID:
            message_text = f"Фрагмент <code>{PROTECTED_FRAGMENT_ID}</code> является начальным и не может быть удален."
            keyboard = [[
                InlineKeyboardButton("⬅️ Назад к карте/списку", callback_data=f"edit_story_{requesting_user_id_str}_{story_id}")
            ]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await safe_edit_or_resend(query, context, message_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
            return EDIT_STORY_MAP

        logger.info(f"Parsed story_id: {story_id}, fragment_id: {target_fragment_id}, user_id: {requesting_user_id_str}")


        story_data = load_user_story(requesting_user_id_str, story_id)        
        # Определим владельца истории с учётом прав доступа
        user_id = int(requesting_user_id_str)
        owner_id_str = get_owner_id_or_raise(user_id, story_id, story_data)
        if not story_data or "fragments" not in story_data:
            await safe_edit_or_resend(query, context, "Ошибка: История или фрагменты не найдены.")
            return EDIT_STORY_MAP

        all_fragments = story_data["fragments"]

        if target_fragment_id not in all_fragments:
            await safe_edit_or_resend(query, context, f"Ошибка: Фрагмент <code>{target_fragment_id}</code> не найден.", parse_mode=ParseMode.HTML)
            return EDIT_STORY_MAP

        logging.info(f"🔍 Начинается поиск потомков для удаления от фрагмента {target_fragment_id}")

        potential_full_subtree = find_descendant_fragments(all_fragments, target_fragment_id)
        if not potential_full_subtree:
            await safe_edit_or_resend(query, context, f"Ошибка: Фрагмент <code>{target_fragment_id}</code> не найден или не имеет потомков для анализа.", parse_mode=ParseMode.HTML)
            return EDIT_STORY_MAP


        if target_fragment_id not in all_fragments:
            await safe_edit_or_resend(query, context, f"Ошибка: Целевой фрагмент <code>{target_fragment_id}</code> не найден перед формированием превью.", parse_mode=ParseMode.HTML)
            return EDIT_STORY_MAP

        fragments_preview_for_deletion, externally_referenced_in_subtree, descendants_to_list, bypass_reachable = get_fragments_for_deletion_preview(
            all_fragments=all_fragments,
            target_fragment_id=target_fragment_id,
            potential_full_subtree=potential_full_subtree,
            protected_fragment_id=PROTECTED_FRAGMENT_ID,
        )

        logging.info(f"📋 Финальный список фрагментов на удаление (без корневого): {descendants_to_list}")
        # --- КОНЕЦ БОЛЕЕ ТОЧНОГО РАСЧЕТА ---


        confirmation_text = f"Вы уверены, что хотите удалить фрагмент <code>{target_fragment_id}</code>?\n\n"
        
        if descendants_to_list:
            confirmation_text += "⚠️ Будут также удалены следующие связанные дочерние фрагменты (если на них нет других ссылок и путь к ним не прерывается защищенным фрагментом):\n"
            confirmation_text += "\n".join([f"- <code>{f}</code>" for f in descendants_to_list])
        elif len(potential_full_subtree) > 1 and target_fragment_id in fragments_preview_for_deletion and len(fragments_preview_for_deletion) == 1 : # Цель удаляется, но других нет
            confirmation_text += "(Других дочерних фрагментов для удаления нет, т.к. они защищены или имеют внешние ссылки)"
        elif len(potential_full_subtree) == 1 and target_fragment_id in fragments_preview_for_deletion: # Только сам целевой фрагмент
             confirmation_text += "(Дочерних фрагментов для удаления нет)"
        else: # Случай, когда target_fragment_id не будет удален (хотя это должно быть отловлено раньше, если он PROTECTED)
              # Или если fragments_preview_for_deletion пуст (не должно быть, если target_id валиден)
             confirmation_text += "(Нет фрагментов для удаления в этом поддереве согласно текущим правилам)"


        if target_fragment_id in externally_referenced_in_subtree and target_fragment_id in fragments_preview_for_deletion:
            confirmation_text += f"\n\n❗️Внимание: ссылки(кнопки) на удаляемые фрагменты так же будут удалены."

        confirmation_text += "\n\nЭто действие нельзя отменить."

        keyboard = [
            [
                InlineKeyboardButton("✅ Да, удалить", callback_data=f"{DELETE_FRAGMENT_EXECUTE_PREFIX}_{story_id}_{target_fragment_id}"),
                InlineKeyboardButton("❌ Нет, отмена", callback_data=f"edit_story_{owner_id_str}_{story_id}")
            ]
        ]


        # --- КЛЮЧЕВОЕ ИЗМЕНЕНИЕ ДЛЯ КАРТЫ ---
        # Используем УТОЧНЕННЫЙ набор highlight_ids (fragments_preview_for_deletion)
        highlight_set_for_map = fragments_preview_for_deletion

        total_fragments = len(all_fragments)
        # --- КОНЕЦ КЛЮЧЕВОГО ИЗМЕНЕНИЯ ---

        if query.message.from_user.is_bot:
            try:
                await query.message.delete()
            except Exception as e_del:
                logger.warning(f"Не удалось удалить сообщение перед подтверждением: {e_del}")

        if total_fragments > 20:
            confirmation_text += (
                "\n\n📌 История содержит более 20 фрагментов, схема не прикреплена к этому сообщению. "
                "Вы можете отдельно запросить её."
            )
            keyboard.insert(0, [  # Кнопка будет в отдельной строке над остальными
                InlineKeyboardButton("🗺️ Показать карту удаляемой ветки", callback_data=f"mapreq_{story_id}_{target_fragment_id}")
            ])

            reply_markup = InlineKeyboardMarkup(keyboard)           
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=confirmation_text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML
            )
        else:
            reply_markup = InlineKeyboardMarkup(keyboard)
            logging.info(f"highlight_set_for_map: {highlight_set_for_map}")            
            image_path = generate_story_map(story_id, story_data, highlight_set_for_map) # Передаем уточне_set
            if image_path and os.path.exists(image_path):
                try:
                    with open(image_path, 'rb') as photo_file:
                        await context.bot.send_photo(
                            chat_id=query.message.chat_id,
                            photo=photo_file,
                            caption=confirmation_text,
                            reply_markup=reply_markup,
                            parse_mode=ParseMode.HTML
                        )
                finally:
                    if os.path.exists(image_path):
                        os.remove(image_path)
            else: 
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text=confirmation_text + "\n\n(Не удалось сгенерировать карту для предварительного просмотра.)",
                    reply_markup=reply_markup,
                    parse_mode=ParseMode.HTML
                )
        
        context.user_data['fragment_to_delete'] = target_fragment_id
        context.user_data['story_id_for_delete'] = story_id
        context.user_data['user_id_for_delete'] = owner_id_str

        return EDIT_STORY_MAP

    except ValueError as ve:
        logger.warning(f"Ошибка обработки callback_data при подтверждении удаления: {ve}")
        if query: # query может быть None, если ошибка до его инициализации
             await safe_edit_or_resend(query, context, f"Ошибка данных для подтверждения: {ve}")
        return EDIT_STORY_MAP
    except Exception as e:
        logger.error(f"Ошибка в handle_delete_fragment_confirm: {e}", exc_info=True)
        if query:
            await safe_edit_or_resend(query, context, "Произошла ошибка при запросе на удаление фрагмента.")
        return EDIT_STORY_MAP





def get_fragments_for_deletion_preview(
    all_fragments: dict,
    target_fragment_id: str,
    potential_full_subtree: set,
    protected_fragment_id: str
) -> tuple[set, set, list, set]:
    logging.info(f"Найдено {len(potential_full_subtree)} потенциальных потомков фрагмента {target_fragment_id}")

    externally_referenced_in_subtree = set()
    for fid, fragment_content in all_fragments.items():
        if fid not in potential_full_subtree:
            for choice in fragment_content.get("choices", []):
                choice_target = choice.get("target")
                if choice_target in potential_full_subtree:
                    externally_referenced_in_subtree.add(choice_target)
                    logging.info(f"Фрагмент {choice_target} используется вне поддерева, из фрагмента {fid}")

    fragments_preview_for_deletion = set()
    preview_processing_stack = [target_fragment_id] if target_fragment_id in all_fragments else []
    visited_for_preview = set()

    bypass_reachable = set()
    for fragment_id in potential_full_subtree:
        if fragment_id != target_fragment_id:
            if can_reach_without_target(all_fragments, protected_fragment_id, {fragment_id}, target_fragment_id):
                bypass_reachable.add(fragment_id)
                logging.info(f"⚠️ Фрагмент {fragment_id} достижим напрямую из {protected_fragment_id}, минуя {target_fragment_id}")

    while preview_processing_stack:
        current_preview_f_id = preview_processing_stack.pop()

        if current_preview_f_id in visited_for_preview:
            continue
        visited_for_preview.add(current_preview_f_id)

        if current_preview_f_id == protected_fragment_id:
            logging.info(f"⛔ Пропущен защищённый фрагмент {current_preview_f_id}")
            continue

        protected_fragment = all_fragments.get(protected_fragment_id)
        if protected_fragment:
            for choice in protected_fragment.get("choices", []):
                choice_target = choice.get("target")
                if choice_target in potential_full_subtree:
                    externally_referenced_in_subtree.add(choice_target)
                    logging.info(f"Фрагмент {choice_target} используется в защищённом фрагменте {protected_fragment_id}")

        if current_preview_f_id in bypass_reachable:
            logging.info(f"⛔ Фрагмент {current_preview_f_id} не будет удалён — достижим напрямую из защищённого фрагмента")
            continue

        should_be_in_preview = (
            current_preview_f_id == target_fragment_id or 
            current_preview_f_id not in externally_referenced_in_subtree
        )

        if should_be_in_preview:
            reason = "это целевой фрагмент" if current_preview_f_id == target_fragment_id else "не используется вне поддерева"
            fragments_preview_for_deletion.add(current_preview_f_id)
            logging.info(f"✅ Фрагмент {current_preview_f_id} добавлен в список на удаление ({reason})")

            current_fragment_content = all_fragments.get(current_preview_f_id, {})
            direct_children_ids = [choice.get("target") for choice in current_fragment_content.get("choices", [])]

            for child_id in direct_children_ids:
                if child_id in visited_for_preview or child_id == protected_fragment_id:
                    continue
                if child_id in potential_full_subtree:
                    logging.info(f"🔽 Потомок {child_id} добавлен в стек на проверку")
                    preview_processing_stack.append(child_id)
                else:
                    logging.info(f"⚠️ Потомок {child_id} не входит в поддерево, пропущен")

    descendants_to_list = sorted([
        f for f in fragments_preview_for_deletion 
        if f != target_fragment_id
    ])
    logging.info(f"📋 Финальный список фрагментов на удаление (без корневого): {descendants_to_list}")

    return fragments_preview_for_deletion, externally_referenced_in_subtree, descendants_to_list, bypass_reachable



#==========================================================================




#==========================================================================




#КЛАВИАТУРЫ



from html.parser import HTMLParser
from bs4 import BeautifulSoup
class HTMLTextExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self.result = []

    def handle_data(self, d):
        self.result.append(d)

    def get_text(self):
        return ''.join(self.result)

def strip_html_tags(text: str) -> str:
    parser = HTMLTextExtractor()
    parser.feed(text)
    return parser.get_text()


def build_legend_text(story_data: dict, fragment_ids: list[str]) -> str:
    MEDIA_TYPES_RUS = {
        "photo": "фото",
        "video": "видео",
        "animation": "анимация",
        "audio": "аудио"
    }

    # Сортируем fragment_ids согласно новым правилам
    # sorted() создает новый отсортированный список, не изменяя оригинальный fragment_ids (если это важно)
    sorted_fragment_ids = sorted(fragment_ids, key=get_fragment_sort_key)

    fragments = story_data.get("fragments", {})
    lines = []
    for fragment_id in sorted_fragment_ids: # Используем отсортированный список
        fragment = fragments.get(fragment_id)
        if not fragment:
            continue

        line_parts = [f"<code>{fragment_id}</code> –"]
        media = fragment.get("media", [])
        media_summary = {}

        for item in media:
            media_type = item.get("type")
            if media_type in MEDIA_TYPES_RUS:
                media_summary[media_type] = media_summary.get(media_type, 0) + 1
        
        # logger.info(f"media_summary '{media_summary}'") # Закомментировано, если не используется
        if media_summary:
            media_str = ", ".join(f"{count} {MEDIA_TYPES_RUS.get(t)}" for t, count in media_summary.items())
            line_parts.append(media_str)

        text = fragment.get("text", "")
        # logger.info(f"text '{text}'") # Закомментировано, если не используется
        if text:
            trimmed_html = trim_html_preserving_tags(text, 25)
            clean_text = strip_html_tags(trimmed_html)
            clean_text = html.escape(clean_text)
            line_parts.append(f"«{clean_text}»" + ("…" if len(strip_html_tags(text)) > 30 else ""))


        lines.append(" ".join(line_parts))

    return "\n".join(lines) if lines else ""

def trim_html_preserving_tags(html: str, max_length: int) -> str:
    """Обрезает HTML-текст, не ломая теги и сохраняя валидный HTML"""
    soup = BeautifulSoup(html, 'html.parser')
    current_length = 0
    result_fragments = []

    def traverse(node):
        nonlocal current_length
        if current_length >= max_length:
            return None

        if isinstance(node, str):
            remaining = max_length - current_length
            piece = node[:remaining]
            current_length += len(piece)
            return piece

        elif node.name:
            new_node = soup.new_tag(node.name, **node.attrs)
            for child in node.contents:
                child_trimmed = traverse(child)
                if child_trimmed is None:
                    break
                new_node.append(child_trimmed)
            return new_node

    trimmed_content = []
    for element in soup.contents:
        trimmed = traverse(element)
        if trimmed:
            trimmed_content.append(trimmed)

    return ''.join(str(x) for x in trimmed_content)

def clean_html_for_telegram(html_text: str) -> str:
    allowed_tags = {
        "b", "strong", "i", "em", "u", "ins",
        "s", "strike", "del", "code", "pre", "a", "span",
        "blockquote", "expandable_blockquote"
    }

    # Парсим HTML
    soup = BeautifulSoup(html_text, "html.parser")

    for tag in soup.find_all(True):
        tag_name = tag.name

        if tag_name not in allowed_tags:
            tag.unwrap()
        elif tag_name == "span":
            # Разрешаем только <span class="tg-spoiler">
            if tag.get("class") != ["tg-spoiler"]:
                tag.unwrap()
        elif tag_name == "a":
            # Удаляем <a> без href
            if not tag.get("href"):
                tag.unwrap()

    return str(soup)


def build_fragment_action_keyboard(
    fragment_id: str,
    story_data: dict,
    user_id_str: str,
    story_id: str
) -> InlineKeyboardMarkup:

    current_choices = []
    if story_data and "fragments" in story_data and fragment_id in story_data["fragments"]:
        current_choices = story_data["fragments"][fragment_id].get("choices", [])
    else:
        logger.warning(f"Fragment {fragment_id} not found in story_data while building keyboard.")

    keyboard = [
        [InlineKeyboardButton("🦊 Предпросмотр фрагмента", callback_data=f"preview_fragment_{fragment_id}")],
    ]

    has_choices = len(current_choices) > 0

    # --- Верхняя строка с "⬅️ Шаг назад" и первой кнопкой (если есть) ---
    if fragment_id != "main_1":
        row = [InlineKeyboardButton("⬅️ Шаг назад", callback_data=f'prev_fragment_{fragment_id}')]
        if has_choices:
            first_choice = current_choices[0]
            choice_text = first_choice["text"]
            target_fragment_id = first_choice["target"]
            row.append(InlineKeyboardButton(f"➡️Шаг вперёд: {choice_text}", callback_data=f'goto_{target_fragment_id}'))
        keyboard.append(row)
    elif has_choices:
        # Только "следующий фрагмент" без "назад"
        first_choice = current_choices[0]
        choice_text = first_choice["text"]
        target_fragment_id = first_choice["target"]
        keyboard.append([
            InlineKeyboardButton(f"➡️Шаг вперёд: {choice_text}", callback_data=f'goto_{target_fragment_id}')
        ])

    # --- Кнопки добавления переходов ---
    branch_button_text = "➕ Добавить тут вариант развилки" if has_choices else "➕ Добавить вариант выбора (развилку)"
    
    match = re.match(r"(.+?)_(\d+)$", fragment_id)
    if match:
        prefix, number = match.groups()
        next_fragment_id = f"{prefix}_{int(number) + 1}"
        if next_fragment_id in story_data.get("fragments", {}):
            continue_button_text = f"➡️✏️Вставить после {fragment_id} событие"
            continue_callback = f"continue_linear"
        else:
            continue_button_text = "➡️➡️ Продолжить ветку линейно"
            continue_callback = 'continue_linear'
    else:
        continue_button_text = "➡️➡️ Продолжить ветку линейно"
        continue_callback = 'continue_linear'

    keyboard.extend([
        [InlineKeyboardButton(continue_button_text, callback_data=continue_callback)],
        [InlineKeyboardButton(branch_button_text, callback_data='add_branch')],
        [InlineKeyboardButton("🔗 Связать с другим", callback_data='link_to_previous')],
    ])

    if len(current_choices or []) > 1:
        keyboard.append([
            InlineKeyboardButton("🗑️ Удалить связь", callback_data=f"d_c_s_{fragment_id}")
        ])

    if current_choices:
        keyboard.append([InlineKeyboardButton("━━━━━━━━━━ ✦ ━━━━━━━━━━", callback_data='separator_transitions_header')])

        if len(current_choices or []) > 1:
            keyboard.append([InlineKeyboardButton("🔀 ----- Существующие переходы: -----",
                                                  callback_data=f"{REORDER_CHOICES_START_PREFIX}{fragment_id}")])
        else:
            keyboard.append([InlineKeyboardButton("----- Существующие переходы: -----",
                                                  callback_data='noop_transitions_header')])

        rows = []
        for i in range(0, len(current_choices), 2):
            row = []
            for choice in current_choices[i:i + 2]:
                choice_text = choice["text"]
                target_fragment_id = choice["target"]
                row.append(InlineKeyboardButton(f"'{choice_text}' ➡️ {target_fragment_id}", callback_data=f'goto_{target_fragment_id}'))
            rows.append(row)
        keyboard.extend(rows)

        keyboard.append([
            InlineKeyboardButton("✏️ Редактировать текст кнопок", callback_data=f'edit_choice_start_{fragment_id}')
        ])
        keyboard.append([InlineKeyboardButton("━━━━━━━━━━ ✦ ━━━━━━━━━━", callback_data='separator')])

    keyboard.append([
        InlineKeyboardButton(
            "🛠️ Открыть веб редактор",
            web_app=WebAppInfo(url=f"https://novel-qg4c.onrender.com/{user_id_str}_{story_id}")
        )
    ])
    
    # --- Завершающие кнопки ---
    keyboard.append([
        InlineKeyboardButton("🗺️ Карта/Редактировать фрагменты", callback_data=f"edit_story_{user_id_str}_{story_id}")
    ])
    keyboard.append([InlineKeyboardButton("🌃 Сохранить и выйти", callback_data='finish_story')])

    return InlineKeyboardMarkup(keyboard)





def build_branch_fragments_keyboard(
    user_id_str: str, 
    story_id: str, 
    branch_name: str,
    branch_fragment_ids: list[str], 
    current_page: int, 
    story_data: dict # Может понадобиться для каких-то общих данных истории
) -> InlineKeyboardMarkup:
    """
    Строит InlineKeyboardMarkup для списка фрагментов конкретной ветки с пагинацией.
    """
    # Сортировка фрагментов ветки
    # (предполагается, что branch_fragment_ids уже отсортированы или get_fragment_sort_key их обработает)
    sorted_branch_fragment_ids = sorted(branch_fragment_ids, key=get_fragment_sort_key)

    total_fragments_in_branch = len(sorted_branch_fragment_ids)
    # Используем глобальные константы для пагинации, можно завести отдельные для веток
    total_pages = math.ceil(total_fragments_in_branch / FRAGMENT_BUTTONS_PER_PAGE) if total_fragments_in_branch > 0 else 0

    keyboard = []

    # Кнопка "Карта этой ветки"
    # Префикс для callback, чтобы его можно было обработать отдельно, если карта генерируется по кнопке
    SHOW_BRANCH_MAP_PREFIX = "show_bmap_" # Отличается от show_map_ для всей истории
    if total_fragments_in_branch > 0 : # Показывать кнопку карты, если есть что показывать
         keyboard.append([
             InlineKeyboardButton(f"🗺️ Карта ветки '{branch_name}'", callback_data=f"{SHOW_BRANCH_MAP_PREFIX}{story_id}_{branch_name}")
         ])


    # Кнопки редактирования/удаления фрагментов ветки
    if total_fragments_in_branch > 0:
        start_index = (current_page - 1) * FRAGMENT_BUTTONS_PER_PAGE
        end_index = start_index + FRAGMENT_BUTTONS_PER_PAGE
        fragments_on_page = sorted_branch_fragment_ids[start_index:end_index]

        row = []
        for i, fragment_id in enumerate(fragments_on_page):
            # Префиксы для редактирования/удаления остаются теми же, т.к. они глобальны по story_id и fragment_id
            edit_button_data = f"e_f_{story_id}_{fragment_id}" 
            row.append(InlineKeyboardButton(f"✏️Ред: {fragment_id}", callback_data=edit_button_data))

            # Убедитесь, что DELETE_FRAGMENT_CONFIRM_PREFIX определен
            delete_button_data = f"{DELETE_FRAGMENT_CONFIRM_PREFIX}{story_id}_{fragment_id}"
            row.append(InlineKeyboardButton("🗑️ Удалить", callback_data=delete_button_data))
            
            # PAIRS_PER_ROW должна быть определена
            if (i + 1) % PAIRS_PER_ROW == 0: # PAIRS_PER_ROW - сколько пар (Ред+Уд) в ряду
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)

        # Пагинация для фрагментов ветки
        if total_pages > 1:
            pagination_row = []
            P_BF_PREFIX = "p_bf_" # Page Branch Fragment
            
            if current_page > 1:
                pagination_row.append(InlineKeyboardButton("«", callback_data=f"{P_BF_PREFIX}{user_id_str}_{story_id}_{branch_name}_{current_page - 1}"))
            else:
                pagination_row.append(InlineKeyboardButton(" ", callback_data="ignore")) # Пустышка для выравнивания

            pagination_row.append(InlineKeyboardButton(f"{current_page}/{total_pages}", callback_data=f"page_info_branch_{user_id_str}_{story_id}_{branch_name}_{current_page}")) # page_info_branch_ для информации о странице ветки

            if current_page < total_pages:
                pagination_row.append(InlineKeyboardButton("»", callback_data=f"{P_BF_PREFIX}{user_id_str}_{story_id}_{branch_name}_{current_page + 1}"))
            else:
                pagination_row.append(InlineKeyboardButton(" ", callback_data="ignore"))

            keyboard.append(pagination_row)
    else:
        keyboard.append([InlineKeyboardButton("В этой ветке пока нет фрагментов.", callback_data="ignore")])


    # Кнопки навигации
    # Назад к списку веток (show_branches_ ожидает user_id, story_id, page)
    keyboard.append([InlineKeyboardButton("◀️ К списку веток", callback_data=f"show_branches_{user_id_str}_{story_id}_1")])
    # Опционально: Назад к общему редактированию истории (edit_story_ ожидает user_id, story_id)
    # keyboard.append([InlineKeyboardButton("⏪ К редактированию истории", callback_data=f"edit_story_unused_{user_id_str}_{story_id}")]) # edit_story_ ожидает callback 'edit_story_action_user_story'
    keyboard.append([InlineKeyboardButton("🌃В Главное Меню🌃", callback_data='restart_callback')]) # Или restart_callback

    return InlineKeyboardMarkup(keyboard)




def get_fragment_sort_key(fragment_id: str):
    """
    Создает ключ сортировки для идентификатора фрагмента.
    Правила сортировки:
    1. "main_X" фрагменты идут первыми, сортируются по X (число).
    2. Остальные "text_Y" фрагменты идут следующими, сортируются по "text" (алфавитно), затем по Y (число).
    """
    if fragment_id.startswith("main_"):
        parts = fragment_id.split("_", 1)
        if len(parts) == 2 and parts[1].isdigit():
            # (приоритет для main, числовая часть, пустая строка для сравнения кортежей одинаковой длины)
            return (0, int(parts[1]), "")
        else:
            # Некорректный формат main_ (например, "main_abc" или "main_")
            # Обрабатываем как обычный фрагмент, чтобы он попал в общую группу
            return (1, fragment_id, 0) # Сортируем по полному ID

    # Для остальных фрагментов вида "ТЕКСТ_ЧИСЛО"
    try:
        text_part, num_str = fragment_id.rsplit('_', 1) # rsplit отделяет по последнему '_'
        num_part = int(num_str)
        # (приоритет для не-main, текстовая часть, числовая часть)
        return (1, text_part.lower(), num_part) # .lower() для регистронезависимой сортировки текста
    except ValueError:
        # Фрагменты, не соответствующие формату "ТЕКСТ_ЧИСЛО" или "main_ЧИСЛО"
        # (например, "простотекст", "текст_без_числа_вконце")
        # Сортируем их по полному ID в общей группе не-main фрагментов
        return (1, fragment_id.lower(), 0)



# --- Убедитесь, что эти константы определены где-то ---
FRAGMENT_BUTTONS_PER_PAGE = 16 # Пример: сколько фрагментов показывать на одной странице
# --- Константа для компоновки ---
PAIRS_PER_ROW = 1 # Сколько пар кнопок (Редакт.+Удалить) помещать в один ряд

def build_fragment_keyboard(
    user_id_str: str,
    story_id: str,
    fragment_ids: list[str],
    current_page: int,
    story_data: dict,
    legend_too_long: bool = False
) -> 'InlineKeyboardMarkup':
    """
    Строит InlineKeyboardMarkup для списка фрагментов с учетом пагинации,
    кнопками редактирования/удаления, публичности и скачивания.
    story_data - словарь с данными текущей истории.
    """
    sorted_fragment_ids = sorted(fragment_ids, key=get_fragment_sort_key)

    total_fragments = len(sorted_fragment_ids)
    total_pages = math.ceil(total_fragments / FRAGMENT_BUTTONS_PER_PAGE) if total_fragments > 0 and FRAGMENT_BUTTONS_PER_PAGE > 0 else 0

    keyboard = []
    keyboard.append([
        InlineKeyboardButton(f"▶️ Воспроизвести эту историю", callback_data=f"nstartstory_{user_id_str}_{story_id}_main_1"),
    ])

    keyboard.append([
        InlineKeyboardButton("👥 Совместное редактирование", callback_data=f"coop_edit_menu_{user_id_str}_{story_id}")
    ])


    keyboard.append([
        InlineKeyboardButton("🧠 Нейро-помощник", callback_data=f"neurohelper_{user_id_str}_{story_id}_1")
    ])    

    # --- Кнопка "нейрорежим" ---
    if story_data.get("neuro_fragments", False):
        keyboard.append([
            InlineKeyboardButton("🚫 Выключить нейрорежим", callback_data=f"{DISABLE_NEURO_MODE_PREFIX}{user_id_str}_{story_id}")
        ])
    else:
        keyboard.append([
            InlineKeyboardButton("🤖 Включить нейрорежим", callback_data=f"{ENABLE_NEURO_MODE_PREFIX}{user_id_str}_{story_id}")
        ])

    keyboard.append([
        InlineKeyboardButton("🌿 Показать ветки", callback_data=f"show_branches_{user_id_str}_{story_id}_1")
    ])
    # --- Кнопка "Посмотреть карту" на отдельной строке ---


    # --- Кнопки "публичность" и "скачать историю" ---
    top_action_row = []
    if story_data.get("public", False):
        top_action_row.append(InlineKeyboardButton("🚫 Убрать из публичных", callback_data=f"{MAKE_PRIVATE_PREFIX}{user_id_str}_{story_id}"))
    else:
        top_action_row.append(InlineKeyboardButton("🌍 Сделать публичной", callback_data=f"{MAKE_PUBLIC_PREFIX}{user_id_str}_{story_id}"))

    top_action_row.append(InlineKeyboardButton("💾 Скачать историю", callback_data=f"{DOWNLOAD_STORY_PREFIX}{user_id_str}_{story_id}"))

    if top_action_row:
        keyboard.append(top_action_row)


    # --- Кнопки редактирования фрагментов ---
    if total_fragments > 0:
        start_index = (current_page - 1) * FRAGMENT_BUTTONS_PER_PAGE
        end_index = start_index + FRAGMENT_BUTTONS_PER_PAGE
        fragments_on_page = sorted_fragment_ids[start_index:end_index]

        row = []
        for i, fragment_id in enumerate(fragments_on_page):
            edit_button_data = f"e_f_{story_id}_{fragment_id}" 
            row.append(InlineKeyboardButton(f"✏️Ред: {fragment_id}", callback_data=edit_button_data))

            delete_button_data = f"{DELETE_FRAGMENT_CONFIRM_PREFIX}_{story_id}_{fragment_id}"
            row.append(InlineKeyboardButton("🗑️ Удалить", callback_data=delete_button_data))

            if (i + 1) % PAIRS_PER_ROW == 0:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)

        # --- Пагинация ---
        if total_pages > 1:
            pagination_row = []
            if current_page > 1:
                pagination_row.append(InlineKeyboardButton("«", callback_data=f"p_f_{user_id_str}_{story_id}_{current_page - 1}"))
            else:
                pagination_row.append(InlineKeyboardButton(" ", callback_data="ignore_"))

            pagination_row.append(InlineKeyboardButton(f"{current_page}/{total_pages}", callback_data=f"page_info_{user_id_str}_{story_id}_{current_page}"))

            if current_page < total_pages:
                pagination_row.append(InlineKeyboardButton("»", callback_data=f"p_f_{user_id_str}_{story_id}_{current_page + 1}"))
            else:
                pagination_row.append(InlineKeyboardButton(" ", callback_data="ignore_"))

            keyboard.append(pagination_row)

    keyboard.append([
        InlineKeyboardButton("🗺️ Посмотреть карту", callback_data=f"show_map_{story_id}")
    ])
    keyboard.append([
        InlineKeyboardButton(
            "🛠️ Визуальный веб редактор и карта",
            web_app=WebAppInfo(url=f"https://novel-qg4c.onrender.com/{user_id_str}_{story_id}")
        )
    ])      
    # --- Кнопки навигации ---
    keyboard.append([InlineKeyboardButton("❔ Помощь по этому окну", callback_data="edithelp")])    
    keyboard.append([InlineKeyboardButton("◀️ Назад к списку историй", callback_data="view_stories")])
    keyboard.append([InlineKeyboardButton("🌃В Главное Меню🌃", callback_data='restart_callback')])

    return InlineKeyboardMarkup(keyboard)





def build_neuro_fragment_keyboard(user_id_str: str, story_id: str, fragment_ids: list[str], current_page: int) -> InlineKeyboardMarkup:
    """
    Строит клавиатуру с кнопками редактирования фрагментов (без удаления),
    кнопки располагаются по 2 в строке, всего 16 кнопок на страницу.
    В конце добавляется кнопка "❌ Отмена".
    """
    FRAGMENT_BUTTONS_PER_PAGE = 16
    BUTTONS_PER_ROW = 2

    sorted_fragment_ids = sorted(fragment_ids, key=get_fragment_sort_key)

    total_fragments = len(sorted_fragment_ids)
    total_pages = math.ceil(total_fragments / FRAGMENT_BUTTONS_PER_PAGE) if total_fragments > 0 else 0

    keyboard = []

    if total_fragments > 0:
        start_index = (current_page - 1) * FRAGMENT_BUTTONS_PER_PAGE
        end_index = start_index + FRAGMENT_BUTTONS_PER_PAGE
        fragments_on_page = sorted_fragment_ids[start_index:end_index]

        row = []
        for i, fragment_id in enumerate(fragments_on_page):
            neuro_fragment_data = f"neuro_{story_id}_{fragment_id}" 
            row.append(InlineKeyboardButton(f"{fragment_id}", callback_data=neuro_fragment_data))

            if (i + 1) % BUTTONS_PER_ROW == 0:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)

        # --- Пагинация ---
        if total_pages > 1:
            pagination_row = []
            if current_page > 1:
                pagination_row.append(InlineKeyboardButton("«", callback_data=f"npf_{user_id_str}_{story_id}_{current_page - 1}"))
            else:
                pagination_row.append(InlineKeyboardButton(" ", callback_data="ignore_"))

            pagination_row.append(InlineKeyboardButton(f"{current_page}/{total_pages}", callback_data=f"page_info_{user_id_str}_{story_id}_{current_page}"))

            if current_page < total_pages:
                pagination_row.append(InlineKeyboardButton("»", callback_data=f"npf_{user_id_str}_{story_id}_{current_page + 1}"))
            else:
                pagination_row.append(InlineKeyboardButton(" ", callback_data="ignore_"))

            keyboard.append(pagination_row)

    # --- Кнопка отмены ---
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="delete_this_message")])

    return InlineKeyboardMarkup(keyboard)




#==========================================================================
#СНОВНАЯ ЛОГИКА

def parse_timed_edits(text):
    steps = []
    # Обновлённый паттерн: поддержка [[+2]] и ((-4))
    pattern = re.compile(r"(\[\[|\(\()([+-])(\d+)(\]\]|\)\))")
    matches = list(pattern.finditer(text))

    for idx, match in enumerate(matches):
        symbol, raw_seconds = match.group(2), match.group(3)
        delay = min(int(raw_seconds), 60)
        start = match.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        content = text[start:end]
        if symbol == "-" and not content.strip():
            continue
        steps.append({
            "delay": delay,
            "mode": symbol,
            "text": content,
            "insert_at": start
        })

    return steps


async def run_timed_edits(bot, chat_id, message_id, original_text, steps, is_caption, user_id_str, story_id):
    current_text = original_text
    for step in steps:
        await sleep(step["delay"])
        if step["mode"] == "+":
            insert_text = step["text"]
            pos = step["insert_at"]
            current_text = current_text[:pos] + insert_text + current_text[pos:]
        elif step["mode"] == "-":
            current_text = step["text"]


        # Создаём кнопки
        buttons = [
            [InlineKeyboardButton(
                "▶️ Воспроизвести историю отсюда",
                callback_data=f"nstartstory_{user_id_str}_{story_id}_main_1"
            )],
            [InlineKeyboardButton("❌ Закрыть это окно", callback_data="delete_this_message")]
        ]

        try:
            if is_caption:
                await bot.edit_message_caption(
                    chat_id=chat_id,
                    message_id=message_id,
                    caption=current_text.strip(),
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(buttons)
                )
            else:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=current_text.strip(),
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(buttons)
                )
        except Exception as e:
            print(f"Ошибка при редактировании: {e}")
            break



async def toggle_story_public_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int | None:
    """Обрабатывает нажатие кнопок 'Сделать публичной' / 'Убрать из публичных'."""
    query = update.callback_query


    callback_data = query.data

    try:
        parts = callback_data.split('_')
        logger.info(f"parts: {parts}")        
        action_prefix_part = '_'.join(parts[:2]) + '_'  # 'mk_pub_'
        user_id_from_callback = parts[2]
        story_id_from_callback = parts[3]

        if str(update.effective_user.id) != user_id_from_callback:
            await update.callback_query.answer(
                text="Вы не можете изменить статус этой истории. Обратитесь к владельцу.",
                show_alert=True
            )
            return None

        story_data = load_user_story(user_id_from_callback, story_id_from_callback)

        if not story_data:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="Ошибка: История не найдена.")
            return None

        made_public_now = False
        action_taken = False

        if action_prefix_part == MAKE_PUBLIC_PREFIX and not story_data.get("public", False):
            story_data["public"] = True
            user = update.effective_user
            user_name = user.username or user.first_name or f"User_{user_id_from_callback}"
            if user.first_name and user.last_name:
                user_name = f"{user.first_name} {user.last_name}"
            story_data["user_name"] = user_name
            save_story_data(user_id_from_callback, story_id_from_callback, story_data)
            logger.info(f"История {story_id_from_callback} (user: {user_id_from_callback}) сделана публичной. Автор: {user_name}.")
            await query.answer("✅ История сделана публичной! Теперь её видно в списке общих историй", show_alert=True)
            made_public_now = True
            action_taken = True

        elif action_prefix_part == MAKE_PRIVATE_PREFIX and story_data.get("public", False):
            story_data["public"] = False
            save_story_data(user_id_from_callback, story_id_from_callback, story_data)
            logger.info(f"История {story_id_from_callback} (user: {user_id_from_callback}) убрана из публичных.")
            await query.answer("ℹ️ История убрана из публичных.", show_alert=True)
            action_taken = True


        if not action_taken:
            logger.warning(f"Действие {action_prefix_part} не применимо к текущему состоянию истории {story_id_from_callback}.")
            await context.bot.send_message(chat_id=update.effective_chat.id, text="Статус истории не изменен (возможно, она уже в нужном состоянии).")

        context.user_data['current_story'] = story_data
        fragment_ids = sorted(story_data.get("fragments", {}).keys())
        current_page = context.user_data.get('current_fragment_page', 1)

        reply_markup = build_fragment_keyboard(user_id_from_callback, story_id_from_callback, fragment_ids, current_page, story_data)
        try:
            if query.message.photo or query.message.document:
                await query.edit_message_reply_markup(reply_markup=reply_markup)
            else:
                await query.edit_message_reply_markup(reply_markup=reply_markup)
        except BadRequest as e:
            logger.error(f"Не удалось обновить reply_markup для {story_id_from_callback}: {e}. Попытка edit_message_text.")
            current_text = query.message.text or query.message.caption
            if current_text:
                await query.edit_message_text(text=current_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML if query.message.caption else None)
            else:
                logger.error(f"Не удалось получить текст для edit_message_text для {story_id_from_callback}")
                await context.bot.send_message(chat_id=update.effective_chat.id, text="Не удалось обновить сообщение с историей.")

        return context.user_data.get('current_conversation_state', EDIT_STORY_MAP)

    except Exception as e:
        logger.exception(f"Ошибка в toggle_story_public_status для data {callback_data}:")
        await query.answer("Произошла ошибка при изменении статуса истории.")
        try:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="Произошла ошибка при изменении статуса истории.")
        except Exception as e_inner:
            logger.error(f"Не удалось даже отправить сообщение об ошибке: {e_inner}")
        return context.user_data.get('current_conversation_state', EDIT_STORY_MAP)



async def download_story_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int | None:
    """Обрабатывает нажатие кнопки 'Скачать историю'."""
    query = update.callback_query
    callback_data = query.data

    try:
        # callback_data: dl_story_USERID_STORYID
        parts = callback_data.split('_', 3)
        user_id_from_callback = parts[2]
        story_id_from_callback = parts[3]
        current_user_id = str(update.effective_user.id)

        # Загружаем историю напрямую по owner_id (user_id_from_callback)

        story_data = load_user_story(user_id_from_callback, story_id_from_callback)


        if not story_data:
            await query.edit_message_text("История не найдена.")
            return context.user_data.get('current_conversation_state', EDIT_STORY_MAP)







        await query.answer("История найдена, готовлю файл...")



        story_json = json.dumps(story_data, ensure_ascii=False, indent=4)
        json_bytes = story_json.encode('utf-8')
        
        file_to_send = BytesIO(json_bytes)
        filename = f"story_{story_id_from_callback}.json"

        await context.bot.send_document(
            chat_id=update.effective_chat.id,
            document=file_to_send,
            filename=filename,
            caption=f"JSON файл для истории \"{story_data.get('title', story_id_from_callback)}\""
        )
        logger.info(f"История {story_id_from_callback} (user: {user_id_from_callback}) отправлена как JSON.")
        
        return context.user_data.get('current_conversation_state', EDIT_STORY_MAP)

    except Exception as e:
        logger.exception(f"Ошибка в download_story_handler для data {callback_data}:")
        await query.answer("Произошла ошибка при подготовке файла.")
        await context.bot.send_message(chat_id=update.effective_chat.id, text="Произошла ошибка при скачивании истории.")
        return context.user_data.get('current_conversation_state', EDIT_STORY_MAP)







async def toggle_neuro_mode(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int | None:
    """Обрабатывает нажатие кнопок включения/отключения нейрорежима."""
    query = update.callback_query
    callback_data = query.data

    try:
        parts = callback_data.split('_')
        action_prefix = '_'.join(parts[:2]) + '_'
        user_id_from_callback = parts[2]
        story_id_from_callback = parts[3]

        if str(update.effective_user.id) != user_id_from_callback:
            await query.answer("Вы не можете изменить режим этой истории. Обратитесь к владельцу истории.", show_alert=True)
            return None

        story_data = load_user_story(user_id_from_callback, story_id_from_callback)

        if not story_data:
            await query.answer("Ошибка: История не найдена.", show_alert=True)
            return None

        changed = False
        if action_prefix == ENABLE_NEURO_MODE_PREFIX and not story_data.get("neuro_fragments", False):
            story_data["neuro_fragments"] = True
            await query.answer("🤖 Нейрорежим включён. Теперь пустые фрагменты вашей истории будут генерироваться автоматически.", show_alert=True)
            changed = True
        elif action_prefix == DISABLE_NEURO_MODE_PREFIX and story_data.get("neuro_fragments", False):
            story_data["neuro_fragments"] = False
            await query.answer("🧠 Нейрорежим выключен. Теперь пустые фрагменты вашей истории вы можете добавить только самостоятельно.", show_alert=True)
            changed = True
        else:
            await query.answer("Режим уже установлен.", show_alert=True)

        if changed:
            save_story_data(user_id_from_callback, story_id_from_callback, story_data)
        else:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="Режим уже находится в нужном состоянии.")

        # Перерисуем клавиатуру
        context.user_data['current_story'] = story_data
        fragment_ids = sorted(story_data.get("fragments", {}).keys())
        current_page = context.user_data.get('current_fragment_page', 1)

        reply_markup = build_fragment_keyboard(user_id_from_callback, story_id_from_callback, fragment_ids, current_page, story_data)
        try:
            await query.edit_message_reply_markup(reply_markup=reply_markup)
        except BadRequest as e:
            logger.error(f"Не удалось обновить reply_markup: {e}")
            current_text = query.message.text or query.message.caption
            if current_text:
                await query.edit_message_text(text=current_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML if query.message.caption else None)
        return context.user_data.get('current_conversation_state', EDIT_STORY_MAP)

    except Exception as e:
        logger.exception("Ошибка при переключении нейрорежима:")
        await query.answer("Произошла ошибка при переключении нейрорежима.")
        return context.user_data.get('current_conversation_state', EDIT_STORY_MAP)




async def edithelp_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int | None:
    """Обработчик нажатия на кнопку для вызова помощи по окну редактирования."""
    query = update.callback_query
    await query.answer()

    help_text = """
<b>Помощь по редактированию истории 🛠</b>

id истории — вы можете использовать его для создания кнопки, ведущей прямиком на данную историю и автоматически запускающей её из любого чата или группы Telegram. Просто введите:
<pre>@FoxNovel_bot id_истории</pre>
И нажмите на кнопку, выпавшую в предложенном меню. Либо отправьте id любому человеку — он может просто переслать его боту, и история тут же запустится.

<b>Кнопки:</b>
• ✏️ Ред — редактировать конкретный фрагмент.
• 🗑️ Удалить — удалить фрагмент, а также цепочки, ставшие пустыми.
• 🧠 Включить нейрорежим — при воспроизведении данной истории все недостающие фрагменты будут автоматически генерироваться при переходах к ним и заноситься в историю для дальнейшего редактирования.
• 🗺️ Посмотреть карту — появляется при большой истории. Генерируется файл с её структурой.
• 🌿 Посмотреть ветки — редактировать отдельную ветку, а не всю историю.
• 💾 Скачать историю — сохранить файл. В будущем планируется возможность загрузки по файлу.
• 🌍 Сделать публичной — отправить историю в раздел "Общие истории" главного меню.
• 🛠️ Визуальный веб редактор — открывает редактирование и отображение вашей истории через удобный визуальный веб-редактор
"""

    await query.message.reply_text(
        help_text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Закрыть", callback_data="delete_this_message")]
        ])
    )







async def linkhelp_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int | None:

    """Обработчик помощи, вызываемый через /help или кнопку."""
    if update.message:
        # Вызов через команду /help
        target = update.message
    elif update.callback_query:
        # Вызов через нажатие кнопки
        query = update.callback_query
        await query.answer()
        target = query.message
    else:
        return

    help_text = """
▶️Если вместо текста вы отправите число от 0.1 до 90000, то в таком случае кнопки у <u>текущего</u> фрагмента не будет, вместо этого он переключится дальше автоматически через время равное заданному числу в секундах. 

▶️Кроме того в двойных фигурных скобках формата <code>{{фраза:значение}}</code> вы можете указать различные атрибуты истории, такие как харакстеристики персонажа, наличие предмета и прочие меняющиеся в ходе истории данные.
Возможные знаки:
"+", "-", "=", "&lt;", "&gt;"

<b>Примеры:</b>
🔸<code>Взять книгу{{книга}}</code> - Создаёт кнопку "Взять книгу" и задаёт ей атрибут "книга" на значение по-умолчанию (поскольку ничего не указано после фразы). Это всегда единица если не указано иного. То есть <code>{{книга:1}}</code> - ровно то же самое
🔸<code>Выпить зелье{{Сила:4}}</code> - Создаёт кнопку "Выпить зелье" и задаёт ей атрибут "Сила" на значение 4(удаляя любое прошлое значение если оно было или как-то формировалось до этого в истории)
🔸<code>Накинуть капюшон{{Подозрения стражи:+3}}</code> - Создаёт кнопку "Накинуть капюшон" и меняет атрибут "Подозрения стражи" на +3 относительно прошлого значения. Если прошлого значения не было, то используется значение по-умолчанию - ноль. То есть значение становится 3.
🔸<code>Прочитать надпись{{Интеллект:>3}}</code> - Создаёт кнопку "Прочитать надпись" и проверяет соотвествие атрибута "Интеллект" для данной кнопки. Если оно не больше 3х, то бот не даст нажать на кнопку к которой этот атрибут относится выдав пользователю сообщение о том что ему не хватает значения атрибута
▶️Если вы укажете несколько проверок на одну кнопку, то пользователь должен пройти все из них чтобы нажать на кнопку. 

▶️Для любой кнопки вы можете задать несколько проверок или изменений. Проверки или изменения осуществляются в указанном вами порядке. Вы можете указать каждый атрибут и/или проверку в отдельных двойных фигурных скобках, а можете перечислить через запятую в рамках одной конструкции.
<b>Примеры:</b>
🔸<code>Поспорить{{Интеллект:>5}}{{Риторика:>7}}{{Знания:+3}}</code> - Создаёт кнопку  "Поспорить", которая проверяет сначала соответствие интеллекту, затем риторике, затем в случае успеха меняет значение "знаний" увеличивая их на 3
🔸<code>Начать историю{{Интеллект:4, Сила:5, Магия:3, ловкость:6}}</code> - Задаёт перечнь начальных характеристик которые в дальнейшем можно менять по ходу истории. Уместно так делать в самом начале

▶️Если атрибутов несколько то они будут применяться именно в той последовательности в какой вы их прпишите
<b>Примеры:</b>
🔸<code>Попытаться открыть {{Ловкость: +3, Ловкость: >9}}</code> - сначала изменит атрибут "ловкость" на +3 а затем выполнит проверку соответвия. Если проверка неудачна то пользователь сможет нажать ещё раз, но будут учитыываться уже обновлённые значения "ловкости"
🔸<code>Попытаться открыть {{Ловкость: >9, Ловкость: +3}}</code> - сначала проверит соответвие и прибавит +3 только если проверка будет успешной.

▶️Пользователь будет получать уведомления о меняющихся характеристиках или о том что проверка на один из них не сработала.


▶️Если вы не хотите чтобы пользователь получал уведомления, то после числа вы можете написать <code>(hide)</code>, тогда никаких уведомлений пользователь получать не будет. 
<b>Примеры:</b>
🔸<code>Молча уйти{{отношение:-5(hide)}}</code> - Создаёт кнопку "Молча уйти" меняющую атрибут отношения на "-5", пользователь при этом не получает никаких уведомлений и не знает о изменениях.
🔸<code>Прочитать книгу{{Интеллект:>5(hide)}}</code> - Проверяет соответвие требованиям атрибута "Интеллект". Если в данный момент этот атрибут у пользователя 5 или меньше то кнопка даже не появится в перечне кнопок. Если при этом пользователю недоступны все кнопки из-за несоответвия атрибутам, то он получит сообщение "Похоже, на этом ваша история завершилась...но возможно тут были и иные варианты?"

▶️Будьте внимательны и не допускайте ошибки в названиях атрибутов. Например "Использовать" и "Исползовать" - в коде будут разными атрибутами и логика вашей истории из-за этого сломается. Кроме того не забывайте про двоеточие и запятые при указании атрибутов

"""
    await target.reply_text(
        help_text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📔Простое обучение", callback_data='play_000_001_main_1')],
            [InlineKeyboardButton("📚Продвинутое обучение", callback_data='play_000_002_main_1')],   
            [InlineKeyboardButton("❌ Закрыть", callback_data="delete_this_message")]
        ])
    )





async def mainhelp_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int | None:
    """Обработчик помощи, вызываемый через /help или кнопку."""
    if update.message:
        # Вызов через команду /help
        target = update.message
    elif update.callback_query:
        # Вызов через нажатие кнопки
        query = update.callback_query
        await query.answer()
        target = query.message
    else:
        return

    help_text = """
<b>🛠Общая информация по боту 🛠</b>

Бот предназначен для создания и воспроизведения визуальных новелл, интерактивных историй. Так же этот функционал можно использовать для создания инструкций, обучающих материалов в формате интерактивных телеграм сообщений с кнопками и тд.

На данный момент бот поддерживает:
🗒Текст, включая всю разметку такую как жирный, курсивный текст, спойлеры и тд
🖼Изображения и медиагруппы
📹Видео и GIF-анимации
🎧Аудио-файлы

Текст и фрагменты истории поддерживают отправку по таймеру. Для подробностей и примеров можете пройти обучение.

<b>❔Как поделиться?❔</b>
Созданной в боте готовой историей можно легко поделиться скопировав её id из окна редактирования. Просто в любом диалоге или чате напишите
<pre>@FoxNovel_bot</pre>
После чего высветится список ваших историй. Нажмите на нужную и в этом чате сосдастся интерактивная кнопка ведущая прямиком на запуск вашей истории.
Либо
<pre>@FoxNovel_bot id_истории</pre>
Тогда высветится конкретная история

Кроме того  вы можете отправить другому человеку id вашей истории и тому будет достаточно просто отправить этот id боту - история тут же будет запущена. Так же если добавить бота в групповой чат то с помощью id можно запускать те или иные истории и проходить их вместе с другим человеком или людьми

<b>💬Нейро-функции💬</b>

В бота интегрирована нейросеть для помощи и для генерации историй. 
В групповом чате куда добавлен бот с правами админа, либо в личной переписке с ним вы можете использовать команду /nstory для генерации полностью нейросетевой истории. Пример:
<pre>/nstory сгенерируй историю которую можно было бы использовать в качестве квеста для игры ведьмак 3</pre>
Сгенерированные истории чисто текстовые, генерация картинок не поддерживается. Сгенерированные истории автоматически попадают в список ваших историй в раздел "екйроистории" там, где вы можете их редактировать, добавлять изображения, менять текст и тд

Так же вы можете создать основу для истории самостоятельно, но заполнять не все фрагменты, некоторые оставить пустыми. Затем в окне редактирования истории включить "Нейро-режим" и тогда при воспроизведении данной истории, при попытке перейти на несуществующий фрагмент, бот будет генерировать его автоматически отталкиваясь от остальных фрагментов и общей логики

Кроме того в окне редактирования истории есть "Нейропомшник", с ним вы можете обсудить конкретный фрагмент вашей истории, задать ему вопросы, попросить придумать варианты или что-то ещё/

<b>Важно! Нейропомошник пока что не поддерживает контекст. Каждое сообщение отправленное ему  воспринимает как новое. Не "видит" прошлую вашу переписку. Поэтому нужно в каждом вопросе попытаться максимально полно изложить что именно вы хотите</b>

<i>Если ответить на сообщение бота и отправить команду /nd  то данное сообщение бота будет удалено. Полезно для групповых чатов</i>

Для более продвинутой помощи пройдите обучение:

"""

    await target.reply_text(
        help_text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📔Простое обучение", callback_data='play_000_001_main_1')],
            [InlineKeyboardButton("📚Продвинутое обучение", callback_data='play_000_002_main_1')],   
            [InlineKeyboardButton("❌ Закрыть", callback_data="delete_this_message")]
        ])
    )



async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None | int:
    """Обрабатывает нажатия кнопок."""
    query = update.callback_query


    data = query.data
    logger.info(f"data2 {data}.")
    if data == 'create_story_start':
        await query.edit_message_text(text="Тогда начнём!🦊\n\nВведите название вашей новой истории:")
        return ASK_TITLE

    elif data == 'view_stories':
        await view_stories_list(update, context)
        return None 
    elif data == 'view_coop_stories':
        await view_stories_list(update, context)
        return None 
    elif data == 'view_neural_stories':
        await view_stories_list(update, context)
        return None 
    elif data.startswith('view_stories_'):
        await view_stories_list(update, context)
        return None 

    elif data.startswith('dl_story_'):
        await download_story_handler(update, context)
        return None  
    elif data.startswith('mk_pub_'):
        await toggle_story_public_status(update, context)
        return None  
    elif data.startswith('mk_priv_'):
        await toggle_story_public_status(update, context)
        return None  
    elif data.startswith('coop_edit_menu_'):
        await show_coop_edit_menu(update, context)
        return None 
    elif data.startswith('coop_remove_'):
        await handle_coop_remove(update, context)
        return None                           
    elif data.startswith(ENABLE_NEURO_MODE_PREFIX) or data.startswith(DISABLE_NEURO_MODE_PREFIX):
        await toggle_neuro_mode(update, context)
        return None
    elif data in ['main_menu_from_view', 'main_menu_finish', 'main_menu_start']:
        await start(update, context)
        return None
    elif data.startswith('coop_add_'):
        await handle_coop_add(update, context)
        return None 
    elif data.startswith('cancel_coop_add'):
        await cancel_coop_add(update, context)
        return None 


    elif data == 'restart_callback':
        return await restart(update, context)

    elif data.startswith('play_'):
        await show_story_fragment(update, context)
        return None
    elif data == 'edithelp':
        await edithelp_callback(update, context)
        return None

    elif data.startswith('delete_story_'):
        await confirm_delete_story(update, context)
        return None

    elif data.startswith('confirm_delete'):
        await delete_story_confirmed(update, context)
        return None


    elif data.startswith("send_story_map_doc_"):
        # Разбиваем всё
        parts = data.split("_")
        
        # Проверим, что структура корректна
        if len(parts) >= 8 and parts[0] == "send" and parts[1] == "story" and parts[2] == "map" and parts[3] == "doc":
            user_id_str = parts[4]
            story_id = parts[5]
            fragment_id = "_".join(parts[7:])  # поддержка фрагментов с точками и подчеркиваниями
            logger.info(f"data {data}.")  
            logger.info(f"user_id_str {user_id_str}.")   
            logger.info(f"story_id {story_id}.")  
            logger.info(f"fragment_id {fragment_id}.")                               
            story_data = load_user_story(user_id_str, story_id)
            logger.info(f"story_data {story_data}.")
        else:
            logger.warning("Неверный формат callback data.")       
        if not story_data:
            await query.edit_message_text("История не найдена.")
            return

        all_fragments = story_data.get("fragments", {})
        highlight_set = set(find_descendant_fragments(all_fragments, fragment_id))
        image_path = generate_story_map(story_id, story_data, highlight_set)

        if image_path:
            try:
                with open(image_path, 'rb') as doc_file:
                    await context.bot.send_document(
                        chat_id=query.message.chat_id,
                        document=doc_file,
                        caption=f"Схема с выделением удаляемых фрагментов <code>{fragment_id}</code>.",
                        parse_mode=ParseMode.HTML
                    )
            finally:
                os.remove(image_path)
        return


    elif data.startswith("preview_fragment_"):
        await query.answer()
        fragment_id = data[len("preview_fragment_"):]

        # Логируем содержимое context.user_data
        logger.info("context.user_data: %s", context.user_data)

        story_data = context.user_data.get("current_story", {})
        fragment_data = story_data.get("fragments", {}).get(fragment_id)

        if not fragment_data:
            await query.message.reply_text("Фрагмент не найден.", parse_mode=ParseMode.HTML)
            return

        text = fragment_data.get("text", "")
        media = fragment_data.get("media", [])
        first_match = re.search(r"(\[\[|\(\()[+-]?\d+(\]\]|\)\))", text)
        base_text = text[:first_match.start()] if first_match else text
        steps = parse_timed_edits(text)

        # Получаем user_id и story_id
        user_id_str = context.user_data.get("user_id_str")
        story_id = context.user_data.get("story_id")

        if not user_id_str or not story_id:
            await query.message.reply_text("Ошибка: user_id или story_id не найдены.", parse_mode=ParseMode.HTML)
            return

        close_button = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"▶️ Воспроизвести историю отсюда", callback_data=f"nstartstory_{user_id_str}_{story_id}_{fragment_id}")],
            [InlineKeyboardButton("❌ Закрыть это окно", callback_data="delete_this_message")],
        ])




        if not media and not text:
            await query.message.reply_text("Фрагмент пуст.", reply_markup=close_button, parse_mode=ParseMode.HTML)
            return

        elif not media:
            msg = await query.message.reply_text(base_text, reply_markup=close_button, parse_mode=ParseMode.HTML)
            if steps:
                create_task(run_timed_edits(
                    bot=context.bot,
                    chat_id=msg.chat_id,
                    message_id=msg.message_id,
                    original_text=base_text,
                    steps=steps,
                    is_caption=False,
                    user_id_str=user_id_str,
                    story_id=story_id
                ))
            return

        # Медиа-группа
        if len(media) > 1:
            media_group = []
            for i, m in enumerate(media):
                m_type = m.get("type")
                file_id = m.get("file_id")
                spoiler = m.get("spoiler") is True
                caption = base_text if i == 0 else None

                if m_type == "photo":
                    media_group.append(InputMediaPhoto(
                        media=file_id,
                        caption=caption,
                        parse_mode=ParseMode.HTML,
                        has_spoiler=spoiler
                    ))
                elif m_type == "video":
                    media_group.append(InputMediaVideo(
                        media=file_id,
                        caption=caption,
                        parse_mode=ParseMode.HTML,
                        has_spoiler=spoiler
                    ))
                elif m_type == "animation":
                    media_group.append(InputMediaAnimation(
                        media=file_id,
                        caption=caption,
                        parse_mode=ParseMode.HTML,
                        has_spoiler=spoiler
                    ))

            media_messages = await context.bot.send_media_group(
                chat_id=query.message.chat_id,
                media=media_group
            )
            context.user_data["preview_message_ids"] = [msg.message_id for msg in media_messages]
            await query.message.reply_text("Закрыть", reply_markup=close_button, parse_mode=ParseMode.HTML)

            if steps:
                # Только caption первого сообщения редактируется
                create_task(run_timed_edits(
                    bot=context.bot,
                    chat_id=query.message.chat_id,
                    message_id=media_messages[0].message_id,
                    original_text=base_text,
                    steps=steps,
                    is_caption=True,
                    user_id_str=user_id_str,
                    story_id=story_id
                ))

        else:
            m = media[0]
            m_type = m.get("type")
            file_id = m.get("file_id")
            spoiler = m.get("spoiler") is True

            if m_type == "photo":
                msg = await query.message.reply_photo(
                    file_id, caption=base_text or None, reply_markup=close_button,
                    parse_mode=ParseMode.HTML, has_spoiler=spoiler
                )
            elif m_type == "video":
                msg = await query.message.reply_video(
                    file_id, caption=base_text or None, reply_markup=close_button,
                    parse_mode=ParseMode.HTML, has_spoiler=spoiler
                )
            elif m_type == "animation":
                msg = await query.message.reply_animation(
                    file_id, caption=base_text or None, reply_markup=close_button,
                    parse_mode=ParseMode.HTML, has_spoiler=spoiler
                )
            elif m_type == "audio":
                msg = await query.message.reply_audio(
                    file_id, caption=base_text or None, reply_markup=close_button,
                    parse_mode=ParseMode.HTML
                )
            else:
                await query.message.reply_text("Неподдерживаемый тип медиа.", parse_mode=ParseMode.HTML)
                return

            if steps:
                create_task(run_timed_edits(
                    bot=context.bot,
                    chat_id=msg.chat_id,
                    message_id=msg.message_id,
                    original_text=base_text,
                    steps=steps,
                    is_caption=True,
                    user_id_str=user_id_str,
                    story_id=story_id
                ))


    elif data.startswith('mapreq_'):

        pattern = r"^mapreq_([a-zA-Z0-9]{10})_(.+)$"
        match = re.match(pattern, query.data)

        if not match:
            await safe_edit_or_resend(query, context, "Ошибка: Неверный формат данных для запроса карты.")
            return

        story_id, fragment_id = match.groups()
        user_id_str = str(update.effective_user.id)

        # Загружаем только одну историю пользователя
        story_data = load_user_story(user_id_str, story_id)

        # Если пользователь не владелец, пытаемся найти владельца

        if not story_data:
            await query.edit_message_text("История не найдена.")
            return

        all_fragments = story_data.get("fragments", {})   
        if fragment_id not in all_fragments:
            await safe_edit_or_resend(query, context, f"Ошибка: Фрагмент <code>{fragment_id}</code> не найден.", parse_mode=ParseMode.HTML)
            return

        # Гашение кнопки
        await query.answer()

        # Уведомление о создании карты
        loading_message = await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="🛠️ Карта создаётся. Ожидайте...",
        )

        potential_full_subtree = find_descendant_fragments(all_fragments, fragment_id) or set()



        fragments_preview_for_deletion, externally_referenced_in_subtree, descendants_to_list, bypass_reachable = get_fragments_for_deletion_preview(
            all_fragments=all_fragments,
            target_fragment_id=fragment_id,
            potential_full_subtree=potential_full_subtree,
            protected_fragment_id=PROTECTED_FRAGMENT_ID,
        )
        highlight_ids = fragments_preview_for_deletion    

        image_path = generate_story_map(story_id, story_data, highlight_ids)
        if image_path and os.path.exists(image_path):
            try:
                with open(image_path, 'rb') as doc_file:
                    await context.bot.edit_message_media(
                        chat_id=loading_message.chat_id,
                        message_id=loading_message.message_id,
                        media=InputMediaDocument(
                            media=doc_file,
                            caption=f"🗺️ Карта удаляемой ветки из <code>{fragment_id}</code>",
                            parse_mode=ParseMode.HTML
                        ),
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("❌ Закрыть", callback_data="delete_this_message")]
                        ])
                    )
            finally:
                os.remove(image_path)
        else:
            await context.bot.edit_message_text(
                chat_id=loading_message.chat_id,
                message_id=loading_message.message_id,
                text="Ошибка: Не удалось сгенерировать карту."
            )


    

    elif data.startswith('show_map_'):
        story_id = data[len('show_map_'):]
        user_id_str = str(update.effective_user.id)


        # Загружаем только нужную историю
        story_data = load_user_story(user_id_str, story_id)

        # Если не нашли — ищем среди всех пользователей

        if not story_data:
            await query.edit_message_text("История не найдена.")
            return


        await query.answer()  # Закрыть анимацию загрузки

        # Шаг 1: отправляем предварительное сообщение
        loading_message = await query.message.reply_text("Создаю карту, подождите...")

        # Шаг 2: генерируем карту
        image_path = generate_story_map(story_id, story_data)

        # Шаг 3: удаляем сообщение и отправляем карту
        try:
            if image_path:
                await loading_message.delete()
                with open(image_path, 'rb') as f:
                    await query.message.reply_document(
                        document=f,
                        caption=f"Карта истории '{story_data.get('title', story_id)}'",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("❌ Закрыть", callback_data="delete_this_message")]
                        ])
                    )
            else:
                await loading_message.edit_text("Ошибка при создании карты.")
        except BadRequest as e:
            logging.error(f"Ошибка при отправке карты: {e}")
            await loading_message.edit_text("Не удалось отправить карту.")


    # --- Исходный обработчик edit_story_, теперь ведет на первую страницу фрагментов ---
    elif data.startswith('edit_story_'):
        try:
            _, _, user_id_str, story_id = data.split('_', 3)
            logger.info(f"Initial edit_story_ callback. User: {user_id_str}, Story: {story_id}")

            story_data = load_user_story(user_id_str, story_id)

            user_id = int(user_id_str)
            owner_id_str = get_owner_id_or_raise(user_id, story_id, story_data)

            # Если не нашли — ищем среди всех пользователей
            if not story_data:
                await query.edit_message_text("История не найдена.")
                return


            # Здесь мы всегда начинаем с первой страницы
            current_page = 1
            fragment_ids = sorted(story_data.get("fragments", {}).keys())
            total_fragments = len(fragment_ids)

            if total_fragments == 0:
                # Если фрагментов нет, отправляем сообщение без схемы и клавиатуры фрагментов
                 await query.edit_message_text(
                    f"История '{story_data.get('title', story_id)}' пока не содержит фрагментов. "
                    f"Вы можете добавить первый фрагмент вручную (если такая функция есть) или создать новый сюжет."
                 )
                 # Можно добавить кнопку "Назад" или "Создать первый фрагмент"
                 # Пример кнопки назад:
                 # back_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="main_menu_from_view")]])
                 # await query.edit_message_reply_markup(reply_markup=back_keyboard)
                 return None # Или другое состояние, если есть меню редактирования истории

            # --- Логика генерации схемы и отправки сообщения (оставляем как есть) ---
            # Схема генерируется для всей истории, не для страницы
            # --- Решаем: генерировать карту или нет ---

            context.user_data['current_fragment_page'] = current_page  
            raw_fragment_keys = list(story_data.get("fragments", {}).keys())
            sorted_full_fragment_ids = sorted(raw_fragment_keys, key=get_fragment_sort_key)

            fragment_ids_for_legend = sorted_full_fragment_ids[(current_page-1)*FRAGMENT_BUTTONS_PER_PAGE: current_page*FRAGMENT_BUTTONS_PER_PAGE]
            legend_text = build_legend_text(story_data, fragment_ids_for_legend)
            legend_too_long = len(legend_text) > 800
            reply_markup = build_fragment_keyboard(owner_id_str, story_id, fragment_ids, current_page, story_data, legend_too_long)            
            logger.info(f"legend_text {legend_text}.")             
            if total_fragments <= 15 and len(legend_text) <= 800:
                edited = True
                sent_wait_message = None
                escaped_title = html.escape(story_data.get('title', story_id))
                try:
                    await query.edit_message_text("Создаю схему истории, подождите...")
                except telegram.error.BadRequest as e:
                    if "There is no text in the message to edit" in str(e):
                        await query.message.reply_text("Создаю схему истории, подождите...")
                    else:
                        raise

                image_path = generate_story_map(story_id, story_data)

                if image_path:
                    try:
                        with open(image_path, 'rb') as photo_file:
                            try:
                                sent_message = await query.message.reply_photo(
                                    photo=photo_file,
                                    caption=(
                                        f"Схема истории \"{escaped_title}\".\n"
                                        f"id истории: <code>{story_id}</code>.\n"  
                                        f"<i>(Вы можете скопировать id истории и использовать его для создания кнопки мгновенного запуска данной истории, подробнее по кнопке помощи ниже или в обучении)</i>\n\n"
                                        f"Выберите фрагмент для редактирования:\n\n"
                                        f"{legend_text}"
                                    ),
                                    reply_markup=reply_markup,
                                    parse_mode=ParseMode.HTML
                                )

                            except BadRequest:
                                photo_file.seek(0)
                                sent_message = await query.message.reply_document(
                                    document=photo_file,
                                    caption=(
                                        f"Схема истории \"{escaped_title}\".\n"
                                        f"id истории: <code>{story_id}</code>.\n"  
                                        f"<i>(Вы можете скопировать id истории и использовать его для создания кнопки мгновенного запуска данной истории, подробнее по кнопке помощи ниже или в обучении)</i>\n\n"
                                        f"Выберите фрагмент для редактирования:\n\n"
                                        f"{legend_text}"
                                    ),
                                    reply_markup=reply_markup,
                                    parse_mode=ParseMode.HTML
                                )

                            # Удаляем предыдущее сообщение ("Создаю схему...")
                            if edited:
                                await query.delete_message()
                            elif sent_wait_message:
                                await sent_wait_message.delete()

                    finally:
                        if os.path.exists(image_path):
                            os.remove(image_path)
                            logger.info(f"Временный файл карты {image_path} удален.")
                else:
                    if edited:
                        await query.edit_message_text("Ошибка при создании схемы.", reply_markup=reply_markup)
                    elif sent_wait_message:
                        await sent_wait_message.edit_text("Ошибка при создании схемы.", reply_markup=reply_markup)

            else:
                escaped_title = html.escape(story_data.get('title', story_id))

                await query.edit_message_text(
                    f"Редактирование \"{escaped_title}\".\n"
                    f"id истории: <code>{story_id}</code>.\n"  
                    f"<i>(Вы можете скопировать id истории и использовать его для создания кнопки мгновенного запуска данной истории, подробнее по кнопке помощи ниже или в обучении)</i>\n\n"                                                                               
                    f"Выберите фрагмент для редактирования или воспользуйтесь кнопками:\n\n"
                    f"{legend_text}",
                    reply_markup=reply_markup,
                    parse_mode=ParseMode.HTML
                )

            # Сохраняем данные в user_data, включая текущую страницу
            context.user_data['story_id'] = story_id
            context.user_data['user_id_str'] = owner_id_str
            context.user_data['current_story'] = story_data
            context.user_data['current_fragment_page'] = current_page

            # Переходим в состояние ожидания выбора фрагмента или пагинации
            return EDIT_STORY_MAP




        except Exception as e:
            logger.exception("Ошибка при обработке редактирования истории:")
            await query.edit_message_text("Произошла ошибка при редактировании истории.")
            return None


    elif data.startswith('neurohelper_'):
        try:
            _, user_id_str, story_id, page = data.split('_')
            current_page = int(page)

            # Пробуем сначала загрузить историю по user_id_str
            story_data = load_user_story(user_id_str, story_id)
            

            if not story_data:
                await query.edit_message_text("История не найдена.")
                return

            raw_fragment_keys = list(story_data.get("fragments", {}).keys())
            sorted_fragment_ids = sorted(raw_fragment_keys, key=get_fragment_sort_key)
            fragment_ids_for_legend = sorted_fragment_ids[(current_page - 1) * FRAGMENT_BUTTONS_PER_PAGE : current_page * FRAGMENT_BUTTONS_PER_PAGE]
            legend_text = build_legend_text(story_data, fragment_ids_for_legend)

            user_id = int(user_id_str)
            owner_id = get_owner_id_or_raise(user_id, story_id, story_data)
            reply_markup = build_neuro_fragment_keyboard(owner_id, story_id, sorted_fragment_ids, current_page)

            await query.message.reply_text(
                f"<b>Выберите фрагмент, с которым вам нужна помощь</b>:\n\n{legend_text}",
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML
            )
            await query.answer()

        except Exception as e:
            logger.exception("Ошибка в нейро-помощнике")
            await query.answer("Ошибка при открытии нейро-помощника", show_alert=True)


    elif data.startswith('npf_'):
        try:
            _, user_id_str, story_id, page = data.split('_')
            current_page = int(page)

            story_data = load_user_story(user_id_str, story_id)
            if not story_data:
                await query.edit_message_text("История не найдена.")
                return

            raw_fragment_keys = list(story_data.get("fragments", {}).keys())
            sorted_fragment_ids = sorted(raw_fragment_keys, key=get_fragment_sort_key)
            fragment_ids_for_legend = sorted_fragment_ids[(current_page - 1) * FRAGMENT_BUTTONS_PER_PAGE : current_page * FRAGMENT_BUTTONS_PER_PAGE]
            legend_text = build_legend_text(story_data, fragment_ids_for_legend)

            user_id = int(user_id_str)
            owner_id = get_owner_id_or_raise(user_id, story_id, story_data)
            reply_markup = build_neuro_fragment_keyboard(owner_id, story_id, sorted_fragment_ids, current_page)

            await query.edit_message_text(
                f"<b>Выберите фрагмент, с которым вам нужна помощь</b>:\n\n{legend_text}",
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML
            )
            await query.answer()
        except Exception as e:
            logger.exception("Ошибка при пагинации нейро-помощника")
            await query.answer("Ошибка при переключении страницы", show_alert=True)


    elif data.startswith('neuro_'):
        try:
            _, story_id, fragment_id = data.split('_', 2)
            context.user_data['neuro_story_id'] = story_id
            context.user_data['neuro_fragment_id'] = fragment_id

            user_id_str = str(update.effective_user.id)
            story_data = load_user_story(user_id_str, story_id)
            if not story_data:
                await query.edit_message_text("История не найдена.")
                return None

            # Дальнейшая логика работы с context.user_data и story_data...

            # Например, можно сразу отдать сообщение о фрагменте
            fragment = story_data.get("fragments", {}).get(fragment_id)
            if not fragment:
                await query.edit_message_text("Фрагмент не найден.")
                return None

            # Сохраняем полную историю
            context.user_data['neuro_full_story'] = story_data

            message_text = (
                "Теперь введите ваш вопрос:\n\n"
                "<blockquote expandable>"
                "Внимание!!! Нейросети недоступен контекст вашей с ней беседы. "
                "Всё что она видит - это вашу историю целиком, фрагмент касательно которого вы обращаетесь и ваш текущий запрос. "
                "Это сделано потому что истории и так могут быть весьма крупными, если добавлять ещё и контекст, "
                "то нейросеть с высокой вероятностью начнёт глупить. "
                "Кроме того она не видит изображения и прочие медиа.\n\n"
                "Поэтому в каждом запросе задавайте ваш вопрос максимально полно и всеобъемлюще."
                "</blockquote>"
            )

            await query.edit_message_text(message_text, parse_mode="HTML")
            return NEURAL_INPUT

        except Exception as e:
            logger.exception("Ошибка при работе с фрагментом нейро-помощника")
            await query.answer("Произошла ошибка", show_alert=True)
            return None



    elif data.startswith('p_f_'):
        try:
            parts = data.split('_')
            if len(parts) != 5:
                logger.warning(f"Неверный формат callback_data пагинации: {data}")
                await query.answer("Ошибка пагинации.", show_alert=True)
                return

            _, _, user_id_str, story_id, page_num_str = parts

            try:
                target_page = int(page_num_str)
            except ValueError:
                logger.warning(f"Неверный номер страницы в callback_data: {data}")
                await query.answer("Ошибка: неверный номер страницы.", show_alert=True)
                return

            story_data = load_user_story(user_id_str, story_id)

            user_id = int(user_id_str)
            owner_id = get_owner_id_or_raise(user_id, story_id, story_data)


            if not story_data:
                logger.warning(f"История не найдена для пагинации: {story_id} user: {owner_id}")
                await query.answer("История не найдена.", show_alert=True)
                return

            raw_fragment_keys = list(story_data.get("fragments", {}).keys())
            
            # --- КРИТИЧЕСКОЕ ИЗМЕНЕНИЕ: Используем вашу кастомную сортировку ---
            sorted_full_fragment_ids = sorted(raw_fragment_keys, key=get_fragment_sort_key)

            total_fragments = len(sorted_full_fragment_ids)
            
            # Убедитесь, что FRAGMENT_BUTTONS_PER_PAGE > 0, чтобы избежать деления на ноль
            if FRAGMENT_BUTTONS_PER_PAGE <= 0:
                logger.error("FRAGMENT_BUTTONS_PER_PAGE настроено некорректно (ноль или меньше).")
                await query.answer("Ошибка конфигурации пагинации.", show_alert=True)
                return None # Или установите значение по умолчанию

            total_pages = math.ceil(total_fragments / FRAGMENT_BUTTONS_PER_PAGE) if total_fragments > 0 else 0
            
            current_page_for_display = target_page

            # Проверка корректности запрошенной страницы
            if total_fragments > 0 and not (1 <= current_page_for_display <= total_pages):
                logger.warning(f"Запрошена недопустимая страница {current_page_for_display} (всего {total_pages}) для истории {story_id}. Действий не предпринято.")
                await query.answer(f"Страница {current_page_for_display} не существует.", show_alert=True)
                return None # Не меняем сообщение, если страница недействительна
            elif total_fragments == 0 and current_page_for_display != 1: # Если фрагментов нет, только страница 1 (пустая) имеет смысл
                 if total_pages == 0 : # Если фрагментов нет, total_pages будет 0. current_page_for_display лучше сделать 1.
                     current_page_for_display = 1 # или 0, если ваша логика это обрабатывает специфично. Для 1-based пагинации, 1.
                 else: # Эта ветка маловероятна, если total_fragments == 0
                     logger.warning(f"Запрошена страница {current_page_for_display}, но фрагментов нет.")
                     await query.answer("Фрагментов нет.", show_alert=True)
                     return None


            context.user_data['current_fragment_page'] = current_page_for_display

            # --- Генерация legend_text для текущей страницы ---
            # Срез берется из ПОЛНОСТЬЮ отсортированного списка `sorted_full_fragment_ids`
            start_index = (current_page_for_display - 1) * FRAGMENT_BUTTONS_PER_PAGE
            end_index = start_index + FRAGMENT_BUTTONS_PER_PAGE
            fragment_ids_for_legend_on_page = sorted_full_fragment_ids[start_index:end_index]
            
            # `build_legend_text` получит уже отсортированный (в рамках страницы) список.
            # Если `build_legend_text` внутри себя тоже сортирует (как в предыдущем примере),
            # это будет просто повторная сортировка небольшого, уже упорядоченного списка, что не страшно.
            legend_text = build_legend_text(story_data, fragment_ids_for_legend_on_page)
            logger.info(f"PAGINATION: legend_text для страницы {current_page_for_display} (ID фрагментов: {fragment_ids_for_legend_on_page}): '{legend_text}'")

            # --- Генерация клавиатуры для текущей страницы ---
            # `build_fragment_keyboard` получает ПОЛНЫЙ отсортированный список и текущую страницу
            reply_markup = build_fragment_keyboard(owner_id, story_id, sorted_full_fragment_ids, current_page_for_display, story_data)

            escaped_title = html.escape(story_data.get('title', story_id))
            message_text = (
                f"Схема истории \"{escaped_title}\".\n"
                f"id истории: <code>{story_id}</code>.\n"
                f"<i>(Вы можете скопировать id истории и отправить его другим людям. Им будет достаточно просто отправить этот id боту и ваша история тут же запустится)</i>\n\n"
                f"Выберите фрагмент для редактирования (Страница {current_page_for_display}/{total_pages if total_pages > 0 else 1}):\n\n" # Добавлена информация о странице в текст
                f"{legend_text}"
            )

            await query.edit_message_text(
                text=message_text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML # Убедитесь, что ParseMode импортирован
            )
            return None # Остаемся в том же состоянии (если у вас есть ConversationHandler)

        except Exception as e:
            logger.exception("Ошибка при обработке пагинации фрагментов:")
            try:
                # Пытаемся отредактировать сообщение с текстом ошибки, если это возможно
                await query.edit_message_text(text="Произошла ошибка при смене страницы.", reply_markup=None)
            except Exception as e_inner:
                # Если даже это не удалось (например, сообщение слишком старое), просто логируем
                logger.error(f"Не удалось отправить сообщение об ошибке пользователю: {e_inner}")
            return None

    # --- Обработчик для кнопки информации о странице (опционально) ---
    # Эта кнопка обычно не должна делать ничего, кроме ответа на query.answer()
    # Но добавим обработку, чтобы явно игнорировать
    elif data.startswith('page_info_'):
         # Просто отвечаем на callback, чтобы убрать индикатор загрузки
         # await query.answer() # Уже сделано в начале функции
         logger.info(f"Clicked page info button: {data}")
         return None # Остаемся в том же состоянии


    elif data.startswith('show_branches_'):
        logging.info(f"Получен callback с data: {data}")
        try:
            parts = data.split('_')
            logging.info(f"Разбито на части: {parts}")
            # Пример: ['show', 'branches', '6217936347', '94f6cd0c68', '1']

            user_id_str = parts[2]
            story_id = parts[3]
            page_str = parts[4]
            current_page = int(page_str)

            logging.info(f"user_id_str: {user_id_str}, story_id: {story_id}, current_page: {current_page}")

            story_data = load_user_story(user_id_str, story_id)
            if not story_data:
                await query.message.reply_text("История не найдена.")
                return None

            fragment_ids = story_data.get("fragments", {}).keys()
            branch_names = set()
            for fid in fragment_ids:
                if "_" in fid:
                    branch_name = fid.rsplit("_", 1)[0]
                else:
                    branch_name = fid
                branch_names.add(branch_name)

            branch_list = sorted(branch_names)
            if "main" in branch_list:
                branch_list.remove("main")
                branch_list.insert(0, "main")

            branches_per_page = 20
            total_pages = math.ceil(len(branch_list) / branches_per_page)
            page_branches = branch_list[(current_page - 1) * branches_per_page: current_page * branches_per_page]

            keyboard = []
            row = []
            for i, branch in enumerate(page_branches):
                row.append(InlineKeyboardButton(branch, callback_data=f"branch_select_{user_id_str}_{story_id}_{branch}"))
                if len(row) == 2:
                    keyboard.append(row)
                    row = []
            if row:
                keyboard.append(row)

            # Пагинация
            pagination_row = []
            if current_page > 1:
                pagination_row.append(InlineKeyboardButton("«", callback_data=f"show_branches_{user_id_str}_{story_id}_{current_page - 1}"))
            pagination_row.append(InlineKeyboardButton(f"{current_page}/{total_pages}", callback_data="ignore"))
            if current_page < total_pages:
                pagination_row.append(InlineKeyboardButton("»", callback_data=f"show_branches_{user_id_str}_{story_id}_{current_page + 1}"))
            keyboard.append(pagination_row)

            # Кнопка назад
            keyboard.append([
                InlineKeyboardButton("◀️ Назад", callback_data=f"edit_story_{user_id_str}_{story_id}")
            ])

            # Удаляем старое сообщение и отправляем новое
            await query.message.delete()
            await query.message.chat.send_message(
                f"🌿 Ветки истории «{story_data.get('title', story_id)}»:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return EDIT_STORY_MAP

        except Exception as e:
            logger.exception("Ошибка при отображении веток:")
            await query.message.reply_text("Ошибка при получении списка веток.")
            return None




    elif data.startswith('goto_'):
        target_fragment_id = data.split('_', 1)[1]
        story_id = context.user_data.get("story_id")
        story_data = context.user_data['current_story']
        fragment_data = story_data.get("fragments", {}).get(target_fragment_id)

        context.user_data['current_fragment_id'] = target_fragment_id  # Установим на всякий случай

        if fragment_data is None:
            # Если фрагмент ещё не создан — просим ввести контент
            await query.edit_message_text(
                f"Переход к созданию фрагмента '{target_fragment_id}'.\n"
                "Отправьте контент (текст, фото, видео и т.д.)."
            )
            context.user_data['is_editing_fragment'] = False
            return ADD_CONTENT

        # Если фрагмент уже существует — редактируем его
        context.user_data[EDIT_FRAGMENT_DATA] = {
            'story_id': story_id,
            'fragment_id': target_fragment_id
        }

        current_text = fragment_data.get("text", "")
        text_to_send = clean_html_for_telegram(current_text)        
        current_media = fragment_data.get("media", [])
        
        # === Добавляем проверку на пустоту текста и медиа ===
        if not current_text.strip() and not current_media:
            await query.edit_message_text(
                f"Теперь отправьте контент \(текст или фото, gif, музыку, видео\) для нового фрагмента ветки `{target_fragment_id}`",
                parse_mode="MarkdownV2",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🌃В Главное Меню🌃", callback_data='restart_callback')]
                ])
            )
            context.user_data['is_editing_fragment'] = True
            return ADD_CONTENT

        # Генерация описания медиа
        media_desc = ""
        if current_media:

            media_counts = defaultdict(int)
            for item in current_media:
                media_counts[item.get("type", "unknown")] += 1
            media_desc = ", ".join([f"{count} {m_type}" for m_type, count in media_counts.items()])
            media_desc = f"\nМедиа: [{media_desc}]"

        # Клавиатура для текущего фрагмента
        user_id_str = str(update.effective_user.id)
        reply_markup = build_fragment_action_keyboard(
            fragment_id=target_fragment_id,
            story_data=story_data,
            user_id_str=user_id_str,
            story_id=story_id
        )

        await query.edit_message_text(
            f"Выбран фрагмент: <code>{target_fragment_id}</code>\n"
            f"Текущий текст: \n✦ ━━━━━━━━━━\n{text_to_send or '*Нет текста*'}\n✦ ━━━━━━━━━━{media_desc}\n\n"
            f"<b>Любой отправленный сейчас боту текст и/или медиа (фото, видео, gif, аудио) заменят текущее содержимое фрагмента.</b>\n"
            f"Либо воспользуйтесь одной из кнопок:",
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML
        )

        context.user_data['is_editing_fragment'] = True
        return ADD_CONTENT



    # --- НОВАЯ ЛОГИКА: Начало редактирования конкретного фрагмента ---
    elif data.startswith('e_f_'):
        try:
            logging.info(f"[Edit Fragment] Received callback data: {data}")
            
            prefix = 'e_f_'
            raw_payload = data[len(prefix):]  # 'af5c94774f_go_left_or_right_1'

            # Разделяем только по первому символу '_' после префикса
            story_id, fragment_id_to_edit = raw_payload.split('_', 1)

            logging.info(f"[Edit Fragment] story_id: {story_id}, fragment_id_to_edit: {fragment_id_to_edit}, original data: {data}")

            # Проверяем, что мы находимся в правильном контексте (карта была показана)
            if 'story_id' not in context.user_data or context.user_data['story_id'] != story_id:
                await query.message.reply_text("Контекст редактирования потерян. Пожалуйста, начните заново.")
                return ConversationHandler.END # Или возврат в главное меню

            story_data = context.user_data['current_story']
            fragment_data = story_data.get("fragments", {}).get(fragment_id_to_edit)

            if fragment_data is None:
                 await query.message.reply_text(f"Фрагмент {fragment_id_to_edit} не найден в текущей истории.")
                 return EDIT_STORY_MAP # Остаемся в состоянии выбора фрагмента

            # Сохраняем ID редактируемого фрагмента
            context.user_data[EDIT_FRAGMENT_DATA] = {
                'story_id': story_id,
                'fragment_id': fragment_id_to_edit
            }






            # Показываем текущее содержимое фрагмента (опционально, но полезно)
            current_text = fragment_data.get("text", "*Нет текста*")
            text_to_send = clean_html_for_telegram(current_text)          
            current_media = fragment_data.get("media", [])
            media_desc = ""
            if current_media:
                 media_counts = defaultdict(int)
                 for item in current_media: media_counts[item.get("type", "unknown")] += 1
                 media_desc = ", ".join([f"{count} {m_type}" for m_type, count in media_counts.items()])
                 media_desc = f"\nМедиа: [{media_desc}]"

            # Спрашиваем новое содержимое
            user_id_str = str(update.effective_user.id)
            story_id = context.user_data['story_id']


            # ➕ Добавим информацию об эффектах выборов
            current_choices = fragment_data.get("choices", [])
            text_lines = []
            for choice in current_choices:
                if "effects" in choice:
                    effect_lines = []
                    for effect in choice["effects"]:
                        stat = html.escape(str(effect.get("stat", "неизвестно")))
                        if effect.get("hide"):
                            stat = f"{stat}(Скрытый)"
                        raw_value = str(effect.get("value", "?"))
                        value = html.escape(raw_value)

                        # Определяем тип эффекта по raw_value
                        if re.match(r'^[+-]?\(\-?\d+\-+\-?\d+\)$', raw_value):  # например +(5-9), -(3--8)
                            verb = "меняет атрибут"
                        elif re.match(r'^[+-]\d+$', raw_value):  # например +5, -3
                            verb = "меняет атрибут"
                        elif re.match(r'^[<>=]', raw_value):  # сравнение
                            verb = "проверка атрибута"
                        elif re.match(r'^-?\d+\-+\-?\d+$', raw_value):  # диапазон: 3-5, -2--1, 3--8
                            verb = "задаёт атрибут"
                        elif re.match(r'^-?\d+$', raw_value):  # просто число: 7, -1
                            verb = "задаёт атрибут"
                        else:
                            verb = "неизвестный эффект"

                        effect_lines.append(f"{stat}: {value} ({verb})")

                    if effect_lines:
                        effects_text = ", ".join(effect_lines)
                        text_lines.append(
                            f"\n\n🔸 Выбор <b>«{html.escape(choice['text'])}»</b> ведущий на фрагмент <code>{html.escape(choice['target'])}</code> имеет эффект: {effects_text}"
                        )

            effects_info = "".join(text_lines)



            reply_markup = build_fragment_action_keyboard(
                fragment_id=fragment_id_to_edit,
                story_data=story_data,
                user_id_str=user_id_str,
                story_id=story_id
            )



            # Собираем финальный текст
            await query.message.reply_text(
                f"Редактирование фрагмента: <code>{fragment_id_to_edit}</code>\n"
                f"Текущий текст: \n✦ ━━━━━━━━━━\n{text_to_send}\n✦ ━━━━━━━━━━{media_desc}"
                f"{effects_info}\n\n"
                f"➡️ <b>Отправьте новый текст и/или медиа (фото, видео, gif, аудио) для этого фрагмента.</b>\n"
                f"Новый контент полностью заменит старый.",
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML
            )

            # Здесь нужен переход в состояние ожидания нового контента.
            # Используем существующий обработчик `add_content_handler`, но нужно убедиться,
            # что он правильно обработает ситуацию редактирования.
            # Передадим ID редактируемого фрагмента через user_data.
            context.user_data['current_fragment_id'] = fragment_id_to_edit # Устанавливаем ID для add_content_handler
            context.user_data['is_editing_fragment'] = True 
            # Возвращаем состояние, которое ожидает ввода контента
            # Возможно, твой ADD_CONTENT уже подходит? Или создай новое состояние EDITING_FRAGMENT_CONTENT
            return ADD_CONTENT # ИЛИ return EDITING_FRAGMENT_CONTENT, если хочешь разделить логику

        except Exception as e:
            logger.error(f"Ошибка при обработке edit_fragment: {e}", exc_info=True)
            await query.message.reply_text("Произошла ошибка при выборе фрагмента для редактирования.")
            return EDIT_STORY_MAP # Возвращаемся к карте


    elif data.startswith('branch_select_'):
        try:
            # Отбрасываем префикс и парсим остальное
            payload = data[len('branch_select_'):]
            user_id_str, story_id, branch_name = payload.split('_', 2)

            logger.info(f"Выбор ветки: user_id={user_id_str}, story_id={story_id}, branch_name={branch_name}")
            
            story_data = load_user_story(user_id_str, story_id)

            if not story_data:
                await query.edit_message_text("История не найдена.")
                return None

            user_id = int(user_id_str)
            owner_id = get_owner_id_or_raise(user_id, story_id, story_data)


            all_story_fragments = story_data.get("fragments", {})
            branch_fragment_ids = [
                frag_id for frag_id in all_story_fragments
                if frag_id == branch_name or frag_id.startswith(branch_name + "_")
            ]

            # Сортировка для отображения
            branch_fragment_ids = sorted(branch_fragment_ids, key=get_fragment_sort_key)

            current_page = 1  # Первая страница
            
            # Сохраняем в user_data для пагинации и других действий с этой веткой
            context.user_data['current_story_id'] = story_id
            context.user_data['current_branch_name'] = branch_name
            context.user_data['current_branch_page'] = current_page
            # context.user_data['current_user_id_str'] = user_id_str # Если нужно для других обработчиков

            # Формируем клавиатуру для фрагментов ветки
            reply_markup = build_branch_fragments_keyboard(
                owner_id, story_id, branch_name, branch_fragment_ids, current_page, story_data
            )
            
            # Формируем легенду для текущей страницы фрагментов ветки
            start_idx = (current_page - 1) * FRAGMENT_BUTTONS_PER_PAGE
            end_idx = start_idx + FRAGMENT_BUTTONS_PER_PAGE
            fragments_on_page_for_legend = branch_fragment_ids[start_idx:end_idx]
            legend_text = build_legend_text(story_data, fragments_on_page_for_legend) # Используем существующую build_legend_text

            story_title = story_data.get('title', story_id)
            message_text_parts = [
                f"🌿 Ветка: <b>{branch_name}</b> (в истории «{story_title}»)",
                f"🆔 Истории: <code>{story_id}</code>\n"
            ]

            if not branch_fragment_ids:
                message_text_parts.append("Эта ветка пока не содержит фрагментов.")
            else:
                message_text_parts.append("Фрагменты на текущей странице:")
                message_text_parts.append(legend_text if legend_text else "Нет информации о фрагментах для отображения.")

            final_message_text = "\n".join(message_text_parts)

            # Генерация карты ветки (например, если фрагментов в ветке не слишком много)
            # Порог можно настроить
            MAX_FRAGMENTS_FOR_INLINE_BRANCH_MAP = 15 
            
            # Удаляем предыдущее сообщение (от списка веток)
            # await query.delete_message() # Раскомментируйте, если нужно удалить старое

            if branch_fragment_ids and len(branch_fragment_ids) <= MAX_FRAGMENTS_FOR_INLINE_BRANCH_MAP:
                loading_map_msg = await query.message.reply_text("Создаю карту ветки, подождите...") # Используем reply_text к исходному сообщению
                # await query.edit_message_text("Создаю карту ветки, подождите...") # Если не удаляли старое
                
                image_path = generate_branch_map(story_id, story_data, branch_name)
                await loading_map_msg.delete()

                if image_path:
                    try:
                        with open(image_path, 'rb') as photo_file:
                            # Сначала удаляем исходное сообщение от CallbackQuery, чтобы избежать конфликтов ID
                            await query.delete_message()
                            sent_message = await context.bot.send_photo( # query.message.chat.send_photo
                                chat_id=query.message.chat_id,
                                photo=photo_file,
                                caption=final_message_text,
                                reply_markup=reply_markup,
                                parse_mode=ParseMode.HTML
                            )
                    except BadRequest as e: # Если фото слишком большое, пробуем как документ
                        logger.warning(f"Не удалось отправить карту ветки как фото: {e}, пробую как документ.")
                        with open(image_path, 'rb') as doc_file:
                            await query.delete_message()
                            sent_message = await context.bot.send_document( # query.message.chat.send_document
                                chat_id=query.message.chat_id,
                                document=doc_file,
                                caption=final_message_text,
                                reply_markup=reply_markup,
                                parse_mode=ParseMode.HTML
                            )
                    finally:
                        if os.path.exists(image_path):
                            os.remove(image_path)
                            logger.info(f"Временный файл карты ветки {image_path} удален.")
                else:
                    # await query.edit_message_text(final_message_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
                    # Если не смогли сгенерировать карту, но сообщение удалили, нужно отправить новое
                    await query.delete_message()
                    await context.bot.send_message(
                         chat_id=query.message.chat_id,
                         text=final_message_text,
                         reply_markup=reply_markup,
                         parse_mode=ParseMode.HTML
                    )

            else: # Слишком много фрагментов или нет фрагментов - просто текст и кнопки
                # Если карта не генерируется сразу, кнопка "Карта ветки" будет в клавиатуре
                await query.edit_message_text(
                    final_message_text,
                    reply_markup=reply_markup,
                    parse_mode=ParseMode.HTML
                )
            
            return EDIT_STORY_MAP # Новое состояние для обработки действий в контексте ветки

        except Exception as e:
            logger.exception("Ошибка при обработке выбора ветки:")
            await query.edit_message_text("Произошла ошибка при отображении ветки.")
            return None # Или возврат в предыдущее безопасное состояние

    # Добавить обработчик для кнопки "Карта этой ветки", если она не генерируется сразу
    elif data.startswith('show_bmap_'):  # SHOW_BRANCH_MAP_PREFIX
        try:
            payload = data.removeprefix('show_bmap_')
            story_id, branch_name = payload.split('_', 1)
            user_id_str = str(update.effective_user.id)

            story_data = load_user_story(user_id_str, story_id)
            if not story_data:
                await query.answer("История не найдена.", show_alert=True)
                return None

            await query.answer()
            loading_message = await query.message.reply_text("Создаю карту ветки, подождите...")

            image_path = generate_branch_map(story_id, story_data, branch_name)
            logger.info(f"Карта истории '{image_path}'")
            await loading_message.delete()

            if image_path:
                caption_text = f"Карта ветки '{branch_name}' истории '{story_data.get('title', story_id)}'."
                close_markup = InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Закрыть", callback_data="delete_this_message")]
                ])
                try:
                    with open(image_path, 'rb') as doc_file:
                        await query.message.reply_document(
                            document=doc_file,
                            caption=caption_text,
                            reply_markup=close_markup
                        )
                finally:
                    if os.path.exists(image_path):
                        os.remove(image_path)
            else:
                await query.message.reply_text("Ошибка при создании карты ветки.")

            return None

        except Exception as e:
            logger.exception("Ошибка при показе карты ветки:")
            if 'loading_message' in locals() and loading_message:
                await loading_message.delete()
            await query.message.reply_text("Произошла ошибка при создании карты ветки.")
            return None


    # Добавить обработчик для пагинации фрагментов ветки
    elif data.startswith('p_bf_'):  # P_BF_PREFIX (Page Branch Fragment)
        try:
            # p_bf_{user_id_str}_{story_id}_{branch_name}_{page}
            _, user_id_str, story_id, branch_name, page_str = data.split('_', 4)
            current_page = int(page_str)

            story_data = load_user_story(user_id_str, story_id)
            if not story_data:
                await query.edit_message_text("История не найдена.")
                return None

            all_story_fragments = story_data.get("fragments", {})
            branch_fragment_ids = sorted(
                [frag_id for frag_id in all_story_fragments if frag_id == branch_name or frag_id.startswith(branch_name + "_")],
                key=get_fragment_sort_key
            )
            user_id = int(user_id_str)
            owner_id = get_owner_id_or_raise(user_id, story_id, story_data)
            # Обновляем user_data
            context.user_data['current_branch_page'] = current_page

            reply_markup = build_branch_fragments_keyboard(
                owner_id, story_id, branch_name, branch_fragment_ids, current_page, story_data
            )
            
            start_idx = (current_page - 1) * FRAGMENT_BUTTONS_PER_PAGE
            end_idx = start_idx + FRAGMENT_BUTTONS_PER_PAGE
            fragments_on_page_for_legend = branch_fragment_ids[start_idx:end_idx]
            legend_text = build_legend_text(story_data, fragments_on_page_for_legend)

            story_title = story_data.get('title', story_id)
            message_text_parts = [
                f"🌿 Ветка: <b>{branch_name}</b> (в истории «{story_title}»)",
                f"🆔 Истории: <code>{story_id}</code>\n",
                "Фрагменты на текущей странице:",
                legend_text if legend_text else "Нет информации о фрагментах для отображения."
            ]
            final_message_text = "\n".join(message_text_parts)

            await query.edit_message_text(
                final_message_text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML
            )
            return EDIT_BRANCH_FRAGMENTS # Остаемся в состоянии просмотра/редактирования ветки

        except Exception as e:
            logger.exception("Ошибка при пагинации фрагментов ветки:")
            await query.edit_message_text("Ошибка при смене страницы.")
            return None # или EDIT_STORY_MAP

    elif data.startswith('page_info_branch_'): # page_info_branch_user_story_branch_page
        logger.info(f"Нажата кнопка информации о странице ветки: {data}")
        # await query.answer() # Уже сделано в начале button_handler
        return None # Ничего не делаем, остаемся в том же состоянии

    elif data.startswith('ignore_'):
         # Просто отвечаем на callback, чтобы убрать индикатор загрузки
         # await query.answer() # Уже сделано в начале функции
         logger.info(f"Clicked page info button: {data}")
         return None # Остаемся в том же состоянии
    elif data.startswith('nstartstory_'):
        await handle_neuralstart_story_callback(update, context)
        return None  


    else:
        # Обработка других кнопок, если есть
        await query.message.reply_text("Неизвестное действие.")
        return None



async def handle_nstory_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message_text = update.message.text

    # Убираем "/nstory" и всё, что после упоминания бота (например, /nstory@my_bot)
    command_and_args = message_text.split(" ", 1)
    if len(command_and_args) < 2:
        await update.message.reply_text(
            "❗ <b>Пожалуйста, укажите, о чём именно вы хотите сгенерировать историю после команды</b> <code>/nstory</code>\n\n"
            "<b>Например:</b>\n"
            "<pre>/nstory история про ведьмака на 15 фрагментов</pre>",
            parse_mode="HTML"
        )
        return ConversationHandler.END

    clean_title = command_and_args[1].strip()

    # Устанавливаем user_id_str и сокращённый story_id
    user = update.message.from_user
    context.user_data["user_id_str"] = str(user.id)
    context.user_data["story_id"] = uuid.uuid4().hex[:10]  # короткий id истории

    return await neural_story(update, context, clean_title)


async def handle_full_nstory_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message_text = update.message.text

    # Убираем "/nstory" и всё, что после упоминания бота (например, /nstory@my_bot)
    command_and_args = message_text.split(" ", 1)
    if len(command_and_args) < 2:
        await update.message.reply_text(
            "❗ <b>Пожалуйста, укажите, о чём именно вы хотите сгенерировать историю после команды</b> <code>/nstory</code>\n\n"
            "Вы можете как дать общие указания касательно тематики, так и пошагово по фрагментам описать, что именно вам нужно.\n\n"
            "<b>Например:</b>\n"
            "<pre>/nfullstory история про ведьмака на 15 фрагментов. Первый фрагмент ознакомительный, во втором происходит распределение изначальных характеристик персонажа: внимание 7, сила 11, ловкость 8. Далее идут два фрагмента, один из которых даёт... и т. д.</pre>\n"
            "Это весьма сложная для нейросети задача, поэтому во многих случаях она может возвращать неверные данные, которые бот не сможет обработать или обработает неверно. В таком случае попробуйте ещё раз.",
            parse_mode="HTML"
        )    
        return ConversationHandler.END

    clean_title = command_and_args[1].strip()

    # Устанавливаем user_id_str и сокращённый story_id
    user = update.message.from_user
    context.user_data["user_id_str"] = str(user.id)
    context.user_data["story_id"] = uuid.uuid4().hex[:10]  # короткий id истории

    return await neural_full_story(update, context, clean_title)


async def handle_neuralstart_story_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    logging.info(f"Received callback_data: {query.data}")

    # Отрезаем префикс (всё до первого подчёркивания + само подчёркивание)
    _, _, callback_suffix = query.data.partition("_")
    logging.info(f"Extracted suffix from callback_data: {callback_suffix}")

    # Получаем имя пользователя
    user = query.from_user
    user_id = str(user.id)
    if user.full_name:
        username_display = user.full_name
    elif user.username:
        username_display = f"@{user.username}"
    else:
        username_display = f"id:{user.id}"

    # Извлекаем story_id из callback_suffix
    parts = callback_suffix.split("_")
    if len(parts) < 3:
        logging.warning("Некорректный callback_suffix, не удалось извлечь story_id.")
        return

    story_id = parts[1]
    logging.info(f"story_id: {story_id}")    
    fragment_id = "_".join(parts[2:])



    story_data = load_user_story(user_id, story_id)

    if not story_data:
        # Пробуем загрузить через fallback-доступ
        logging.info(f"Попытка fallback-загрузки истории {story_id} для пользователя {user_id}")
        story_data = load_story_by_id_fallback(story_id)

    if not story_data:
        await query.message.reply_text("⚠️ История не найдена.")
        return





    title = story_data.get("title", "Без названия")
    neural = story_data.get("neural", False)
    author = story_data.get("author")

    # Собираем подпись
    story_info_lines = [f"📖 История: «{title}»"]
    if author:
        if neural:
            story_info_lines.append(f"✍️ Автор: {author} (нейроистория)")
        else:
            story_info_lines.append(f"✍️ Автор: {author}")

    story_info = "\n".join(story_info_lines)

    # Обновляем суффикс
    parts[0] = user_id
    new_suffix = "_".join(parts)

    # Кнопка "Играть"
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ Играть", callback_data=f"play_{new_suffix}")]
    ])

    await query.message.reply_text(
        f"🎮 Запуск истории готов для пользователя: {username_display}.\n\n{story_info}\n\nНажмите кнопку ниже, чтобы начать играть:",
        reply_markup=keyboard
    )


DEBUG_DIR = "stories_debug"
os.makedirs(DEBUG_DIR, exist_ok=True)


async def neural_story(update: Update, context: ContextTypes.DEFAULT_TYPE, clean_title: str) -> int:
    user = update.message.from_user
    user_id = user.id
    username = user.full_name  # или user.username, если нужен ник
    user_id_str = context.user_data.get("user_id_str")
    story_id = context.user_data.get("story_id")

    if not user_id_str or not story_id:
        await update.message.reply_text("Произошла ошибка: не удалось определить пользователя или ID истории.")
        return ConversationHandler.END

    waiting_message = await update.message.reply_text(
        "⌛ Генерирую историю с помощью нейросети. Пожалуйста, подождите..."
    )

    async def background_generation():
        raw_response = None
        try:
            # Убедитесь, что generate_neural_story, save_story_data и DEBUG_DIR определены
            raw_response = await generate_neural_story(clean_title)

            if not isinstance(raw_response, str):
                raw_response = json.dumps(raw_response, ensure_ascii=False)

            start = raw_response.find('{')
            end = raw_response.rfind('}') + 1
            cleaned_json_str = raw_response[start:end]
            generated_story = json.loads(cleaned_json_str)

            if not isinstance(generated_story, dict) or \
               "title" not in generated_story or \
               "fragments" not in generated_story or \
               not isinstance(generated_story["fragments"], dict):
                raise ValueError("Ошибка в структуре сгенерированной истории")

            generated_story["neural"] = True
            generated_story["neuro_fragments"] = True    

            # 👉 Добавляем автора:
            generated_story["author"] = f"{username}"
            generated_story["owner_id"] = f"{user_id}"

            save_story_data(user_id_str, story_id, generated_story)

            context.user_data['current_story'] = generated_story
            context.user_data['current_fragment_id'] = "1" # Обычно начальный фрагмент
            context.user_data['next_choice_index'] = 1

            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("📖 Перейти к запуску истории", callback_data=f"nstartstory_{user_id_str}_{story_id}_main_1")]
            ])

            await waiting_message.edit_text(
                f"✅ <b>История успешно сгенерирована!</b>\n\n<b>Название: {generated_story['title']}</b>\n\nДля запуска воспользуйтесь кнопкой ниже",
                reply_markup=keyboard,
                parse_mode='HTML'
            )
        except asyncio.CancelledError:
            logger.info(f"Генерация истории для пользователя {user_id} была отменена.")
            try:
                await waiting_message.edit_text("Генерация истории была отменена.")
            except Exception as e_edit:
                logger.warning(f"Не удалось изменить сообщение ожидания при отмене (neural_story): {e_edit}")
        except Exception as e:
            logger.error(f"Ошибка при генерации истории для пользователя {user_id}: {e}")
            try:
                await waiting_message.edit_text(
                    "⚠️ Произошла ошибка при генерации истории. Нейросеть иногда сбоит и возвращает не верные данные, попробуйте ещё раз."
                )
            except Exception as e_edit:
                logger.warning(f"Не удалось изменить сообщение ожидания при ошибке (neural_story): {e_edit}")

    # Создаем и сохраняем задачу
    task = asyncio.create_task(background_generation())
    user_tasks_set = context.user_data.setdefault('user_tasks', set())
    user_tasks_set.add(task)
    task.add_done_callback(lambda t: _remove_task_from_context(t, context.user_data))

    return ConversationHandler.END



async def neural_full_story(update: Update, context: ContextTypes.DEFAULT_TYPE, clean_title: str) -> int:
    user = update.message.from_user
    user_id = user.id
    username = user.full_name  # или user.username, если нужен ник
    user_id_str = context.user_data.get("user_id_str")
    story_id = context.user_data.get("story_id")

    if not user_id_str or not story_id:
        await update.message.reply_text("Произошла ошибка: не удалось определить пользователя или ID истории.")
        return ConversationHandler.END

    waiting_message = await update.message.reply_text(
        "⌛ Генерирую историю с помощью нейросети. Пожалуйста, подождите..."
    )

    async def background_generation():
        raw_response = None
        try:
            # Убедитесь, что generate_neural_story, save_story_data и DEBUG_DIR определены
            raw_response = await generate_neural_story_full(clean_title)

            if not isinstance(raw_response, str):
                raw_response = json.dumps(raw_response, ensure_ascii=False)

            start = raw_response.find('{')
            end = raw_response.rfind('}') + 1
            cleaned_json_str = raw_response[start:end]
            generated_story = json.loads(cleaned_json_str)

            if not isinstance(generated_story, dict) or \
               "title" not in generated_story or \
               "fragments" not in generated_story or \
               not isinstance(generated_story["fragments"], dict):
                raise ValueError("Ошибка в структуре сгенерированной истории")

            generated_story["neural"] = True
            generated_story["neuro_fragments"] = True    

            # 👉 Добавляем автора:
            generated_story["author"] = f"{username}"
            generated_story["owner_id"] = f"{user_id}"

            save_story_data(user_id_str, story_id, generated_story)

            context.user_data['current_story'] = generated_story
            context.user_data['current_fragment_id'] = "1" # Обычно начальный фрагмент
            context.user_data['next_choice_index'] = 1

            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("📖 Перейти к запуску истории", callback_data=f"nstartstory_{user_id_str}_{story_id}_main_1")]
            ])

            await waiting_message.edit_text(
                f"✅ <b>История успешно сгенерирована!</b>\n\n<b>Название: {generated_story['title']}</b>\n\nДля запуска воспользуйтесь кнопкой ниже",
                reply_markup=keyboard,
                parse_mode='HTML'
            )
        except asyncio.CancelledError:
            logger.info(f"Генерация истории для пользователя {user_id} была отменена.")
            try:
                await waiting_message.edit_text("Генерация истории была отменена.")
            except Exception as e_edit:
                logger.warning(f"Не удалось изменить сообщение ожидания при отмене (neural_story): {e_edit}")
        except Exception as e:
            logger.error(f"Ошибка при генерации истории для пользователя {user_id}: {e}")
            try:
                await waiting_message.edit_text(
                    "⚠️ Произошла ошибка при генерации истории. Попробуйте ещё раз позже. Это весьма сложная для нейросети задача, поэтому во многих случаях она может возврщать неверные данные которые бот не сможет обработать или обработает не верно, в таком случае попытайтесь ещё раз"
                )
            except Exception as e_edit:
                logger.warning(f"Не удалось изменить сообщение ожидания при ошибке (neural_story): {e_edit}")

    # Создаем и сохраняем задачу
    task = asyncio.create_task(background_generation())
    user_tasks_set = context.user_data.setdefault('user_tasks', set())
    user_tasks_set.add(task)
    task.add_done_callback(lambda t: _remove_task_from_context(t, context.user_data))

    return ConversationHandler.END



async def ask_title_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Получает название истории, вызывает нейросеть при (нейро), иначе — генерирует ID и переходит к добавлению первого фрагмента."""
    logging.info(f"Update: {Update}")      

    user = update.message.from_user
    username = user.full_name  # Либо .username для @никнейма
    user_id_str = str(user.id)
    title = update.message.text.strip()    
    # # Вариант 2: HTML (самый надежный)
    escaped_title = html.escape(title)
    current_parse_mode = ParseMode.HTML
    story_id = uuid.uuid4().hex[:10]

    context.user_data['user_id_str'] = user_id_str
    context.user_data['story_id'] = story_id

    if title.lower().endswith("(нейро)"):
        clean_title = title[:-7].strip()
        return await neural_story(update, context, clean_title)

    context.user_data['current_story'] = {
        "title": title,
        "author": username,  # <--- Сохраняем имя автора
        "owner_id": user_id_str,  # <--- Сохраняем имя автора        
        "fragments": {}
    }
    context.user_data['current_fragment_id'] = "main_1"
    context.user_data['next_choice_index'] = 1

    # Сохраняем начальную версию истории
    save_current_story_from_context(context)

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🌃В Главное Меню🌃", callback_data='restart_callback')]
    ])

    message_text = (
        f"<b>Отлично!</b>\n"
        f"Название истории: <b>{escaped_title}</b>\n"
        f"Уникальный ID истории: <code>{story_id}</code>\n"
        f"<i>Сейчас или в дальнейшем вы сможете скопировать его и отправить другим людям.</i>\n"
        f"<i>Им будет достаточно просто отправить этот ID боту, и бот тут же запустит вашу историю.</i>\n\n"
        f"<b>Теперь отправьте контент для первого фрагмента.</b>\n"
        f"<i>Это может быть текст, фото (с подписью или без), видео, GIF или аудио.</i>\n"
        f"<i>Поддерживается вся доступная в телеграм разметка, например спойлеры. А также тэги для автоматической смены слайдов и редактирования текста. Для подробностей пройдите обучение из главного меню.</i>"
    )

    await update.message.reply_text(
        message_text,
        reply_markup=keyboard,
        parse_mode=current_parse_mode
    )

    return ADD_CONTENT


async def confirm_replace_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    if query.data.startswith("confirm_replace:"):
        fragment_id = query.data.split(":")[1]
        pending = context.user_data.get("pending_fragment")
        if pending and pending["fragment_id"] == fragment_id:
            # Обновляем фрагмент
            story_data = context.user_data["current_story"]
            story_data["fragments"][fragment_id] = pending
            save_current_story_from_context(context)

            await show_fragment_actions(update, context, fragment_id)
            context.user_data.pop("pending_fragment", None)
            return ADD_CONTENT

    elif query.data == "cancel_replace":
        await query.delete_message()
        context.user_data.pop("pending_fragment", None)
        return ADD_CONTENT
    await show_fragment_actions(update, context, fragment_id)
    return ADD_CONTENT



media_group_tasks = {}  # глобальная переменная, чтобы избежать повторной обработки

async def add_content_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.message
    logger.info(f"message: {message}")

    if not context.user_data.get('story_id'):
        await message.reply_text("Похоже, произошла ошибка или вы отменили создание. Начните заново с /start.")
        return ConversationHandler.END



    user_id_str = context.user_data['user_id_str']
    fragment_id = context.user_data['current_fragment_id']
    story_data = context.user_data['current_story']
    story_data.setdefault("fragments", {})
    is_editing = context.user_data.get('is_editing_fragment', False)

    # ===== МЕДИАГРУППА =====
    if message.media_group_id:
        media_group_id = message.media_group_id

        if "media_groups" not in context.user_data:
            context.user_data["media_groups"] = defaultdict(list)

        context.user_data["media_groups"][media_group_id].append(message)

        # Если задача для этой группы уже существует — ничего не делаем
        if media_group_id in media_group_tasks:
            return ADD_CONTENT

        # Иначе создаём задачу, которая сработает через паузу
        async def process_group():
            await asyncio.sleep(2.5)  # даём время Telegram прислать остальные сообщения
            media_messages = context.user_data["media_groups"].pop(media_group_id, [])
            media_content = []
            caption_text = ""

            for m in media_messages:
                if m.photo:
                    media_content.append({
                        "type": "photo",
                        "file_id": m.photo[-1].file_id,
                        "spoiler": m.has_media_spoiler
                    })
                elif m.video:
                    media_content.append({
                        "type": "video",
                        "file_id": m.video.file_id,
                        "spoiler": m.has_media_spoiler
                    })
                elif m.animation:
                    media_content.append({
                        "type": "animation",
                        "file_id": m.animation.file_id,
                        "spoiler": m.has_media_spoiler
                    })
                elif m.audio:
                    media_content.append({"type": "audio", "file_id": m.audio.file_id})
                if m.caption:
                    caption_text = format_text_to_html(m)  # используем caption из одного сообщения

            story_data["fragments"][fragment_id] = {
                "text": caption_text or "",
                "media": media_content,
                "choices": story_data["fragments"].get(fragment_id, {}).get("choices", [])
            }

            logger.info(f"Добавлен медиаконтент для фрагмента {fragment_id} истории {context.user_data['story_id']}")
            save_current_story_from_context(context)

            if is_editing:
                await message.reply_text("Фрагмент успешно отредактирован.")
                context.user_data.pop('is_editing_fragment', None)
                await show_fragment_actions(update, context, fragment_id)
            else:
                await show_fragment_actions(update, context, fragment_id)

            # Удаляем задачу после завершения
            media_group_tasks.pop(media_group_id, None)

        # Запускаем задачу в фоне
        media_group_tasks[media_group_id] = asyncio.create_task(process_group())
        return ADD_CONTENT

    # ===== ОДИНОЧНОЕ СООБЩЕНИЕ =====
    media_content = []
    caption_text = None

    if message.text or message.caption:
        caption_text = format_text_to_html(message)

    if message.photo:
        media_content.append({
            "type": "photo",
            "file_id": message.photo[-1].file_id,
            "spoiler": message.has_media_spoiler
        })
    elif message.video:
        media_content.append({
            "type": "video",
            "file_id": message.video.file_id,
            "spoiler": message.has_media_spoiler
        })
    elif message.animation:
        media_content.append({
            "type": "animation",
            "file_id": message.animation.file_id,
            "spoiler": message.has_media_spoiler
        })
    elif message.audio:
        media_content.append({"type": "audio", "file_id": message.audio.file_id})

    if not caption_text and not media_content:
        await message.reply_text("Пожалуйста, отправьте текст или медиафайл (фото, видео, gif, аудио) для фрагмента.")
        return ADD_CONTENT

    existing_fragment = story_data["fragments"].get(fragment_id, {})
    existing_media = existing_fragment.get("media", [])

    if existing_media and not media_content and caption_text:
        media_types = set(m["type"] for m in existing_media)
        media_str = ", ".join(media_types)
        media_count = len(existing_media)

        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("Да", callback_data=f"confirm_replace:{fragment_id}"),
                InlineKeyboardButton("Нет", callback_data="cancel_replace")
            ]
        ])
        await message.reply_text(
            f"Вы уверены, что хотите заменить {media_count} медиа ({media_str}) на текст?",
            reply_markup=keyboard
        )

        context.user_data["pending_fragment"] = {
            "fragment_id": fragment_id,
            "text": caption_text or "",
            "media": [],
            "choices": existing_fragment.get("choices", [])
        }

        return ADD_CONTENT

    # Добавление нового одиночного контента
    story_data["fragments"][fragment_id] = {
        "text": caption_text or "",
        "media": media_content,
        "choices": story_data["fragments"].get(fragment_id, {}).get("choices", [])
    }

    logger.info(f"Добавлен/обновлен контент для фрагмента {fragment_id} истории {context.user_data['story_id']}")
    save_current_story_from_context(context)

    await show_fragment_actions(update, context, fragment_id)
    return ADD_CONTENT



CUSTOM_TAG_PATTERN = re.compile(r"(\(\([+-]?\d+\)\)|\[\[[+-]?\d+\]\])")

def split_html_around_custom_tags(text):
    def replacer(match):
        tag = match.group(1) # Это сам кастомный тег, например, "((+2))"
        
        # Текст до кастомного тега (из оригинальной строки 'text')
        before_custom_tag = text[:match.start()]
        # Текст после кастомного тега (из оригинальной строки 'text')
        # after_custom_tag = text[match.end():] # Не используется в этой исправленной логике напрямую

        # Ищем ближайший открытый HTML-тег перед кастомным тегом
        # Используем re.IGNORECASE для большей универсальности (например, <B> вместо <b>)
        # Добавил \d к [a-z] для тегов типа <h1>
        open_tag_match = re.search(r'<([a-z\d]+)([^>]*)>([^<]*)$', before_custom_tag, re.IGNORECASE)
        
        if not open_tag_match:
            # Кастомный тег не находится внутри HTML-тега, который мы можем обработать,
            # или HTML-структура не соответствует ожиданиям.
            # Возвращаем сам тег без изменений.
            return tag 

        tag_name = open_tag_match.group(1)
        tag_attrs = open_tag_match.group(2)  # Атрибуты тега, включая пробел перед ними, если есть
        # inner_text_before_tag = open_tag_match.group(3) # Текст между открывающим HTML-тегом и кастомным тегом

        # Формируем строку для замены: закрываем HTML-тег, вставляем кастомный тег, снова открываем HTML-тег.
        # Эта строка заменит match.group(0) (весь кастомный тег) в основном цикле.
        return f"</{tag_name}>{tag}<{tag_name}{tag_attrs}>"

    result = text # Начинаем с исходного текста
    # Итерируемся по совпадениям кастомных тегов в ОБРАТНОМ порядке,
    # чтобы изменения индексов не влияли на последующие замены.
    # Важно: finditer работает по оригинальному 'text', а замены происходят в 'result'.
    for match in reversed(list(CUSTOM_TAG_PATTERN.finditer(text))):
        replacement_string = replacer(match)
        result = result[:match.start()] + replacement_string + result[match.end():]
        
    return result

def escape(text):
    return html.escape(text)

def format_text_to_html(message):
    raw_text = message.text or message.caption
    if not raw_text:
        return ""

    entities = message.entities if message.text else message.caption_entities
    if not entities:
        return escape(raw_text)

    # Преобразуем строку в UTF-16
    utf16_text = raw_text.encode('utf-16-le')
    code_units = []
    i = 0
    while i < len(utf16_text):
        # каждый символ в utf-16 занимает 2 байта
        code_unit = utf16_text[i:i+2]
        code_units.append(code_unit)
        i += 2

    # Сопоставим UTF-16 позиции с индексами в Python строке
    utf16_index_to_py_index = {}
    py_index = 0
    utf16_pos = 0
    for ch in raw_text:
        utf16_len = len(ch.encode('utf-16-le')) // 2
        for _ in range(utf16_len):
            utf16_index_to_py_index[utf16_pos] = py_index
            utf16_pos += 1
        py_index += 1

    tag_events = defaultdict(list)
    for entity in entities:
        tag = entity.type
        if tag == "bold":
            tag = "b"
        elif tag == "italic":
            tag = "i"
        elif tag == "underline":
            tag = "u"
        elif tag == "strikethrough":
            tag = "s"
        elif tag == "code":
            tag = "code"
        elif tag == "pre":
            tag = "pre"
        elif tag == "spoiler":
            tag = "span class=\"tg-spoiler\""
        elif tag in ["blockquote", "expandable_blockquote"]:
            tag = "blockquote expandable"
        elif tag == "text_link":
            tag = f'a href="{entity.url}"'
        elif tag == "url":
            continue

        start = utf16_index_to_py_index.get(entity.offset, None)
        end = utf16_index_to_py_index.get(entity.offset + entity.length - 1, None)
        if start is None or end is None:
            continue
        end += 1  # включительно
        tag_events[start].append(("open", tag))
        tag_events[end].append(("close", tag.split()[0]))

    result = ""
    open_tags = []

    for i, ch in enumerate(raw_text):
        if i in tag_events:
            for event, tag in sorted(tag_events[i], key=lambda x: x[0] != "close"):
                if event == "close":
                    while open_tags:
                        last = open_tags.pop()
                        result += f"</{last}>"
                        if last == tag:
                            break
                elif event == "open":
                    tagname = tag.split()[0]
                    open_tags.append(tagname)
                    result += f"<{tag}>"
        result += escape(ch)

    while open_tags:
        result += f"</{open_tags.pop()}>"

    return result

def add_plain_links(text):
    # Регулярное выражение для поиска обычных ссылок
    url_pattern = re.compile(r"(https?://[^\s]+)")
    return url_pattern.sub(r'<a href="\1">\1</a>', text)

async def cancel_creation_from_edit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отменяет процесс редактирования текста кнопки и возвращает к действиям фрагмента."""
    logger.info("Вызвана отмена из состояния редактирования кнопки.")
    # Определяем, из какого фрагмента мы пришли
    fragment_id = context.user_data.get('editing_choice_fragment_id', context.user_data.get('current_fragment_id'))

    # Чистим все временные данные, связанные с редактированием кнопки
    context.user_data.pop('editing_choice_fragment_id', None)
    context.user_data.pop('choice_key_to_edit', None)
    context.user_data.pop('editable_choice_keys', None)

    await update.message.reply_text("Редактирование текста кнопки отменено.")

    if fragment_id and 'current_story' in context.user_data:
         # Возвращаемся к отображению действий для этого фрагмента
         await show_fragment_actions(update, context, fragment_id)
         return ADD_CONTENT
    else:
         # Если что-то пошло не так, возвращаемся в главное меню или начальное состояние
         logger.warning("Не удалось определить фрагмент для возврата после отмены редактирования кнопки.")
         # await start(update, context) # или другая логика возврата
         return ConversationHandler.END # Завершаем диалог как fallback



async def handle_edit_choice_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Начинает процесс редактирования текста кнопки: показывает кнопки выбора."""
    query = update.callback_query
    await query.answer()
    data = query.data  # format: edit_choice_start_{fragment_id}

    try:
        prefix = 'edit_choice_start_'
        if data.startswith(prefix):
            fragment_id = data[len(prefix):]
        else:
            # Если префикс не найден, это неожиданные данные.
            raise ValueError("Callback_data не содержит ожидаемый префикс.")
    except (IndexError, ValueError) as e:
        logger.error(f"Не удалось извлечь fragment_id из callback_data: {data}. Ошибка: {e}")
        await query.edit_message_text("Ошибка: Некорректные данные для редактирования.")
        current_fragment_id_fallback = context.user_data.get('current_fragment_id', '1')
        # Проверяем, существует ли такой фрагмент перед вызовом show_fragment_actions
        if 'current_story' in context.user_data and \
           context.user_data['current_story'].get("fragments", {}).get(current_fragment_id_fallback):
            await show_fragment_actions(update, context, current_fragment_id_fallback)
        else:
            await query.edit_message_text("Не удалось вернуться к предыдущему меню. Попробуйте начать заново.")
        return ADD_CONTENT

    context.user_data['editing_choice_fragment_id'] = fragment_id

    story_data = context.user_data.get('current_story')
    if not story_data:
        logger.error(f"В user_data отсутствует 'current_story' при попытке редактирования кнопки для fragment_id: {fragment_id}")
        await query.edit_message_text("Ошибка: Данные истории не найдены.")
        return ADD_CONTENT # Или другое подходящее состояние

    # Новая логика: choices это список словарей
    # Используем .get() с дефолтными значениями для безопасности
    choices_list = story_data.get("fragments", {}).get(fragment_id, {}).get("choices", [])

    if not choices_list:
        await query.edit_message_text("В этом фрагменте нет кнопок выбора для редактирования.")
        await show_fragment_actions(update, context, fragment_id)
        return ADD_CONTENT

    keyboard = []
    # 'editable_choice_keys' больше не нужен, так как мы используем индекс

    for i, choice_item in enumerate(choices_list):
        choice_text = choice_item.get("text", "Текст отсутствует") # Получаем текст кнопки
        # Используем индекс в callback_data
        keyboard.append([InlineKeyboardButton(f"'{choice_text}'", callback_data=f'edit_choice_select_{i}')])

    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data=f'edit_choice_cancel')])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text("Выберите кнопку, текст которой хотите изменить:", reply_markup=reply_markup)

    return SELECT_CHOICE_TO_EDIT










async def handle_select_choice_to_edit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает выбор кнопки для редактирования и запрашивает новый текст."""
    query = update.callback_query
    await query.answer()
    data = query.data
    logger.info(f"context.user_data: {context.user_data}")
    fragment_id = context.user_data.get('editing_choice_fragment_id')

    if not fragment_id:
        logger.error("editing_choice_fragment_id отсутствует в user_data на этапе выбора кнопки.")
        await query.edit_message_text("Произошла ошибка состояния. Попробуйте начать редактирование заново.")
        current_fragment_id_fallback = context.user_data.get('current_fragment_id', '1')
        if 'current_story' in context.user_data and \
           context.user_data['current_story'].get("fragments", {}).get(current_fragment_id_fallback):
            await show_fragment_actions(update, context, current_fragment_id_fallback)
        else:
            await query.edit_message_text("Не удалось вернуться к меню фрагмента.")
        return ADD_CONTENT

    if data == 'edit_choice_cancel':
        context.user_data.pop('editing_choice_fragment_id', None)
        await query.edit_message_text("Редактирование отменено.")
        await show_fragment_actions(update, context, fragment_id)
        return ADD_CONTENT

    try:
        prefix = 'edit_choice_select_'
        if not data.startswith(prefix):
            raise ValueError("Неверный формат callback_data для выбора кнопки.")
        choice_index_to_edit = int(data[len(prefix):])

        story_data = context.user_data.get('current_story')
        if not story_data:
            logger.error(f"current_story отсутствует в user_data при выборе кнопки для fragment_id: {fragment_id}")
            raise ValueError("Данные истории не найдены.")

        choices_list = story_data.get("fragments", {}).get(fragment_id, {}).get("choices", [])

        if not (0 <= choice_index_to_edit < len(choices_list)):
            logger.warning(f"Индекс кнопки {choice_index_to_edit} вне диапазона ({len(choices_list)}) для fragment_id {fragment_id}.")
            raise ValueError(f"Выбранная кнопка не найдена.")

        choice_to_edit_data = choices_list[choice_index_to_edit]
        current_choice_text = choice_to_edit_data.get("text", "Текст отсутствует")

        # Добавим эффекты, если они есть
        effects = choice_to_edit_data.get("effects", [])
        if effects:
            effect_parts = []
            for effect in effects:
                stat = effect.get("stat", "???")
                value = effect.get("value", "?")
                hide = effect.get("hide", False)
                if hide:
                    effect_parts.append(f"{stat}:{value}(hide)")
                else:
                    effect_parts.append(f"{stat}:{value}")
            effects_str = "{{" + ", ".join(effect_parts) + "}}"
            current_choice_text += f" {effects_str}"

    except (IndexError, ValueError, TypeError) as e:
        logger.error(f"Ошибка при извлечении индекса/ключа для редактирования из {data} для fragment_id {fragment_id}: {e}")
        context.user_data.pop('editing_choice_fragment_id', None)
        await query.edit_message_text(f"Произошла ошибка при выборе кнопки: {e}. Попробуйте снова.")
        await show_fragment_actions(update, context, fragment_id)
        return ADD_CONTENT
    
    context.user_data['choice_index_to_edit'] = choice_index_to_edit
    
    escaped_choice_text = html.escape(current_choice_text)
    
    await query.edit_message_text(
        f"Вы выбрали кнопку: <code>{escaped_choice_text}</code>.\nВведите новый текст для этой кнопки:",
        parse_mode='HTML'
    )
    
    return AWAITING_NEW_CHOICE_TEXT





async def handle_new_choice_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Получает новый текст для кнопки, обновляет данные истории и сохраняет."""
    new_text = update.message.text.strip()

    check_text = re.sub(r"\{\{.*?\}\}", '', new_text).strip()

    if not check_text or len(check_text) > 355: # Ограничение длины текста кнопки
        await update.message.reply_text("Текст кнопки не может быть пустым и должен быть не длиннее 35 символов. Попробуйте снова:")
        return AWAITING_NEW_CHOICE_TEXT

    fragment_id = context.user_data.get('editing_choice_fragment_id')
    choice_index_to_edit = context.user_data.get('choice_index_to_edit') # Теперь это индекс

    if fragment_id is None or choice_index_to_edit is None or not isinstance(choice_index_to_edit, int):
        logger.error(f"Отсутствуют или некорректны fragment_id ('{fragment_id}') или choice_index_to_edit ('{choice_index_to_edit}') в user_data.")
        await update.message.reply_text("Произошла внутренняя ошибка состояния. Редактирование прервано.")

        # Очистка временных данных из контекста
        current_fragment_id_fallback = context.user_data.get('current_fragment_id', fragment_id or '1')
        context.user_data.pop('editing_choice_fragment_id', None)
        context.user_data.pop('choice_index_to_edit', None)

        if 'current_story' in context.user_data and \
           context.user_data['current_story'].get("fragments", {}).get(current_fragment_id_fallback):
            await show_fragment_actions(update, context, current_fragment_id_fallback)
        else:
            await update.message.reply_text("Не удалось вернуться к редактированию фрагмента.")
        return ADD_CONTENT

    story_data = context.user_data.get('current_story')
    if not story_data:
        logger.error("current_story отсутствует в user_data при обновлении текста кнопки.")
        await update.message.reply_text("Ошибка: данные истории не найдены. Редактирование прервано.")
        context.user_data.pop('editing_choice_fragment_id', None)
        context.user_data.pop('choice_index_to_edit', None)
        return ADD_CONTENT # Или другое подходящее состояние

    # Получаем текущий список choices
    choices_list = story_data.get("fragments", {}).get(fragment_id, {}).get("choices", [])

    # Проверяем, валиден ли индекс (кнопка могла быть удалена в другом сеансе)
    if not (0 <= choice_index_to_edit < len(choices_list)):
        logger.warning(f"Индекс кнопки {choice_index_to_edit} для редактирования недействителен для fragment_id {fragment_id}. Количество кнопок: {len(choices_list)}.")
        await update.message.reply_text(f"Выбранная кнопка больше не найдена. Возможно, она была изменена или удалена.")
        context.user_data.pop('editing_choice_fragment_id', None)
        context.user_data.pop('choice_index_to_edit', None)
        await show_fragment_actions(update, context, fragment_id)
        return ADD_CONTENT

    old_text = choices_list[choice_index_to_edit].get("text", "N/A") # Для логгирования и сообщений

    # Проверка, не используется ли новый текст уже в ДРУГОЙ кнопке этого фрагмента


    cleaned_text, parsed_effects, errors = parse_effects_from_text(new_text)

    if errors:
        await update.message.reply_text(
            "Обнаружены ошибки в тегах эффектов:\n\n" + "\n".join(errors) + "\n\nПожалуйста, исправьте их и отправьте текст снова."
        )
        return AWAITING_NEW_CHOICE_TEXT


    for i, choice_item in enumerate(choices_list):
        if i != choice_index_to_edit and choice_item.get("text") == cleaned_text:
            await update.message.reply_text(f"Текст '{cleaned_text}' уже используется для другой кнопки в этом фрагменте. Введите другое название:")
            return AWAITING_NEW_CHOICE_TEXT

    # --- Начало измененной логики для обновления списка ---
    # Обновляем текст у элемента списка по его индексу. 'target' остается неизменным.


    try:
        choice_entry = context.user_data['current_story']['fragments'][fragment_id]['choices'][choice_index_to_edit]
        choice_entry['text'] = cleaned_text
        choice_entry['effects'] = parsed_effects
    except (KeyError, IndexError, TypeError) as e:
        logger.error(f"Ошибка при обновлении текста кнопки: {e}. fragment_id={fragment_id}, choice_index={choice_index_to_edit}")
        await update.message.reply_text("Произошла ошибка при сохранении изменений. Попробуйте снова.")
        # Не очищаем данные, чтобы пользователь мог попробовать еще раз или отменить
        return AWAITING_NEW_CHOICE_TEXT # или можно вернуть к show_fragment_actions

    # --- Конец измененной логики ---
    logger.info(f"Текст кнопки во фрагменте '{fragment_id}' (индекс {choice_index_to_edit}) изменен с '{old_text}' на '{new_text}'.")

    save_current_story_from_context(context) # Сохраняем изменения

    # Очищаем временные данные из контекста
    context.user_data.pop('editing_choice_fragment_id', None)
    context.user_data.pop('choice_index_to_edit', None)

    await update.message.reply_text(f"Текст кнопки успешно изменен на '{new_text}'.")
    await show_fragment_actions(update, context, fragment_id)
    return ADD_CONTENT






async def handle_prev_fragment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data  # например, "prev_fragment_go_left_44"
    logger.info(f"data: {data}")      

    current_id = data.replace("prev_fragment_", "", 1)
    logger.info(f"фрагмента: {current_id}")    

    story_data = context.user_data.get("current_story", {})
    fragments = story_data.get("fragments", {})

    def get_parent_fragment_id(fragment_id: str) -> str | None:
        match = re.match(r"(.+?)_(\d+)$", fragment_id)
        if not match:
            return None
        
        base, num = match.groups()
        num = int(num)

        if num > 1:
            return f"{base}_{num - 1}"
        else:
            # num == 1, ищем кто ссылается на этот фрагмент
            referring = [
                fid for fid, frag in fragments.items()
                if fid != fragment_id and any(choice.get("target") == fragment_id for choice in frag.get("choices", []))
            ]
            if not referring:
                return None

            def extract_suffix(frag_id: str) -> int:
                match = re.match(r".+?_(\d+)$", frag_id)
                return int(match.group(1)) if match else float('inf')

            main_refs = [fid for fid in referring if fid.startswith("main_")]
            if main_refs:
                return min(main_refs, key=extract_suffix)
            return min(referring, key=extract_suffix)

    parent_id = get_parent_fragment_id(current_id)
    if parent_id:
        current_id = parent_id

    logger.info(f"Переход к предыдущему фрагменту: {current_id}")
    context.user_data['current_fragment_id'] = current_id
    await show_fragment_actions(update, context, current_id)



async def dellink_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    fragment_id = query.data.replace("dellink_cancel_", "")
    await show_fragment_actions(update, context, fragment_id)

# удаление связи
async def select_choice_to_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info("Вызвана функция select_choice_to_delete")

    query = update.callback_query
    if not query:
        logger.warning("Нет callback_query в update")
        return

    user_id = update.effective_user.id if update.effective_user else "Неизвестно"
    logger.info(f"Пользователь: {user_id}")
    logger.info(f"Данные callback_query: {query.data}")

    if not query.data.startswith("d_c_s_"):
        logger.warning(f"Неизвестный формат callback_data: {query.data}")
        return

    fragment_id = query.data.replace("d_c_s_", "")
    logger.info(f"ID фрагмента для удаления: {fragment_id}")

    if "current_story" not in context.user_data:
        logger.warning("Отсутствует ключ 'current_story' в context.user_data")
        return

    story_data = context.user_data["current_story"]
    logger.info(f"Текущие данные истории: {story_data}")

    fragments = story_data.get("fragments")
    if fragments is None:
        logger.warning("Отсутствует ключ 'fragments' в story_data")
        return

    fragments = story_data.get("fragments")
    fragment = fragments.get(fragment_id) if fragments else None
    if not fragment:
        logger.warning(f"Фрагмент с ID {fragment_id} не найден")
        return

    choices = fragment.get("choices", [])
    if len(choices) <= 1:
        await query.answer("Удаление невозможно: нет двух и более связей.")
        return

    keyboard = [
        [InlineKeyboardButton(f"❌ {choice['text']} ➡️ {choice['target']}",
                              callback_data=f"c_d_c_{choice['text']}_{fragment_id}")]
        for choice in choices
    ]
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data=f"dellink_cancel_{fragment_id}")])

    await query.edit_message_text(
        "Выберите связь для удаления:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
def find_reachable_fragments(fragments: dict, start_id: str) -> set:
    visited = set()
    queue = [start_id]
    while queue:
        current = queue.pop()
        if current in visited:
            continue
        visited.add(current)
        current_fragment = fragments.get(current, {})
        for choice in current_fragment.get("choices", []):
            next_id = choice.get("target")
            if next_id and next_id not in visited:
                queue.append(next_id)
    return visited

async def confirm_delete_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data.replace("c_d_c_", "")
    first_underscore_index = data.find("_")
    choice_text = data[:first_underscore_index]
    fragment_id = data[first_underscore_index + 1:]

    story_data = context.user_data["current_story"]
    fragments = story_data["fragments"]
    fragment = fragments.get(fragment_id)


    if not fragment:
        await query.answer("Фрагмент не найден.")
        return

    # Удаляем элемент из списка choices по 'text'
    choices = fragment.get("choices", [])
    deleted_target = None
    new_choices = []
    for choice in choices:
        if choice["text"] == choice_text:
            deleted_target = choice["target"]
            continue
        new_choices.append(choice)

    if len(new_choices) == len(choices):
        await query.answer("Связь уже удалена.")
        return

    fragment["choices"] = new_choices
    save_current_story_from_context(context)

    # Проверка на недостижимые фрагменты
    reachable = find_reachable_fragments(fragments, "main_1")
    unreachable = [frag_id for frag_id in fragments if frag_id not in reachable]

    warning_text = ""
    if unreachable:
        warning_text = "\n⚠️ После удаления этой связи стали недоступны следующие фрагменты:\n" + \
                       "\n".join(f"- `{frag_id}`" for frag_id in unreachable)

    else:
        await query.answer("Связь уже удалена.")

    # Повторно показать фрагмент с обновлённой клавиатурой
    current_text = fragment.get("text", "") or "*Нет текста*"
    text_to_send = clean_html_for_telegram(current_text)    
    media_desc = ""
    current_media = fragment.get("media", [])
    if current_media:
        media_counts = defaultdict(int)
        for item in current_media:
            media_counts[item.get("type", "unknown")] += 1
        media_desc = ", ".join([f"{count} {m_type}" for m_type, count in media_counts.items()])
        media_desc = f"\nМедиа: [{media_desc}]"

    user_id_str = str(update.effective_user.id)
    story_id = context.user_data.get("story_id")

    reply_markup = build_fragment_action_keyboard(
        fragment_id=fragment_id,
        story_data=story_data,
        user_id_str=user_id_str,
        story_id=story_id
    )

    await query.edit_message_text(
        f"Связь <code>{choice_text}</code> удалена из фрагмента <code>{fragment_id}</code>.{warning_text}\n\n"
        f"Фрагмент: <code>{fragment_id}</code>\n"
        f"Текущий текст: \n<pre>{text_to_send}</pre>{media_desc}\n\n"
        f"Вы можете изменить его или воспользоваться кнопками ниже:",
        reply_markup=reply_markup,
        parse_mode=ParseMode.HTML
    )


async def show_fragment_actions(update: Update, context: ContextTypes.DEFAULT_TYPE, fragment_id: str):
    if 'current_story' not in context.user_data or 'story_id' not in context.user_data:
        logger.error("Отсутствуют current_story или story_id в user_data при вызове show_fragment_actions")
        target_message = update.message or (update.callback_query.message if update.callback_query else None)
        if target_message:
            await target_message.reply_text("Произошла ошибка состояния. Попробуйте начать заново.")
        elif update.effective_chat:
            await context.bot.send_message(update.effective_chat.id, "Произошла ошибка состояния. Попробуйте начать заново.")
        return ConversationHandler.END

    story_id = context.user_data['story_id']
    user_id_str = str(update.effective_user.id)

    # Загрузка всех данных
    # Загружаем историю с учётом coop_edit-доступа
    story_data = load_user_story(user_id_str, story_id)

    if not story_data:
        await update.effective_message.reply_text("У вас нет доступа к просмотру этой истории.")
        return ConversationHandler.END

    # Сохраняем историю и user_id владельца (если нужно — можно определить его отдельно)
    context.user_data['current_story'] = copy.deepcopy(story_data)

    current_fragment = story_data.get("fragments", {}).get(fragment_id)
    if not current_fragment:
        logger.error(f"Фрагмент {fragment_id} не найден в истории {story_id}")
        target_message = update.message or (update.callback_query.message if update.callback_query else None)
        if target_message:
            await target_message.reply_text(f"Ошибка: Фрагмент {fragment_id} не найден.")
        elif update.effective_chat:
            await context.bot.send_message(update.effective_chat.id, f"Ошибка: Фрагмент {fragment_id} не найден.")
        return ADD_CONTENT

    current_choices = current_fragment.get("choices", [])
    first_choice = current_choices[0] if current_choices else None
    remaining_choices = current_choices[1:] if current_choices else []
    choice_items = current_choices
    has_choices = len(choice_items) > 0

    keyboard = [[InlineKeyboardButton("🦊 Предпросмотр фрагмента", callback_data=f"preview_fragment_{fragment_id}")]]

    # --- Верхняя строка с "⬅️ Предыдущий фрагмент" и первой кнопкой (если есть) ---
    if fragment_id != "main_1":
        row = [InlineKeyboardButton("⬅️ Шаг назад", callback_data=f'prev_fragment_{fragment_id}')]
        if has_choices:
            choice_text = choice_items[0]["text"]
            target_fragment_id = choice_items[0]["target"]
            short_id = target_fragment_id[-1]
            row.append(InlineKeyboardButton(f"➡️Шаг вперёд: {choice_text}", callback_data=f'goto_{target_fragment_id}'))
        keyboard.append(row)
    elif has_choices:
        # Только "следующий фрагмент" без "назад"
        choice_text = choice_items[0]["text"]
        target_fragment_id = choice_items[0]["target"]
        short_id = target_fragment_id[-1]
        keyboard.append([
            InlineKeyboardButton(f"➡️Шаг вперёд: {choice_text}", callback_data=f'goto_{target_fragment_id}')
        ])

    # --- Кнопки добавления переходов ---
    branch_button_text = "➕ Добавить тут вариант развилки" if has_choices else "➕ Добавить вариант выбора (развилку)"
    # Определим, существует ли следующий по порядку фрагмент
    match = re.match(r"(.+?)_(\d+)$", fragment_id)
    logger.info(f"match: {match}")    
    if match:
        prefix, number = match.groups()
        next_fragment_id = f"{prefix}_{int(number) + 1}"
        logger.info(f"next_fragment_id: {next_fragment_id}")        
        if next_fragment_id in story_data.get("fragments", {}):
            continue_button_text = f"➡️✏️Вставить после {fragment_id} событие"
            continue_callback = f"continue_linear"
        else:
            continue_button_text = "➡️➡️ Продолжить ветку линейно"
            continue_callback = 'continue_linear'
    else:
        continue_button_text = "➡️➡️ Продолжить ветку линейно"
        continue_callback = 'continue_linear'

    keyboard.extend([
        [InlineKeyboardButton(continue_button_text, callback_data=continue_callback)],
        [InlineKeyboardButton(branch_button_text, callback_data='add_branch')],
        [InlineKeyboardButton("🔗 Связать с другим", callback_data='link_to_previous')],
    ])
    if len(current_choices or []) > 1:
        keyboard.append([
            InlineKeyboardButton("🗑️ Удалить связь", callback_data=f"d_c_s_{fragment_id}")
        ])
    if current_choices:
        keyboard.append([InlineKeyboardButton("━━━━━━━━━━ ✦ ━━━━━━━━━━", callback_data='separator_transitions_header')])

        if len(current_choices or []) > 1:
            # Кнопка для запуска изменения порядка
            keyboard.append([InlineKeyboardButton("🔀 ----- Существующие переходы: -----",
                                                 callback_data=f"{REORDER_CHOICES_START_PREFIX}{fragment_id}")])
        else:
            # Просто заголовок, если менять порядок нельзя
            keyboard.append([InlineKeyboardButton("----- Существующие переходы: -----",
                                                 callback_data='noop_transitions_header')]) # noop_ чтобы не было реакции
        
        rows = []
        for i in range(0, len(current_choices), 2):
            row = []
            for choice in current_choices[i:i + 2]:
                choice_text = choice["text"]
                target_fragment_id = choice["target"]
                row.append(InlineKeyboardButton(f"'{choice_text}' ➡️ {target_fragment_id}", callback_data=f'goto_{target_fragment_id}'))
            rows.append(row)
        keyboard.extend(rows)

        keyboard.append([
            InlineKeyboardButton("✏️ Редактировать текст кнопок", callback_data=f'edit_choice_start_{fragment_id}')
        ])
        keyboard.append([InlineKeyboardButton("━━━━━━━━━━ ✦ ━━━━━━━━━━", callback_data='separator')])
    keyboard.append([
        InlineKeyboardButton(
            "🛠️ Открыть веб редактор",
            web_app=WebAppInfo(url=f"https://novel-qg4c.onrender.com/{user_id_str}_{story_id}")
        )
    ])
    keyboard.append([
        InlineKeyboardButton("🗺️ Карта/Редактировать структуру", callback_data=f"edit_story_{user_id_str}_{story_id}")
    ])
    keyboard.append([InlineKeyboardButton("💾 Завершить и сохранить историю", callback_data='finish_story')])

    reply_markup = InlineKeyboardMarkup(keyboard)

    text_lines = [f"<b>Фрагмент успешно добавлен/обновлён</b>\n"]

    text_lines.append(f"Текущий фрагмент: <code>{fragment_id}</code>")

    media = current_fragment.get("media", [])
    if media:
        types_count = {}
        for item in media:
            media_type = item.get("type", "unknown")
            types_count[media_type] = types_count.get(media_type, 0) + 1
        media_lines = [f"{media_type}: {count}" for media_type, count in types_count.items()]
        text_lines.append("Медиа: " + ", ".join(media_lines))

    text = current_fragment.get("text", "").strip()
    if text:
        text_lines.append(f"Текст: \n✦ ━━━━━━━━━━\n{text}\n✦ ━━━━━━━━━━")


    # Добавим информацию об эффектах, если они есть
    for choice in current_choices:
        if "effects" in choice:
            effect_lines = []
            for effect in choice["effects"]:
                stat = html.escape(str(effect.get("stat", "неизвестно")))
                if effect.get("hide"):
                    stat = f"{stat}(Скрытый)"
                raw_value = str(effect.get("value", "?"))
                value = html.escape(raw_value)

                # Определяем тип эффекта по raw_value
                if re.match(r'^[+-]?\(\-?\d+\-+\-?\d+\)$', raw_value):  # например +(5-9), -(3--8)
                    verb = "меняет атрибут"
                elif re.match(r'^[+-]\d+$', raw_value):  # например +5, -3
                    verb = "меняет атрибут"
                elif re.match(r'^[<>=]', raw_value):  # сравнение
                    verb = "проверка атрибута"
                elif re.match(r'^-?\d+\-+\-?\d+$', raw_value):  # диапазон: 3-5, -2--1, 3--8
                    verb = "задаёт атрибут"
                elif re.match(r'^-?\d+$', raw_value):  # просто число: 7, -1
                    verb = "задаёт атрибут"
                else:
                    verb = "неизвестный эффект"

                effect_lines.append(f"{stat}: {value} ({verb})")

            if effect_lines:
                effects_text = ", ".join(effect_lines)
                text_lines.append(
                    f"\n\n🔸 Выбор <b>«{html.escape(choice['text'])}»</b> ведущий на фрагмент <code>{html.escape(choice['target'])}</code> имеет эффект: {effects_text}"
                )

    # Добавим пояснение и финальное действие
    text_lines.append(
        "\n\n<i>Если сейчас вы отправите боту новый текст или медиа-контент, то он заменит прошлый в данном слайде</i>\n"
    )
    text_lines.append("Либо выберите действие:")

    text_to_send = "\n".join(text_lines)
    text_to_send = clean_html_for_telegram(text_to_send)
    # Попробуем отредактировать, если это был callback_query
    if update.callback_query:
        try:
            await update.callback_query.edit_message_text(
                text=text_to_send,
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
            return # Выходим, если редактирование успешно
        except Exception as e:
            logger.warning(f"Не удалось отредактировать сообщение для show_fragment_actions: {e}")
            # Если не удалось отредактировать, отправим новое

    # Отправляем новое сообщение, если редактирование не удалось или это было не callback_query
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if target_message:
        # Если предыдущее сообщение было от бота, попробуем удалить его перед отправкой нового
        # Это помогает избежать дублирования меню действий
        if update.callback_query and update.callback_query.message.from_user.is_bot:
             try:
                 await update.callback_query.delete_message()
             except Exception as e:
                 logger.warning(f"Не удалось удалить старое сообщение: {e}")

        await context.bot.send_message( # Используем send_message вместо reply_text для чистоты
            chat_id=update.effective_chat.id,
            text=text_to_send,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    else:
        logger.warning("Не удалось найти target_message для отправки show_fragment_actions")
        await context.bot.send_message(
             chat_id=update.effective_chat.id,
             text=text_to_send + "\n(Не удалось обновить предыдущее сообщение)",
             reply_markup=reply_markup,
             parse_mode='HTML'
         )



def build_fragment_selection_keyboard(
    user_id_str: str,
    story_id: str,
    fragment_ids: list[str],
    current_page: int,
    callback_prefix: str,
    items_per_page: int = FRAGMENT_BUTTONS_PER_PAGE
) -> InlineKeyboardMarkup:
    """Создает клавиатуру для выбора фрагмента с пагинацией."""
    keyboard = []
    total_fragments = len(fragment_ids)
    total_pages = (total_fragments + items_per_page - 1) // items_per_page

    start_index = (current_page - 1) * items_per_page
    end_index = start_index + items_per_page
    page_fragment_ids = fragment_ids[start_index:end_index]

    # Кнопки для фрагментов на текущей странице
    keyboard.append([InlineKeyboardButton("🗺️ Посмотреть карту", callback_data=f"show_map_{story_id}")])    
    # Кнопки для фрагментов на текущей странице (по 2 в строку)
    row = []
    for i, fragment_id in enumerate(page_fragment_ids, start=1):
        button_text = f"{fragment_id}"
        callback_data = f"{callback_prefix}{fragment_id}"
        row.append(InlineKeyboardButton(button_text, callback_data=callback_data))
        if len(row) == 2:
            keyboard.append(row)
            row = []

    if row:
        # Добавляем оставшуюся кнопку, если не кратно 2
        keyboard.append(row)
    # Кнопки пагинации
    pagination_buttons = []
    if current_page > 1:
        # Нужен callback_data для пагинации, который будет обработан в select_link_target_handler
        pagination_buttons.append(
            InlineKeyboardButton("◀️ Назад", callback_data=f"{callback_prefix}page_{current_page - 1}")
        )
    if current_page < total_pages:
        pagination_buttons.append(
            InlineKeyboardButton("Вперед ▶️", callback_data=f"{callback_prefix}page_{current_page + 1}")
        )

    if pagination_buttons:
        keyboard.append(pagination_buttons)

    # Кнопка отмены выбора
    # Нужен callback_data для отмены, который будет обработан в select_link_target_handler
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data=f"{callback_prefix}cancel")])

    return InlineKeyboardMarkup(keyboard)

# --- Вам также понадобится функция build_legend_text (если ее еще нет глобально) ---


# --- Убедитесь, что импортирован html ---





async def select_link_target_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает выбор целевого фрагмента для ссылки или пагинацию/отмену."""
    query = update.callback_query
    await query.answer()
    data = query.data

    logger.info(f"Выбор цели ссылки: {data}")

    # --- Получаем данные из контекста ---
    # Убедимся, что все необходимые данные есть в user_data перед их использованием
    if not all(k in context.user_data for k in ['current_story', 'story_id', 'current_fragment_id', 'pending_link_button_text']):
        if data == f"{'select_link_target_'}cancel": # Проверяем, если это отмена, чтобы не выводить ошибку
            pass # Продолжаем обработку отмены ниже
        elif data.startswith(f"{'select_link_target_'}page_"): # Проверяем пагинацию
             pass # Продолжаем обработку пагинации ниже
        else:
            logger.error("Недостаточно данных в context.user_data для select_link_target_handler.")
            await query.edit_message_text("Произошла внутренняя ошибка (отсутствуют данные). Попробуйте снова.")
            # Возвращаемся в безопасное состояние, например, главное меню или начало диалога
            # await start(update, context) # Пример возврата в главное меню
            return ConversationHandler.END # Или другое подходящее состояние/завершение

    story_data = context.user_data['current_story']
    story_id = context.user_data['story_id']
    user_id_str = str(update.effective_user.id) # Получаем user_id из апдейта на всякий случай
    context.user_data['user_id_str'] = user_id_str # Сохраняем на всякий случай
    current_fragment_id = context.user_data['current_fragment_id']
    callback_prefix = 'select_link_target_'

    # Обработка Отмены
    if data == f"{callback_prefix}cancel":
        await query.edit_message_text("Создание ссылки отменено.")
        context.user_data.pop('pending_link_button_text', None)
        context.user_data.pop('current_link_target_page', None)
        await show_fragment_actions(update, context, current_fragment_id)
        return ADD_CONTENT

    # Обработка Пагинации
    if data.startswith(f"{callback_prefix}page_"):
        try:
            new_page = int(data.split('_')[-1])
            context.user_data['current_link_target_page'] = new_page

            all_fragment_ids = sorted(story_data.get("fragments", {}).keys())
            def get_sort_key_by_timing(fragment):
                text = story_data['fragments'][fragment].get('text', '')
                steps = parse_timed_edits(text)
                return steps[0]['delay'] if steps else 0  # Сортировка по первому delay, если есть

            targetable_fragment_ids = sorted(
                all_fragment_ids,
                key=get_sort_key_by_timing
            )
                   


            reply_markup = build_fragment_selection_keyboard(
                user_id_str=user_id_str,
                story_id=story_id,
                fragment_ids=targetable_fragment_ids,
                current_page=new_page,
                callback_prefix=callback_prefix
            )
            legend_text = build_legend_text(story_data, targetable_fragment_ids[
                                                         (new_page - 1) * FRAGMENT_BUTTONS_PER_PAGE:
                                                         new_page * FRAGMENT_BUTTONS_PER_PAGE
                                                         ])
            button_text = context.user_data.get('pending_link_button_text', '...')

            await query.edit_message_text(
                f"Текст кнопки: '{button_text}'.\n"
                f"Выберите фрагмент, на который эта кнопка будет ссылаться (страница {new_page}):\n\n"
                f"{legend_text}",
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML
            )
            return SELECT_LINK_TARGET
        except (ValueError, IndexError, KeyError) as e: # Добавлен KeyError на случай проблем с story_data
            logger.error(f"Ошибка пагинации выбора цели: {e}")
            await query.edit_message_text("Ошибка при переключении страницы.")
            context.user_data.pop('pending_link_button_text', None)
            context.user_data.pop('current_link_target_page', None)
            await show_fragment_actions(update, context, current_fragment_id)
            return ADD_CONTENT



    story_data = load_user_story(user_id_str, story_id)

    if not story_data:
        await update.effective_message.reply_text("У вас нет доступа к просмотру этой истории.")
        return ConversationHandler.END

    try:
        user_id = int(user_id_str)
        actual_owner_id = get_owner_id_or_raise(user_id, story_id, story_data)
    except PermissionError as e:
        logger.warning(str(e))
        await query.edit_message_text("У вас нет прав для редактирования этой истории.")
        return ConversationHandler.END

    # Подменим user_id в context на владельца
    context.user_data['user_id_str'] = actual_owner_id

    # Убираем использование all_data['users_story'][owner][story_id]
    # Просто сохраняем уже полученный story_data
    context.user_data['current_story'] = story_data

    # Обработка выбора фрагмента
    if data.startswith(callback_prefix):
        target_fragment_id = data[len(callback_prefix):]
        # Проверяем наличие текста кнопки перед удалением
        if 'pending_link_button_text' not in context.user_data:
             logger.error("Ключ 'pending_link_button_text' отсутствует в user_data при выборе цели.")
             await query.edit_message_text("Произошла ошибка состояния. Попробуйте добавить ссылку заново.")
             await show_fragment_actions(update, context, current_fragment_id)
             return ADD_CONTENT

        button_text = context.user_data.pop('pending_link_button_text') # Удаляем текст кнопки
        context.user_data.pop('current_link_target_page', None) # Чистим страницу пагинации

        # Проверяем, существует ли целевой фрагмент
        if target_fragment_id not in story_data.get("fragments", {}):
            logger.error(f"Выбранный целевой фрагмент '{target_fragment_id}' не найден.")
            await query.edit_message_text("Выбранный фрагмент не найден. Попробуйте снова.")
            await show_fragment_actions(update, context, current_fragment_id)
            return ADD_CONTENT

        # --- Парсинг текста кнопки и эффектов --

        cleaned_text, effects, errors = parse_effects_from_text(button_text)

        if errors:
            await update.message.reply_text(
                "Обнаружены ошибки в тегах эффектов:\n\n" + "\n".join(errors) + "\n\nПожалуйста, исправьте их и отправьте текст снова."
            )
            return SELECT_LINK_TARGET


        # --- Добавление выбора в фрагмент ---
        if 'choices' not in story_data['fragments'][current_fragment_id]:
            story_data['fragments'][current_fragment_id]['choices'] = []

        choice_data = {
            "text": cleaned_text,
            "target": target_fragment_id
        }

        if effects:
            choice_data["effects"] = effects

        story_data['fragments'][current_fragment_id]['choices'].append(choice_data)

        context.user_data['current_story'] = story_data
        logger.info(f"Добавлена ссылка из '{current_fragment_id}' на '{target_fragment_id}' с текстом '{cleaned_text}' и эффектами {effects}.")
        save_current_story_from_context(context)
        # -----------------------------------------

        # Показываем обновленное меню действий
        await query.edit_message_text(f"Ссылка '{button_text}' на фрагмент `{target_fragment_id}` успешно добавлена.")
        await show_fragment_actions(update, context, current_fragment_id)
        return ADD_CONTENT # Возвращаемся в состояние добавления контента/действий

    # Если callback_data не распознан (не отмена, не пагинация, не выбор)
    logger.warning(f"Нераспознанный callback_data в select_link_target_handler: {data}")
    await query.edit_message_text("Неизвестное действие.")
    # Возвращаемся к действиям текущего фрагмента
    await show_fragment_actions(update, context, current_fragment_id)
    return ADD_CONTENT




async def ask_link_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.callback_query:
        query = update.callback_query
        await query.answer()
        action = query.data
    else:
        action = None

    # Обработка отмены
    if action == 'link_cancel':
        current_fragment_id = context.user_data.get('current_fragment_id')
        if not current_fragment_id:
            await query.edit_message_text("Ошибка: не найден текущий ID фрагмента. Попробуйте /restart")
            return ConversationHandler.END
        await show_fragment_actions(update, context, current_fragment_id)
        return ADD_CONTENT

    # Получаем текст кнопки
    button_text = update.message.text if update.message else None

    check_text = re.sub(r"\{\{.*?\}\}", '', button_text).strip()

    if not check_text:
        await update.message.reply_text("Текст кнопки не может быть пустым. Попробуйте еще раз:")
        return ASK_LINK_TEXT

    if len(check_text) > 355:
        await update.message.reply_text("Текст кнопки не должен превышать 35 символов. Попробуйте еще раз:")
        return ASK_LINK_TEXT

    # Сохраняем текст кнопки
    context.user_data['pending_link_button_text'] = button_text
    logger.info(f"Получен текст для кнопки-ссылки: {button_text}")

    # Загружаем историю с учётом совместного редактирования
    user_id_str = str(update.effective_user.id)
    story_id = context.user_data.get('story_id')

    if not story_id:
        await update.message.reply_text("Не удалось определить ID истории.")
        return ConversationHandler.END

    story_data = load_user_story(user_id_str, story_id)

    if not story_data:
        await update.message.reply_text("У вас нет доступа к этой истории или она не найдена.")
        return ConversationHandler.END

    # Обновляем данные в контексте
    context.user_data['current_story'] = copy.deepcopy(story_data)
    current_fragment_id = context.user_data.get('current_fragment_id')

    # Получаем все ID фрагментов, кроме текущего
    all_fragment_ids = sorted(story_data.get("fragments", {}).keys())
    targetable_fragment_ids = all_fragment_ids




    if not targetable_fragment_ids:
        await update.message.reply_text(
            f"Нет других фрагментов в истории, на которые можно сослаться из '{current_fragment_id}'.\n"
            "Вы можете добавить другие варианты или завершить историю."
        )
        # Возвращаемся к показу кнопок действий для текущего фрагмента
        await show_fragment_actions(update, context, current_fragment_id)
        return ADD_CONTENT

    # Используем пагинацию, если фрагментов много
    current_page = 1
    context.user_data['current_link_target_page'] = current_page

    # Генерируем клавиатуру выбора
    reply_markup = build_fragment_selection_keyboard(
        user_id_str=user_id_str,
        story_id=story_id,
        fragment_ids=targetable_fragment_ids,
        current_page=current_page,
        callback_prefix='select_link_target_' # Префикс для callback_data кнопок
    )

    # Генерируем текст-легенду для текущей страницы
    legend_text = build_legend_text(
        story_data,
        targetable_fragment_ids[(current_page - 1) * FRAGMENT_BUTTONS_PER_PAGE : current_page * FRAGMENT_BUTTONS_PER_PAGE]
    )

    total_pages = (len(targetable_fragment_ids) - 1) // FRAGMENT_BUTTONS_PER_PAGE + 1
    page_text = f"(страница {current_page})" if total_pages > 1 else ""

    await update.message.reply_text(
        f"Текст кнопки: <code>'{button_text}'</code>.\n"
        f"Теперь выберите фрагмент, на который эта кнопка будет ссылаться {page_text}:\n\n"
        f"{legend_text}",
        reply_markup=reply_markup,
        parse_mode=ParseMode.HTML
    )

    return SELECT_LINK_TARGET



async def add_content_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает нажатия кнопок в состоянии ADD_CONTENT."""
    query = update.callback_query
    await query.answer()
    action = query.data

    current_fragment_id = context.user_data.get('current_fragment_id')
    if not current_fragment_id:
        await query.edit_message_text("Ошибка: не найден текущий ID фрагмента. Попробуйте /restart")
        return ConversationHandler.END
        
    story_data = context.user_data.get('current_story')
    if not story_data or 'fragments' not in story_data:
        await query.edit_message_text("Ошибка: не найдены данные истории. Попробуйте /restart")
        return ConversationHandler.END

    logger.info(f"Action '{action}' for fragment_id '{current_fragment_id}'")

    if action == "back_to_fragment_actions":
        current_fragment_id = context.user_data.get('current_fragment_id')
        if current_fragment_id:
            await show_fragment_actions(update, context, current_fragment_id)
        else:
            await query.edit_message_text("Ошибка: не найден фрагмент.")
        return ADD_CONTENT

    elif action == 'continue_linear':
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_fragment_actions")],
            [InlineKeyboardButton("ℹ Помощь", callback_data="linkhelp")]
        ])
        await query.edit_message_text(
            "Введите текст для кнопки ведущей от текущего фрагмента к следующему. (например, \"Далее\", \"Осмотреться\", \"Встать\").\n\n"
            "<i>Для более сложных историй продвинутого уровня вы так же можете задать тут автоматического перехода по таймеру вместо кнопки с текстом. "
            "Или указать любые меняющиеся по ходу истории атрибуты. Подробнее по кнопке Помощи ниже, либо по кнопке Обучения из главного меню.</i>\n\n",            
            parse_mode='HTML',
            reply_markup=keyboard
        )
        return ASK_CONTINUE_TEXT

    elif action == 'add_branch':
        context.user_data['pending_branch_action'] = 'create_new_custom_branch'
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_fragment_actions")]
        ])
        await query.edit_message_text(
            f"Вы создаёте новую развилку из `{current_fragment_id}`.\n\n"
            f"Введите уникальное имя для новой ветки, оно будет использоваться внутри кода и в навигации при редактировании, пользователи нигде не будут его видеть.\n"
            f"Используйте латиницу, кириллицу и цифры. Нижние подчёркивания и пробелы недоступны. Максимальная длина - 15 символов \n(например: `GoLeft`, `ExploreCave`, `Развилка1`, `НачПутеш`)",
            parse_mode='Markdown',
            reply_markup=keyboard
        )
        return ASK_NEW_BRANCH_NAME



    # НОВЫЙ БЛОК: Обработка кнопки "Связать с прошлым"
    elif action == 'link_to_previous':
        keyboard = [
            [InlineKeyboardButton("ℹ Помощь", callback_data="linkhelp")],
            [InlineKeyboardButton("🔙 Назад", callback_data='link_cancel')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            "Введите текст для кнопки отсылающей к другому фрагменту \(например, 'Вернуться назад', 'Перейти к главе 1' и тд\):\n\n"
            "_Вы так же можете задать различные атрибуты в двойных фигурных скобках\\. Подробнее по кнопке ниже_",
            parse_mode="MarkdownV2",
            reply_markup=reply_markup
        )
        context.user_data['pending_action'] = 'link_to_previous'  # Запомним действие
        return ASK_LINK_TEXT  # Переходим в состояние запроса текста



    elif action == 'delete_branch_wip':
        await query.message.reply_text("Функция удаления развилок пока не реализована.")
        # Нужно будет показать кнопки снова или обновить сообщение
        # Пока просто выводим сообщение и остаемся в ADD_CONTENT (пользователь может нажать другую кнопку)
        return ADD_CONTENT

    elif action == 'finish_story':
        # Завершаем создание истории
        return await finish_story_creation(update, context)

    elif action.startswith('goto_'):
        target_fragment_id = action.split('_', 1)[1]
        story_id = context.user_data.get("story_id")
        story_data = context.user_data['current_story']
        fragment_data = story_data.get("fragments", {}).get(target_fragment_id)

        context.user_data['current_fragment_id'] = target_fragment_id  # Установим на всякий случай

        if fragment_data is None:
            # Если фрагмент ещё не создан — просим ввести контент
            await query.edit_message_text(
                f"Переход к созданию фрагмента '{target_fragment_id}'.\n"
                "Отправьте контент (текст, фото, видео и т.д.)."
            )
            context.user_data['is_editing_fragment'] = False
            return ADD_CONTENT

        # Если фрагмент уже существует — редактируем его
        context.user_data[EDIT_FRAGMENT_DATA] = {
            'story_id': story_id,
            'fragment_id': target_fragment_id
        }

        current_text = fragment_data.get("text", "*Нет текста*")
        text_to_send = clean_html_for_telegram(current_text)        
        current_media = fragment_data.get("media", [])
        media_desc = ""
        if current_media:

            media_counts = defaultdict(int)
            for item in current_media:
                media_counts[item.get("type", "unknown")] += 1
            media_desc = ", ".join([f"{count} {m_type}" for m_type, count in media_counts.items()])
            media_desc = f"\nМедиа: [{media_desc}]"

        # Клавиатура для текущего фрагмента
        user_id_str = str(update.effective_user.id)

        reply_markup = build_fragment_action_keyboard(
            fragment_id=target_fragment_id,
            story_data=story_data,
            user_id_str=user_id_str,
            story_id=story_id
        )

        await query.edit_message_text(
            f"Рдактирование фгмента: <code>{target_fragment_id}</code>\n"
            f"Текущий текст: \n<pre>{text_to_send or '*Нет текста*'}</pre>{media_desc}\n\n"
            f"➡️ <b>Отправьте новый текст и/или медиа (фото, видео, gif, аудио) для этого фрагмента.</b>\n"
            f"Новый контент полностью заменит старый.",           
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML
        )

        context.user_data['is_editing_fragment'] = True
        return ADD_CONTENT

    elif action.startswith('edit_story_'):
        try:
            _, _, user_id_str, story_id = action.split('_', 3)
            logger.info(f"Initial edit_story_ callback. User: {user_id_str}, Story: {story_id}")

            if str(update.effective_user.id) != user_id_str:
                await query.edit_message_text("Вы не можете редактировать эту историю.")
                return None

            story_data = load_user_story(user_id_str, story_id)

            if not story_data:
                await query.edit_message_text("История не найдена.")
                return None

            # Здесь мы всегда начинаем с первой страницы
            current_page = 1
            fragment_ids = sorted(story_data.get("fragments", {}).keys())
            total_fragments = len(fragment_ids)

            if total_fragments == 0:
                # Если фрагментов нет, отправляем сообщение без схемы и клавиатуры фрагментов
                 await query.edit_message_text(
                    f"История '{story_data.get('title', story_id)}' пока не содержит фрагментов. "
                    f"Вы можете добавить первый фрагмент вручную (если такая функция есть) или создать новый сюжет."
                 )
                 # Можно добавить кнопку "Назад" или "Создать первый фрагмент"
                 # Пример кнопки назад:
                 # back_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="main_menu_from_view")]])
                 # await query.edit_message_reply_markup(reply_markup=back_keyboard)
                 return None # Или другое состояние, если есть меню редактирования истории

            # --- Логика генерации схемы и отправки сообщения (оставляем как есть) ---
            # Схема генерируется для всей истории, не для страницы
            # --- Решаем: генерировать карту или нет ---

            context.user_data['current_fragment_page'] = current_page            
            legend_text = build_legend_text(story_data, fragment_ids[(current_page-1)*FRAGMENT_BUTTONS_PER_PAGE: current_page*FRAGMENT_BUTTONS_PER_PAGE])
            legend_too_long = len(legend_text) > 800
            reply_markup = build_fragment_keyboard(user_id_str, story_id, fragment_ids, current_page, story_data, legend_too_long)            
            logger.info(f"legend_text {legend_text}.")             
            if total_fragments <= 15 and len(legend_text) <= 800:
                await query.edit_message_text("Создаю схему истории, подождите...")
                image_path = generate_story_map(story_id, story_data)

                if image_path:
                    try:
                        escaped_title = html.escape(story_data.get('title', story_id))
                        with open(image_path, 'rb') as photo_file:
                            try:
                                
                                sent_message = await query.message.reply_photo(
                                    photo=photo_file,
                                    caption = (
                                        f"Схема истории \"{escaped_title}\".\n"
                                        f"id истории: <code>{story_id}</code>.\n"  
                                        f"<i>(Вы можете скопировать id истории и отправить его другим людям. Им будет достаточно просто отправить этот id боту и ваша история тут же запустится)</i>\n\n"                                                                               
                                        f"Выберите фрагмент для редактирования:\n\n"
                                        f"{legend_text}"
                                    ),
                                    reply_markup=reply_markup,
                                    parse_mode=ParseMode.HTML
                                )

                                await query.delete_message()
                            except BadRequest:
                                photo_file.seek(0)
                                await query.message.reply_document(
                                    document=photo_file,
                                    caption = (
                                        f"Схема истории \"{escaped_title}\".\n"
                                        f"id истории: <code>{story_id}</code>.\n"  
                                        f"<i>(Вы можете скопировать id истории и отправить его другим людям. Им будет достаточно просто отправить этот id боту и ваша история тут же запустится)</i>\n\n"                                                                               
                                        f"Выберите фрагмент для редактирования:\n\n"
                                        f"{legend_text}"
                                    ),
                                    reply_markup=reply_markup,
                                    parse_mode=ParseMode.HTML
                                )
                                await query.delete_message()

                    finally:
                        # Гарантированно удаляем временный файл схемы
                        if os.path.exists(image_path):
                            os.remove(image_path)
                            logger.info(f"Временный файл карты {image_path} удален.")

                else:
                    await query.edit_message_text("Ошибка при создании схемы.", reply_markup=reply_markup)

            else:
                escaped_title = html.escape(story_data.get('title', story_id))
                # Если больше 20 — просто показать фрагменты и кнопку "посмотреть карту"
                await query.edit_message_text(
                    f"Редактирование \"{escaped_title}\".\n"
                    f"id истории: <code>{story_id}</code>.\n"  
                    f"<i>(Вы жете скопировать id истории и отправить его другим людям. Им будет достаточно просто отправить этот id боту и ваша история тут же запустится)</i>\n\n"                                                                               
                    f"Выберите фрагмент для редактирования или сгенерируйте карту истории по нажатию кнопки:\n\n"
                    f"{legend_text}",
                    reply_markup=reply_markup,
                    parse_mode=ParseMode.HTML
                )





            # Сохраняем данные в user_data, включая текущую страницу
            context.user_data['story_id'] = story_id
            context.user_data['user_id_str'] = user_id_str
            context.user_data['current_story'] = story_data # Возможно, не нужно сохранять всю story_data, только ID
            context.user_data['current_fragment_page'] = current_page # Сохраняем текущую страницу

            # Переходим в состояние ожидания выбора фрагмента или пагинации
            return EDIT_STORY_MAP



        except Exception as e:
            logger.exception("Ошибка при обработке редактирования истории:")
            await query.edit_message_text("Произошла ошибка при редактировании истории.")
            return None
    elif action == 'noop': # Просто для разделителя
        pass # Ничего не делаем

    else:
        await query.edit_message_text("Неизвестное действие.")
        return ADD_CONTENT







def parse_effects_from_text(button_text: str) -> tuple[str, list[dict], list[str]]:
    clean_text = button_text
    effects = []
    errors = []
    
    effect_tag_pattern = re.compile(r"\{\{(.*?)\}\}")

    for match in effect_tag_pattern.finditer(button_text):
        full_tag = match.group(0)
        content = match.group(1).strip()
        logger.info(f"full_tag {full_tag}.") 
        items = smart_split(content)
        for item in items:
            if ':' not in item:
                phrase = item.strip()
                value_part = "1"
            else:
                phrase, value_part = map(str.strip, item.split(':', 1))

            # Проверка на распределение в диапазоне с модификаторами
            dist_pattern = re.fullmatch(
                r"([+\-<>=]?)"                              # символ перед скобками или диапазоном (например +, -, <)
                r"(\(?-?\d+\s*-\s*-?\d+\)?)"               # диапазон, возможно в скобках
                r"(?:\[(.*?)\])?"                          # опциональные модификаторы
                r"(?:\s*\((hide)\))?",                     # опциональный hide
                value_part,
                re.IGNORECASE
            )

            if dist_pattern:
                symbol = dist_pattern.group(1)
                raw_range = dist_pattern.group(2).replace(' ', '')
                number_range = f"{symbol}{raw_range}" if symbol else raw_range
                modifiers_str = dist_pattern.group(3)
                hide = dist_pattern.group(4) is not None

                modifiers = []
                if modifiers_str:
                    for mod in modifiers_str.split(','):
                        mod = mod.strip()
                        if ':' not in mod:
                            errors.append(f"Ошибка в модификаторе '{mod}' внутри {full_tag}: отсутствует ':'")
                            continue
                        left, right = map(str.strip, mod.split(':', 1))

                        if not re.fullmatch(r"-?\d+", left):
                            errors.append(f"Ошибка в модификаторе '{mod}' внутри {full_tag}: некорректное значение '{left}' до ':' (должно быть целое число)")
                            continue

                        if not re.fullmatch(r"[+-]?\d+%", right):
                            errors.append(f"Ошибка в модификаторе '{mod}' внутри {full_tag}: некорректный процент '{right}' после ':' (ожидается например '+20%')")
                            continue

                        value = int(left)
                        prob = int(right.replace('%', ''))
                        modifiers.append({"value": value, "prob": prob})

                effects.append({
                    "stat": phrase.lower(),
                    "value": number_range,
                    "modifiers": modifiers,
                    "hide": hide
                })
                continue

            # Стандартный формат: +2, -1 (hide), 5-10 и т.п.
            value_match = re.fullmatch(
                r"([+\-<>=]?)\s*(-?\d+(?:\s*-\s*-?\d+)?)(?:\s*\((hide)\))?",
                value_part,
                re.IGNORECASE
            )

            if value_match:
                symbol = value_match.group(1)
                number = value_match.group(2).replace(' ', '')
                hide = value_match.group(3) is not None

                effects.append({
                    "stat": phrase.lower(),
                    "value": f"{symbol}{number}",
                    "hide": hide
                })
            else:
                errors.append(f"Ошибка в теге {full_tag}: неверный формат значения '{value_part}'.")

        clean_text = clean_text.replace(full_tag, '').strip()

    return clean_text, effects, errors






def describe_effects_from_button_text(button_text: str) -> List[str]:
    logger.info(f"button_text {button_text}.")        
    effects = []
    block_pattern = re.compile(r"\{\{(.*?)\}\}", re.DOTALL)

    for block_match in re.finditer(block_pattern, button_text):
        content = block_match.group(1)

        effect_pattern = re.compile(
            r"(?P<stat>\w+)\s*:\s*"
            r"(?P<value>[+\-<>=]?\(?-?\d+\s*-\s*-?\d+\)?|[+\-<>=]?\s*\d+)"  # диапазон или число
            r"(?:\s*\[(?P<probs>[^\]]+)\])?"
            r"(?:\s*\((?P<hide>hide)\))?",
            re.IGNORECASE
        )

        for match in re.finditer(effect_pattern, content):
            stat = match.group('stat')
            value_str = match.group('value').replace(" ", "")
            prob_dist_str = match.group('probs')
            hide = bool(match.group('hide'))

            logger.info(f"stat {stat}, value_str {value_str}, prob_dist_str {prob_dist_str}, hide {hide}")
            desc = ""

            # Проверка на диапазон — с или без скобок, но обязательно с тире внутри
            range_match = re.fullmatch(r"([+\-<>=]?)(\(?-?\d+-\-?\d+\)?)", value_str)
            if range_match:
                symbol, raw_range = range_match.groups()

                if raw_range.startswith("(") and raw_range.endswith(")") and symbol in ('+', '-'):
                    desc = f"Значение ({stat.lower()}) изменится на диапазон {symbol}{raw_range}"
                else:
                    desc = f"Значение ({stat.lower()}) задано на диапазон {symbol}{raw_range}"

                # распределение вероятностей
                if prob_dist_str:
                    prob_pairs = []
                    for p_pair in re.split(r'\s*,\s*', prob_dist_str):
                        prob_match = re.fullmatch(r"(-?\d+)\s*:\s*([+-]?\d+%?)", p_pair)
                        if prob_match:
                            val, prob = prob_match.groups()
                            if not prob.endswith('%'):
                                prob += '%'
                            prob_pairs.append(f"{val}:{prob}")
                    if prob_pairs:
                        desc += f" с распределением вероятностей [{', '.join(prob_pairs)}]"

            elif re.fullmatch(r"[+\-]\d+", value_str):
                desc = f"Значение ({stat.lower()}) изменится на {value_str}"

            elif re.fullmatch(r"[<>=]\d+", value_str):
                desc = f"Проверка ({stat.lower()}) на значение {value_str}"

            elif re.fullmatch(r"\d+", value_str):
                desc = f"Значение ({stat.lower()}) задано на {value_str}"

            else:
                continue

            if hide:
                desc += ". Не отображается пользователю"

            effects.append(desc)

    return effects

def escape_markdown(text: str) -> str:
    """
    Экранирует специальные символы Markdown для безопасного отображения текста.
    Подходит для Markdown и MarkdownV2.
    """
    escape_chars = r"_`#|\\"
    return re.sub(f"([{re.escape(escape_chars)}])", r"\\\1", text)

async def ask_continue_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Получает текст для кнопки линейного перехода и создает ID следующего фрагмента."""
    button_text = update.message.text
    
    check_text = re.sub(r"\{\{.*?\}\}", '', button_text).strip()

    if not check_text:
        await update.message.reply_text("Текст кнопки не может быть пустым. Попробуйте еще раз:")
        return ASK_CONTINUE_TEXT
    # Проверка длины текста кнопки
    if len(check_text) > 355:
        await update.message.reply_text("Текст кнопки не должен превышать 35 символов. Попробуйте еще раз:")
        return ASK_CONTINUE_TEXT  # Остаемся в том же состоянии

    
    # Проверка состояния (может быть не нужна, если ConversationHandler строгий)
    # if not context.user_data.get('story_id'): # Убрал pending_action проверку, т.к. мы здесь только после continue_linear
    #     await update.message.reply_text("Ошибка состояния (нет story_id). Попробуйте перезапустить /restart.")
    #     return ConversationHandler.END

    current_id = context.user_data.get('current_fragment_id')
    story_data = context.user_data.get('current_story')

    if not current_id or not story_data:
        await update.message.reply_text("Критическая ошибка: отсутствует current_fragment_id или story_data. Попробуйте /restart.")
        return ConversationHandler.END

    # Используем новую функцию для создания узла и возможного сдвига
    # Парсим текст кнопки и эффекты
    clean_button_text, effects, errors = parse_effects_from_text(button_text)

    if errors:
        await update.message.reply_text(
            "Обнаружены ошибки в тегах эффектов:\n\n" + "\n".join(errors) + "\n\nПожалуйста, исправьте их и отправьте текст снова."
        )
        return ASK_CONTINUE_TEXT

    # Создаем новый узел
    new_active_fragment_id = create_linear_continuation_node(
        story_data, current_id, clean_button_text, effects=effects
    )

    if not new_active_fragment_id:
        await update.message.reply_text("Не удалось создать следующий фрагмент. Пожалуйста, попробуйте снова или обратитесь к администратору.")
        # Вернуть пользователя к клавиатуре действий для current_id
        # reply_markup = build_fragment_action_keyboard(...)
        # await update.message.reply_text(f"Действия для фрагмента {current_id}:", reply_markup=reply_markup)
        return ADD_CONTENT


    # Обновляем current_fragment_id на тот, который пользователь будет редактировать
    context.user_data['current_fragment_id'] = new_active_fragment_id
    
    # context.user_data.pop('pending_action', None) # Эта логика была специфична для числовых ID и выбора индекса
    save_current_story_from_context(context) # Убедитесь, что эта функция сохраняет изменения

    # Получаем текст описания эффектов для отображения пользователю
    extra_descriptions = describe_effects_from_button_text(button_text)
    # Проверка: является ли button_text числом (целым или дробным), без посторонних символов
    escaped_button_text = escape_markdown(clean_button_text)
    escaped_fragment_id = escape_markdown(str(new_active_fragment_id))
    escaped_extra_descriptions = [escape_markdown(desc) for desc in extra_descriptions]

    if re.fullmatch(r"\d+(\.\d+)?", button_text):
        message = (
            f"Автоматический переход в {escaped_button_text} сек. ведущий на фрагмент {escaped_fragment_id} создана.\n\n"
        )
    else:
        message = (
            f"Кнопка \"{escaped_button_text}\" ведущая на фрагмент {escaped_fragment_id} создана.\n\n"
        )

    if extra_descriptions:
        message += "\n".join(escaped_extra_descriptions) + "\n\n"

    message += (
        f"_Теперь отправьте контент (текст или фото, gif, музыку, видео) для нового фрагмента_ {escaped_fragment_id}.\n"
        f"_Текст поддерживает всю разметку телеграм._"
    )
    logger.info(f"message {message} ")
    await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
    return ADD_CONTENT



async def ask_branch_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Получает текст для кнопки новой развилки и создает ID для ветки."""
    raw_button_text = update.message.text
    
    check_text = re.sub(r"\{\{.*?\}\}", '', raw_button_text).strip()
    if not check_text:
        await update.message.reply_text("Текст кнопки не может быть пустым. Попробуйте еще раз:")
        return ASK_LINK_TEXT
    if len(check_text) > 355:  # Увеличено ограничение для текста с эффектами
        await update.message.reply_text("Текст кнопки не должен превышать 35 символов. Попробуйте еще раз:")
        return ASK_BRANCH_TEXT


    # --- Новый участок: разбираем эффекты из текста кнопки ---

    button_text, effects, errors = parse_effects_from_text(raw_button_text)

    if errors:
        await update.message.reply_text(
            "Обнаружены ошибки в тегах эффектов:\n\n" + "\n".join(errors) + "\n\nПожалуйста, исправьте их и отправьте текст снова."
        )
        return ASK_BRANCH_TEXT


    current_fragment_id = context.user_data.get('current_fragment_id')
    story_data = context.user_data.get('current_story')
    target_branch_name = context.user_data.get('target_branch_name')
    target_branch_index = context.user_data.get('target_branch_index')

    if not all([current_fragment_id, story_data, target_branch_name, target_branch_index is not None]):
        await update.message.reply_text("Ошибка в данных для создания ветки. Попробуйте /restart.")
        # Очистка временных данных
        context.user_data.pop('target_branch_name', None)
        context.user_data.pop('target_branch_index', None)
        context.user_data.pop('pending_branch_action', None)
        return ADD_CONTENT # или ConversationHandler.END

    branch_fragment_id = construct_id(target_branch_name, target_branch_index)

    # Убедимся, что фрагмент, на который ссылаемся, существует или будет создан
    if branch_fragment_id not in story_data['fragments']:
        story_data['fragments'][branch_fragment_id] = {
            "text": "",
            "media": [],
            "choices": []  # ← Изменено: был dict — стал list
        }
        logger.info(f"Создан новый пустой фрагмент: {branch_fragment_id}")

    # Добавляем выбор (ветку) к текущему фрагменту
    if 'choices' not in story_data['fragments'][current_fragment_id]:
        story_data['fragments'][current_fragment_id].setdefault('choices', []).append(
            {"text": button_text, "target": branch_fragment_id}
        )
    choices = story_data['fragments'][current_fragment_id].setdefault('choices', [])
    # --- Новый участок: обновление существующего выбора или добавление нового с effects ---
    for choice in choices:
        if choice['text'] == button_text and choice['target'] == branch_fragment_id:
            # Возможно, стоит обновить только эффекты
            if effects:
                choice['effects'] = effects
            break
    else:
        # Добавим новую ветку, если такой пары text+target ещё не было
        new_choice = {"text": button_text, "target": branch_fragment_id}
        if effects:
            new_choice["effects"] = effects
        choices.append(new_choice)





    logger.info(f"Создана ветка: '{current_fragment_id}' --({button_text})--> '{branch_fragment_id}'")
    save_current_story_from_context(context)

    # Очистка временных данных
    context.user_data.pop('target_branch_name', None)
    context.user_data.pop('target_branch_index', None)
    context.user_data.pop('pending_branch_action', None)

    await update.message.reply_text(f"Ветка '{button_text}' -> `{branch_fragment_id}` добавлена.")

    # Используем build_fragment_action_keyboard для генерации клавиатуры
    # (Убедитесь, что эта функция существует и правильно передает параметры)
    user_id_str = str(update.effective_user.id)
    story_id = context.user_data.get('story_id', 'unknown_story_id') # Убедитесь, что story_id есть

    # Пример вызова, адаптируйте под свою функцию build_fragment_action_keyboard
    reply_markup = build_fragment_action_keyboard( 
        fragment_id=current_fragment_id, 
        story_data=story_data, 
        user_id_str=user_id_str, 
        story_id=story_id 
    )


    current_fragment = story_data['fragments'][current_fragment_id]
    current_choices = current_fragment.get("choices", [])

    text_lines = [
        f"✅ Вы успешно создали новую ветку.\n\nТекущий фрагмент: <code>{current_fragment_id}</code>.\n"
        "Выберите следующее действие или нажмите на созданную ветку, чтобы начать её заполнять:\n"
    ]

    # Медиа
    media = current_fragment.get("media", [])
    if media:
        types_count = {}
        for item in media:
            media_type = item.get("type", "unknown")
            types_count[media_type] = types_count.get(media_type, 0) + 1
        media_lines = [f"{media_type}: {count}" for media_type, count in types_count.items()]
        text_lines.append("📎 Медиа: " + ", ".join(media_lines))

    # Текст фрагмента
    text = current_fragment.get("text", "").strip()
    if text:
        escaped_text = html.escape(text)
        text_lines.append(f"📝 Текст:\n✦ ━━━━━━━━━━\n{escaped_text}\n✦ ━━━━━━━━━━")

    # Добавим информацию об эффектах, если они есть
    for choice in current_choices:
        if "effects" in choice:
            effect_lines = []
            for effect in choice["effects"]:
                stat = html.escape(str(effect.get("stat", "неизвестно")))
                if effect.get("hide"):
                    stat = f"{stat}(Скрытый)"
                raw_value = str(effect.get("value", "?"))
                value = html.escape(raw_value)

                # Определяем тип эффекта по raw_value
                if re.match(r'^[+-]?\(\-?\d+\-+\-?\d+\)$', raw_value):  # например +(5-9), -(3--8)
                    verb = "меняет атрибут"
                elif re.match(r'^[+-]\d+$', raw_value):  # например +5, -3
                    verb = "меняет атрибут"
                elif re.match(r'^[<>=]', raw_value):  # сравнение
                    verb = "проверка атрибута"
                elif re.match(r'^-?\d+\-+\-?\d+$', raw_value):  # диапазон: 3-5, -2--1, 3--8
                    verb = "задаёт атрибут"
                elif re.match(r'^-?\d+$', raw_value):  # просто число: 7, -1
                    verb = "задаёт атрибут"
                else:
                    verb = "неизвестный эффект"

                effect_lines.append(f"{stat}: {value} ({verb})")

            if effect_lines:
                effects_text = ", ".join(effect_lines)
                text_lines.append(
                    f"\n\n🔸 Выбор <b>«{html.escape(choice['text'])}»</b> ведущий на фрагмент <code>{html.escape(choice['target'])}</code> имеет эффект: {effects_text}"
                )


    # Финальное сообщение
    text_lines.append(
        "\n<i>Если сейчас вы отправите боту новый текст или медиа-контент, он заменит прежний в этом слайде.</i>\n"
        "Либо выберите действие:"
    )

    # Сообщение
    final_text = "\n".join(text_lines)

    await update.message.reply_text(
        final_text,
        reply_markup=reply_markup,
        parse_mode=ParseMode.HTML
    )

    return ADD_CONTENT

# Вспомогательные функции для работы с новыми ID
def get_branch_info(fragment_id: str) -> tuple[str, int] | tuple[None, None]:
    """Разбирает ID на имя ветки и индекс (например, "main_1" -> ("main", 1))."""
    match = re.fullmatch(r'(.+?)_([0-9]+)', fragment_id)
    if match:
        return match.group(1), int(match.group(2))
    # Для обратной совместимости или корневых элементов без индекса (если такие планируются)
    # if re.fullmatch(r'[a-zA-Z0-9_]+', fragment_id) and '_' not in fragment_id:
    #     return fragment_id, 0 # Например, "root" -> ("root", 0)
    logger.error(f"Could not parse fragment_id: {fragment_id}")
    return None, None # Или возбуждать исключение


def construct_id(branch_name: str, index: int) -> str:
    """Собирает ID из имени ветки и индекса (например, ("main", 1) -> "main_1")."""
    return f"{branch_name}_{index}"

def get_next_sequential_id_in_branch(fragment_id: str) -> str | None:
    """Возвращает следующий ID в той же ветке (main_1 -> main_2)."""
    branch_name, index = get_branch_info(fragment_id)
    if branch_name is not None and index is not None:
        return construct_id(branch_name, index + 1)
    return None

def get_all_branch_base_names(story_data: dict) -> set[str]:
    """Возвращает множество всех базовых имен веток в истории (например, {"main", "left"})."""
    names = set()
    if story_data and 'fragments' in story_data:
        for fid in story_data['fragments']:
            branch_name, _ = get_branch_info(fid)
            if branch_name:
                names.add(branch_name)
    return names





# Заменит insert_shifted_fragment
def create_linear_continuation_node(story_data: dict, base_id: str, button_text: str, effects: dict | None = None) -> str | None:
    """
    Создает узел для линейного продолжения.
    Если у base_id уже есть choices, они переносятся на новый узел.
    Если следующий по порядку ID в ветке занят, он и последующие сдвигаются.
    Возвращает ID созданного узла, который будет редактироваться.
    """
    fragments = story_data.get('fragments')
    if not fragments or base_id not in fragments:
        logger.error(f"Base_id {base_id} not found in fragments for linear continuation.")
        return None

    base_branch_name, base_index = get_branch_info(base_id)
    if base_branch_name is None or base_index is None:
        logger.error(f"Could not parse base_id {base_id} for linear continuation.")
        return None

    target_node_id = construct_id(base_branch_name, base_index + 1)
    
    ids_to_update_in_choices = {} # old_id -> new_id, для обновления ссылок
    # --- Новый участок: если target ID свободен, просто создаем его ---
    if target_node_id not in fragments:
        old_choices = fragments[base_id].get('choices', []).copy()

        new_choice = {"text": button_text, "target": target_node_id}
        if effects:
            new_choice["effects"] = effects

        new_choices = [new_choice] + old_choices
        fragments[base_id]['choices'] = new_choices

        fragments[target_node_id] = {
            "text": "",
            "media": [],
            "choices": []
        }
        logger.info(f"Linear continuation (simple insert): '{base_id}' --({button_text})--> '{target_node_id}' with old choices preserved.")
        return target_node_id
    # Проверяем, нужно ли сдвигать фрагменты в той же ветке
    if target_node_id in fragments:
        # Собираем все фрагменты текущей ветки, начиная с индекса target_node_id
        # и сортируем их по индексу в ОБРАТНОМ порядке для корректного переименования
        fragments_in_branch_to_shift = sorted(
            [
                fid for fid in fragments 
                if get_branch_info(fid)[0] == base_branch_name and \
                   get_branch_info(fid)[1] is not None and \
                   get_branch_info(fid)[1] >= get_branch_info(target_node_id)[1]
            ],
            key=lambda x: get_branch_info(x)[1],
            reverse=True
        )

        for old_fid in fragments_in_branch_to_shift:
            branch, old_idx = get_branch_info(old_fid)
            if branch is None or old_idx is None: continue # Пропускаем некорректные ID

            new_fid = construct_id(branch, old_idx + 1)
            ids_to_update_in_choices[old_fid] = new_fid
            if old_fid != new_fid : # Избегаем удаления и добавления того же ключа если не было сдвига (маловероятно здесь)
                 fragments[new_fid] = fragments.pop(old_fid)


    # Обновить все choices во всей истории, если были сдвиги
    if ids_to_update_in_choices:
        for frag_id_iter, frag_data_iter in fragments.items():
            if 'choices' in frag_data_iter:
                updated_choices = []
                changed = False
                for choice in frag_data_iter['choices']:
                    updated_target = ids_to_update_in_choices.get(choice['target'], choice['target'])
                    if updated_target != choice['target']:
                        changed = True
                    new_choice = {
                        "text": choice['text'],
                        "target": updated_target
                    }
                    if 'effects' in choice:
                        new_choice['effects'] = choice['effects']
                    updated_choices.append(new_choice)
                if changed:
                    frag_data_iter['choices'] = updated_choices

    old_choices_of_base_id = fragments[base_id].get('choices', []).copy()

    fragments[base_id]['choices'] = [{"text": button_text, "target": target_node_id}]

    preserved_choices = []
    for choice in old_choices_of_base_id:
        new_choice = {
            "text": choice['text'],
            "target": choice['target']
        }
        if 'effects' in choice:
            new_choice['effects'] = choice['effects']
        preserved_choices.append(new_choice)

    fragments[target_node_id] = {
        "text": "",
        "media": [],
        "choices": preserved_choices
    }
    logger.info(f"Linear continuation: '{base_id}' --({button_text})--> '{target_node_id}'. Old choices moved to '{target_node_id}'.")
    return target_node_id

async def ask_new_branch_name_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Получает имя для новой пользовательской ветки."""
    new_branch_name_input = update.message.text.strip()
    story_data = context.user_data.get('current_story')

    # Разрешены латиница, кириллица и цифры; пробелы и "_" — запрещены
    if not re.fullmatch(r'[a-zA-Zа-яА-ЯёЁ0-9]+', new_branch_name_input):
        await update.message.reply_text(
            "Некорректное имя ветки. Используйте только латинские или кириллические буквы и цифры.\n"
            "Пробелы и символ подчёркивания (_) не допускаются.\n"
            "Пожалуйста, введите имя снова:"
        )
        return ASK_NEW_BRANCH_NAME

    # Ограничение на длину
    if len(new_branch_name_input) > 15:
        await update.message.reply_text(
            f"Имя ветки слишком длинное \n \\(текущая длина: {len(new_branch_name_input)}\\)\\.\n\n"
            "Это связано с тем, что Telegram весьма сильно ограничивает количество данных, которые можно передавать в кнопках\\.\n\n"
            "Пожалуйста, используйте не более 15 символов\\. Никто кроме вас имя веток видеть не будет, поэтому просто используйте понятные вам сокращения\\.\n"
            "Введите имя снова:",
            parse_mode="MarkdownV2"
        )
        return ASK_NEW_BRANCH_NAME

    all_existing_bases = get_all_branch_base_names(story_data)
    if new_branch_name_input in all_existing_bases:
        await update.message.reply_text(f"Имя ветки '{new_branch_name_input}' уже используется.\n"
                                        "Пожалуйста, введите другое имя:")
        return ASK_NEW_BRANCH_NAME

    context.user_data['target_branch_name'] = new_branch_name_input
    context.user_data['target_branch_index'] = 1

    await update.message.reply_text(
        f"Отлично\\! Теперь введите текст для кнопки, которая будет вести к началу ветки `{new_branch_name_input}_1`\n"
        f'Например "Пойти направо", "Сесть", "Согласиться" и тд\n\n'
        f"_Вы так же можете задать различные атрибуты в двойных фигурных скобках\\. Подробнее по кнопке ниже_",
        parse_mode="MarkdownV2",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("ℹ Помощь", callback_data="linkhelp")]
        ])
    )

    return ASK_BRANCH_TEXT













# Обработчик для начала процесса изменения порядка
async def reorder_choices_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> str:
    query = update.callback_query
    await query.answer()

    try:
        fragment_id = query.data.split(REORDER_CHOICES_START_PREFIX)[1]
    except IndexError:
        logger.error(f"Ошибка извлечения fragment_id из callback_data: {query.data}")
        await query.edit_message_text("Произошла ошибка. Не удалось определить фрагмент.")
        return ConversationHandler.END

    if 'current_story' not in context.user_data:
        logger.error("current_story отсутствует в user_data при вызове reorder_choices_start")
        await query.edit_message_text("Ошибка состояния истории. Попробуйте начать заново.")
        return ConversationHandler.END

    story_data = context.user_data['current_story']
    current_fragment = story_data["fragments"].get(fragment_id)

    if not current_fragment or "choices" not in current_fragment:
        await query.edit_message_text("Ошибка: Фрагмент или его выборы не найдены.")
        await show_fragment_actions(update, context, fragment_id)
        return ADD_CONTENT

    choices = current_fragment["choices"]
    if len(choices) <= 1:
        await query.edit_message_text("Недостаточно вариантов для изменения порядка.")
        await show_fragment_actions(update, context, fragment_id)
        return ADD_CONTENT

    context.user_data['reorder_fragment_id'] = fragment_id
    # Сохраняем весь список выбора (dict), чтобы не потерять дополнительные поля вроде "effects"
    context.user_data['reorder_choices_list'] = choices.copy()

    keyboard = []
    for index, choice in enumerate(choices):
        button_text = choice.get("text", f"Выбор {index + 1}")
        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"{REORDER_CHOICE_ITEM_PREFIX}{index}")])

    keyboard.append([InlineKeyboardButton("Отмена", callback_data=REORDER_CHOICE_CANCEL)])
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text("Какую кнопку вы хотите передвинуть?", reply_markup=reply_markup)
    return REORDER_CHOICE_SELECT_ITEM


# Обработчик после выбора пользователем кнопки для перемещения
async def reorder_choice_select_position_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> str:
    query = update.callback_query
    await query.answer()
    try:
        selected_index = int(query.data.split(REORDER_CHOICE_ITEM_PREFIX)[1])
    except (IndexError, ValueError):
        logger.error(f"Ошибка извлечения selected_index из callback_data: {query.data}")
        await query.edit_message_text("Произошла ошибка выбора элемента.")
        fragment_id = context.user_data.get('reorder_fragment_id', context.user_data.get('current_fragment_id'))
        if fragment_id:
            await show_fragment_actions(update, context, fragment_id)
            return ADD_CONTENT
        return ConversationHandler.END


    choices_list = context.user_data.get('reorder_choices_list', [])
    if not choices_list or selected_index >= len(choices_list):
        await query.edit_message_text("Ошибка: Выбранный элемент не найден. Попробуйте снова.")
        fragment_id = context.user_data.get('reorder_fragment_id')
        if fragment_id:
            await show_fragment_actions(update, context, fragment_id)
            return ADD_CONTENT
        return ConversationHandler.END

    context.user_data['reorder_selected_item_index'] = selected_index
    selected_item_text = choices_list[selected_index].get("text", "без текста")

    keyboard = [
        [InlineKeyboardButton("В самый верх", callback_data=f"{REORDER_CHOICE_POSITION_PREFIX}top")],
        [InlineKeyboardButton("На один пункт выше", callback_data=f"{REORDER_CHOICE_POSITION_PREFIX}up")],
        [InlineKeyboardButton("Оставить как есть", callback_data=f"{REORDER_CHOICE_POSITION_PREFIX}asis")],
        [InlineKeyboardButton("На один пункт ниже", callback_data=f"{REORDER_CHOICE_POSITION_PREFIX}down")],
        [InlineKeyboardButton("В самый низ", callback_data=f"{REORDER_CHOICE_POSITION_PREFIX}bottom")],
        [InlineKeyboardButton("Отмена", callback_data=REORDER_CHOICE_CANCEL)]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(f"Куда вы хотите передвинуть кнопку '{selected_item_text}'?", reply_markup=reply_markup)
    return REORDER_CHOICE_SELECT_POSITION


# Обработчик для выполнения фактического изменения порядка и сохранения
async def reorder_choice_execute(update: Update, context: ContextTypes.DEFAULT_TYPE) -> str:
    query = update.callback_query
    await query.answer()
    try:
        action = query.data.split(REORDER_CHOICE_POSITION_PREFIX)[1]
    except IndexError:
        logger.error(f"Ошибка извлечения action из callback_data: {query.data}")
        await query.edit_message_text("Произошла ошибка выбора действия.")
        fragment_id = context.user_data.get('reorder_fragment_id', context.user_data.get('current_fragment_id'))
        if fragment_id:
            await show_fragment_actions(update, context, fragment_id)
            return ADD_CONTENT
        return ConversationHandler.END

    fragment_id = context.user_data.get('reorder_fragment_id')
    choices_list = list(context.user_data.get('reorder_choices_list', []))
    selected_item_original_index = context.user_data.get('reorder_selected_item_index')

    if fragment_id is None or not choices_list or selected_item_original_index is None \
            or selected_item_original_index >= len(choices_list):
        error_message = "Ошибка состояния при изменении порядка. Попробуйте снова."
        logger.error(f"{error_message} Данные: f_id={fragment_id}, c_list_empty={not choices_list}, s_idx={selected_item_original_index}")
        await query.edit_message_text(error_message)
        display_fragment_id = fragment_id or context.user_data.get('current_fragment_id')
        if display_fragment_id:
            await show_fragment_actions(update, context, display_fragment_id)
            return ADD_CONTENT
        return ConversationHandler.END

    # Выполняем перемещение элемента
    if action != "asis":
        item_to_move = choices_list.pop(selected_item_original_index)
        if action == "top":
            choices_list.insert(0, item_to_move)
        elif action == "up":
            new_insert_idx = max(0, selected_item_original_index - 1)
            choices_list.insert(new_insert_idx, item_to_move)
        elif action == "down":
            new_insert_idx = min(len(choices_list), selected_item_original_index + 1)
            choices_list.insert(new_insert_idx, item_to_move)
        elif action == "bottom":
            choices_list.append(item_to_move)

    # Обновляем данные истории — просто сохраняем новый список dict
    context.user_data['current_story']['fragments'][fragment_id]['choices'] = choices_list
    save_current_story_from_context(context)
    logger.info(f"Порядок choices для фрагмента {fragment_id} обновлен.")

    # Очищаем временные данные
    for key in ['reorder_fragment_id', 'reorder_choices_list', 'reorder_selected_item_index']:
        context.user_data.pop(key, None)

    context.user_data['current_fragment_id'] = fragment_id

    await query.edit_message_text("Порядок кнопок обновлен.")
    await show_fragment_actions(update, context, fragment_id)
    return ADD_CONTENT

# Обработчик отмены для процесса изменения порядка
async def reorder_choice_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> str:
    query = update.callback_query
    await query.answer()
    fragment_id = context.user_data.get('reorder_fragment_id', context.user_data.get('current_fragment_id'))

    # Очищаем временные данные
    for key in ['reorder_fragment_id', 'reorder_choices_list', 'reorder_selected_item_index']:
        context.user_data.pop(key, None)

    if fragment_id:
        context.user_data['current_fragment_id'] = fragment_id
        await query.edit_message_text("Изменение порядка отменено.") # Можно убрать
        await show_fragment_actions(update, context, fragment_id)
        return ADD_CONTENT
    else:
        logger.warning("reorder_choice_cancel: fragment_id не найден, не могу вернуться к фрагменту.")
        await query.edit_message_text("Отменено. Не удалось определить текущий фрагмент.")
        # Здесь можно вернуть пользователя в главное меню или завершить диалог
        # return await start(update, context) # Если у вас есть общая команда start/main_menu
        return ConversationHandler.END










#==========================================================================





#==========================================================================
#ЛОГИКА КАРТЫ



def generate_branch_colors(fragments):
    """Генерирует уникальный цвет для каждой ветки"""
    prefixes = set(frag_id.rsplit('_', 1)[0] for frag_id in fragments)
    prefix_list = sorted(prefixes)
    n = len(prefix_list)
    branch_colors = {}

    for i, prefix in enumerate(prefix_list):
        hue = i / n  # распределим цвета равномерно
        rgb = colorsys.hsv_to_rgb(hue, 0.6, 0.85)
        color_hex = '#%02x%02x%02x' % tuple(int(c * 255) for c in rgb)
        branch_colors[prefix] = color_hex

    return branch_colors

def generate_story_map(story_id: str, story_data: dict, highlight_ids: set[str] = None) -> str:
    if not isinstance(story_data, dict):
        logger.error(f"Ошибка данных для истории {story_id}: ожидался словарь, получено {type(story_data)}")
        return None

    fragments = story_data.get("fragments")
    if not isinstance(fragments, dict):
        logger.warning(f"В данных истории '{story_id}' отсутствуют фрагменты или они некорректны.")
        return None

    G = nx.MultiDiGraph()
    G.graph['graph'] = {
        'rankdir': 'LR',
        'center': 'true',
        'margin': '0.2',
        'nodesep': '0.1',
        'ranksep': '0.2',
        'ordering': 'out'
    }  # или 'TB' по умолчанию (Top-Bottom)
    node_labels = {}
    node_colors = {}
    edge_colors = {}
    edge_labels = {}
    highlight_ids = highlight_ids or set()
    MEDIA_TYPES = {"photo", "video", "animation", "audio"}
    branch_colors = generate_branch_colors(fragments)
    for fragment_id, fragment_content in fragments.items():
        if not isinstance(fragment_content, dict):
            logger.warning(f"Фрагмент {fragment_id} в истории {story_id} имеет неверный формат.")
            continue

        text = fragment_content.get("text", "").strip()
        media = fragment_content.get("media", [])
        media_count = sum(1 for m in media if m.get("type") in MEDIA_TYPES)
        media_types_present = [m.get("type") for m in media if m.get("type") in MEDIA_TYPES]

        if media_types_present:
            type_labels = {
                "photo": "Фото",
                "video": "Видео",
                "animation": "Анимация",
                "audio": "Аудио"
            }
            media_label = ", ".join(type_labels[t] for t in media_types_present)
        else:
            media_label = ""

        choices = fragment_content.get("choices", [])
        has_children = bool(choices)
        is_end_node = not has_children

        # Формируем метку узла
        if not text and not media:
            label = f"ID: {fragment_id}\n[событие пусто]"
        elif media_label:
            if text:
                label = f"ID: {fragment_id}\n{media_label}\n({text[:20] + '...' if len(text) > 20 else text})"
            else:
                label = f"ID: {fragment_id}\n{media_label}"
        else:
            short_text = text[:20] + "..." if len(text) > 20 else text
            label = f"ID: {fragment_id}\n{short_text}"

        # Помечаем как конец, если нет дочерних событий
        if is_end_node:
            label += "\n[КОНЕЦ]"

        node_labels[fragment_id] = label

        # Определяем цвет узла
        if fragment_id in highlight_ids:
            node_colors[fragment_id] = 'yellow'
        elif fragment_id == 'main_1':
            node_colors[fragment_id] = '#8cd86f'  # Основное начало истории
        elif not text and not media:
            node_colors[fragment_id] = 'lightcoral'  # Пустые события            
        elif fragment_id.endswith('_1') and has_children:
            node_colors[fragment_id] = '#ccffcc'  # Пастельно-зелёный
        elif is_end_node:
            node_colors[fragment_id] = '#689ee8'  # Конечные фрагменты
        else:
            node_colors[fragment_id] = 'skyblue'

        G.add_node(fragment_id)

    for fragment_id, fragment_content in fragments.items():
        if not isinstance(fragment_content, dict):
            continue

        choices = fragment_content.get("choices", [])
        if not isinstance(choices, list):
            logger.warning(f"Поле 'choices' в фрагменте {fragment_id} истории {story_id} имеет неверный формат.")
            continue

        for choice in choices:
            choice_text = choice.get("text")
            next_fragment_id = choice.get("target")
            if not choice_text or not next_fragment_id:
                continue
            # Интерпретируем числовые варианты выбора как "задержка X секунд"
            try:
                int_choice = int(choice_text)
                base_label = f"задержка {int_choice} секунд"
            except ValueError:
                base_label = choice_text[:40] + "..." if len(choice_text) > 40 else choice_text

            # Добавляем эффекты, если есть
            effects = choice.get("effects")
            effects_html_label = ""
            if isinstance(effects, list) and effects:
                effect_rows = ""
                has_check_symbols = False
                check_symbols = ('>', '<', '=')

                for effect in effects:
                    if not isinstance(effect, dict):
                        continue
                    stat = effect.get("stat")
                    value = effect.get("value")
                    hide = effect.get("hide", False)

                    if not stat or value is None:
                        continue

                    raw_effect_value = str(value)
                    is_check = any(symbol in raw_effect_value for symbol in check_symbols)
                    if is_check:
                        has_check_symbols = True

                    safe_stat = html.escape(stat)
                    safe_value = html.escape(raw_effect_value)
                    effect_row = f"{safe_stat}: {safe_value}"
                    if hide:
                        effect_row += " (скрытый)"

                    # Выбор цвета строки:
                    if hide:
                        row_color = "#c7c7d2"  # скрытые — общий цвет
                    elif is_check:
                        row_color = "#c5b7ff"  # проверки (не скрытые)
                    else:
                        row_color = "#d1e9ff"  # эффекты (не скрытые)

                    effect_rows += f"<TR><TD ALIGN='LEFT' BGCOLOR='{row_color}'>{effect_row}</TD></TR>"

                table_title = "Проверка" if has_check_symbols else "Эффекты"
                title_color = "#c3b5f5" if has_check_symbols else "#b5d6f5"

                effects_html_label = f"""<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0">
                <TR><TD ALIGN='CENTER' BGCOLOR='{title_color}'><B>{table_title}:</B></TD></TR>
                {effect_rows}
                </TABLE>"""


            # Объединяем метку стрелки
            if effects_html_label:
                edge_label = f"""<
            <TABLE BORDER="0" CELLBORDER="0" CELLSPACING="1">
              <TR><TD>{html.escape(base_label)}</TD></TR>
              <TR><TD>{effects_html_label}</TD></TR>
            </TABLE>
            >"""
            else:
                edge_label = base_label

            if not G.has_node(next_fragment_id):
                G.add_node(next_fragment_id)
                G.add_edge(fragment_id, next_fragment_id, label=edge_label, color='red') # Установите атрибуты напрямую
                node_labels[next_fragment_id] = f"[MISSING]\n{next_fragment_id}"
                node_colors[next_fragment_id] = 'lightcoral'

                logger.warning(
                    f"В истории '{story_id}', фрагмент '{fragment_id}' "
                    f"ссылается на несуществующий фрагмент '{next_fragment_id}' "
                    f"через выбор '{choice_text[:50]}...'."
                )
                edge_colors[(fragment_id, next_fragment_id)] = 'red'
            else:
                branch_prefix = fragment_id.rsplit('_', 1)[0]
                branch_color = branch_colors.get(branch_prefix, 'grey')
                G.add_edge(fragment_id, next_fragment_id, label=edge_label, color=branch_color) # Установите атрибуты напрямую    

            



    if not G:
        logger.warning(f"Не удалось построить граф для истории '{story_id}', нет валидных узлов/ребер.")
        return None

    # --- НОВАЯ ЧАСТЬ: Использование graphviz для рендеринга ---
    dot = gv.Digraph(comment=f'Story Map: {story_data.get("title", story_id)}')
    dot.attr(rankdir='LR', bgcolor='white', dpi='180')  # ← добавлено dpi


    # Добавляем узлы
    for node in G.nodes():
        label_text = node_labels.get(node, node)
        color = node_colors.get(node, 'skyblue')


        dot.node(str(node), # Убедимся, что ID узла - строка
                 label=label_text,
                 shape='box', # Используем форму прямоугольника
                 style='filled', # Заливка
                 fillcolor=color,
                 color='black' # Цвет рамки
                 )



    for u, v, key in G.edges(keys=True):
        edge_data = G.get_edge_data(u, v, key) # Получите данные для этого конкретного ребра
        dot.edge(str(u), str(v),
                 label=edge_data.get('label', ''), # Получите метку из данных ребра
                 color=edge_data.get('color', 'grey'), # Получите цвет из данных ребра
                 fontsize='12',
                 fontcolor='darkred')

    # Сохраняем файл
    try:
        temp_dir = tempfile.gettempdir()
        filename_base = f"story_map_{story_id}_{uuid4().hex[:8]}"
        filepath_dot = os.path.join(temp_dir, filename_base + ".dot")
        filepath_png = os.path.join(temp_dir, filename_base + ".png")

        dot.render(filepath_dot, format='png', outfile=filepath_png, cleanup=False) # Or filename=filepath_png depending on graphviz library version/docs

        logger.info(f"Карта истории '{story_id}' успешно создана: {filepath_png}")
        return filepath_png

    except Exception as e:
        logger.error(f"Ошибка при рендеринге карты истории {story_id} с помощью Graphviz: {e}", exc_info=True)
        return None


def generate_branch_map(story_id: str, story_data: dict, branch_name: str, highlight_ids: set[str] = None) -> str | None:
    """
    Генерирует карту для указанной ветки истории, включая фрагменты,
    ссылающиеся на эту ветку, и фрагменты, на которые ссылается ветка.
    """
    if not isinstance(story_data, dict):
        logger.error(f"Ошибка данных для истории {story_id}: ожидался словарь, получено {type(story_data)}")
        return None

    all_fragments_data = story_data.get("fragments")
    if not isinstance(all_fragments_data, dict):
        logger.warning(f"В данных истории '{story_id}' отсутствуют фрагменты или они некорректны.")
        return None

    # 1. Определяем основные узлы ветки
    branch_node_ids = set()
    for frag_id in all_fragments_data:
        if frag_id == branch_name or frag_id.startswith(branch_name + "_"):
            branch_node_ids.add(frag_id)

    if not branch_node_ids:
        logger.warning(f"Ветка '{branch_name}' не найдена или пуста в истории '{story_id}'.")
        # Можно вернуть пустую карту или None
        # Для примера, создадим карту только с узлами ветки, если они есть,
        # даже если нет связей. Если совсем нет узлов, то None.
        # Если хотим карту даже для одного узла без связей, то нужно адаптировать логику ниже.
        # В данном случае, если branch_node_ids пусто, то и nodes_to_render будет пусто, и граф не построится.
        # Это нормально, если мы не хотим генерировать карту для несуществующей/пустой ветки.


    # 2. Определяем все узлы для отрисовки: узлы ветки + смежные узлы
    nodes_to_render_ids = set(branch_node_ids)
    connecting_node_ids = set() # Узлы, не входящие в ветку, но связанные с ней

    for frag_id, fragment_content in all_fragments_data.items():
        if not isinstance(fragment_content, dict):
            continue
        choices = fragment_content.get("choices", [])
        if not isinstance(choices, list):
            continue

        for choice in choices:
            next_frag_id = choice.get("target")
            if not next_frag_id:
                continue
            # Связь ИЗ ветки ВО ВНЕ
            if frag_id in branch_node_ids and next_frag_id not in branch_node_ids and next_frag_id in all_fragments_data:
                nodes_to_render_ids.add(next_frag_id)
                connecting_node_ids.add(next_frag_id)
            # Связь ИЗВНЕ В ВЕТКУ
            elif frag_id not in branch_node_ids and next_frag_id in branch_node_ids and frag_id in all_fragments_data:
                nodes_to_render_ids.add(frag_id)
                connecting_node_ids.add(frag_id)

    if not nodes_to_render_ids:
         logger.warning(f"Для ветки '{branch_name}' истории '{story_id}' не найдено узлов для отображения на карте.")
         return None

    # Фильтруем данные фрагментов, оставляя только те, что будут на карте
    filtered_fragments = {fid: all_fragments_data[fid] for fid in nodes_to_render_ids if fid in all_fragments_data}

    G = nx.DiGraph()
    G.graph['graph'] = {'rankdir': 'LR', 'center': 'true', 'margin': '0.2', 'nodesep': '0.1', 'ranksep': '0.2', 'ordering': 'out'}
    node_labels = {}
    node_colors = {}
    edge_colors = {}
    edge_labels = {}
    highlight_ids = highlight_ids or set()
    MEDIA_TYPES = {"photo", "video", "animation", "audio"}
    
    # Используем цвета для всей истории, чтобы сохранить консистентность, если нужно
    # Либо можно генерировать цвета только для отображаемых веток
    branch_color_map = generate_branch_colors(all_fragments_data)


    for fragment_id, fragment_content in filtered_fragments.items():
        text = fragment_content.get("text", "").strip()
        media = fragment_content.get("media", [])
        media_types_present = [m.get("type") for m in media if m.get("type") in MEDIA_TYPES]
        media_label = ""
        if media_types_present:
            type_labels = {"photo": "Фото", "video": "Видео", "animation": "Анимация", "audio": "Аудио"}
            media_label = ", ".join(type_labels[t] for t in media_types_present)

        choices = fragment_content.get("choices", [])
        has_children_in_rendered_set = any(choice.get("target") in nodes_to_render_ids for choice in choices)
        is_end_node_for_branch_view = not has_children_in_rendered_set # Конечность в контексте видимых узлов

        label_parts = [f"ID: {fragment_id}"]
        if media_label: label_parts.append(media_label)
        if text: label_parts.append(f"({text[:20] + '...' if len(text) > 20 else text})")
        if not text and not media: label_parts.append("[пусто]")
        if is_end_node_for_branch_view: label_parts.append("[КОНЕЦ ВЕТКИ]")
        
        node_labels[fragment_id] = "\n".join(label_parts)

        # Определение цвета узла
        current_node_branch_prefix = fragment_id.rsplit('_', 1)[0] if '_' in fragment_id else fragment_id
        is_main_branch_node = fragment_id in branch_node_ids

        if fragment_id in highlight_ids:
            node_colors[fragment_id] = 'yellow'
        elif not text and not media:
             node_colors[fragment_id] = 'lightcoral' # Пустые события
        elif is_main_branch_node:
            if fragment_id == f"{branch_name}_1" or (branch_name == "main" and fragment_id == "main_1"): # Начало главной ветки
                 node_colors[fragment_id] = '#8cd86f' # Ярко-зеленый (старт ветки)
            elif is_end_node_for_branch_view:
                 node_colors[fragment_id] = '#689ee8' # Синий (конец ветки)
            else:
                 node_colors[fragment_id] = '#a3d8f4' # Светло-голубой (внутри ветки)
        else: # Соседние узлы (не из основной отображаемой ветки)
            if is_end_node_for_branch_view : # Если это конец пути на карте ветки
                node_colors[fragment_id] = 'lightgrey' # Светло-серый для концов из других веток
            else:
                node_colors[fragment_id] = 'whitesmoke' # Очень светлый для транзитных узлов из других веток


        G.add_node(fragment_id)

    for fragment_id, fragment_content in filtered_fragments.items():
        choices = fragment_content.get("choices", [])
        for choice in choices:
            choice_text = choice.get("text", "")
            next_fragment_id = choice.get("target")
            if not next_fragment_id or next_fragment_id not in nodes_to_render_ids:
                continue
            if next_fragment_id not in nodes_to_render_ids: # Рисуем ребра только к видимым узлам
                continue

            try:
                int_choice = int(choice_text)
                base_label = f"задержка {int_choice} секунд"
            except ValueError:
                base_label = choice_text[:40] + "..." if len(choice_text) > 40 else choice_text

            # Добавляем эффекты, если есть
            effects = choice.get("effects", [])
            effects_html_label = ""
            if isinstance(effects, list) and effects:
                effect_rows = ""
                has_check_symbols = False
                for effect in effects:
                    if not isinstance(effect, dict):
                        continue  # защита от мусора

                    stat = effect.get("stat", "???")
                    value = effect.get("value", "")
                    hide = effect.get("hide", False)

                    raw_effect_value = str(value)
                    if any(symbol in raw_effect_value for symbol in ['>', '<', '=']):
                        has_check_symbols = True

                    safe_effect_name = html.escape(stat)
                    safe_effect_value = html.escape(raw_effect_value)
                    if hide:
                        safe_effect_name = f"<I>{safe_effect_name}</I>"

                    effect_rows += f"<TR><TD ALIGN='LEFT'>{safe_effect_name}: {safe_effect_value}</TD></TR>"

                table_title = "Проверка" if has_check_symbols else "Эффекты"
                bgcolor = "#e6ccff" if has_check_symbols else "#F0FFF0"

                effects_html_label = f"""<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" BGCOLOR="{bgcolor}">
                <TR><TD ALIGN='CENTER'><B>{table_title}:</B></TD></TR>
                {effect_rows}
                </TABLE>"""

            # Объединяем метку стрелки
            if effects_html_label:
                edge_label = f"""<
                <TABLE BORDER="0" CELLBORDER="0" CELLSPACING="1">
                  <TR><TD>{html.escape(base_label)}</TD></TR>
                  <TR><TD>{effects_html_label}</TD></TR>
                </TABLE>
                >"""
            else:
                edge_label = base_label

            edge_labels[(fragment_id, next_fragment_id)] = edge_label

            # Определение цвета ребра
            source_branch_prefix = fragment_id.rsplit('_', 1)[0] if '_' in fragment_id else fragment_id
            
            if fragment_id not in all_fragments_data or next_fragment_id not in all_fragments_data: # Связь с несуществующим узлом (хотя filtered_fragments должен это предотвращать)
                edge_colors[(fragment_id, next_fragment_id)] = 'red'
            elif fragment_id in branch_node_ids and next_fragment_id in branch_node_ids: # Внутри основной ветки
                edge_colors[(fragment_id, next_fragment_id)] = branch_color_map.get(source_branch_prefix, 'blue') # Цвет ветки-источника
            elif fragment_id in branch_node_ids and next_fragment_id not in branch_node_ids: # ИЗ ветки вовне
                edge_colors[(fragment_id, next_fragment_id)] = 'darkorange'
            elif fragment_id not in branch_node_ids and next_fragment_id in branch_node_ids: # ИЗВНЕ в ветку
                edge_colors[(fragment_id, next_fragment_id)] = 'darkgreen'
            else: # Между двумя "внешними" узлами (маловероятно, но для полноты)
                 edge_colors[(fragment_id, next_fragment_id)] = 'grey'


            G.add_edge(fragment_id, next_fragment_id)
            
    if not G:
        logger.warning(f"Не удалось построить граф для ветки '{branch_name}' истории '{story_id}'.")
        return None

    dot = gv.Digraph(comment=f'Branch Map: {story_data.get("title", story_id)} - Branch: {branch_name}')
    dot.attr(rankdir='LR', bgcolor='white', dpi='250', concentrate='true') # concentrate для уменьшения пересечений

    for node_id_gv in G.nodes():
        dot.node(str(node_id_gv),
                 label=node_labels.get(node_id_gv, str(node_id_gv)),
                 shape='box', style='filled',
                 fillcolor=node_colors.get(node_id_gv, 'lightgrey'),
                 color='black', fontsize='10') # Уменьшил шрифт для компактности

    for u_gv, v_gv in G.edges():
        dot.edge(str(u_gv), str(v_gv),
                 label=edge_labels.get((u_gv, v_gv), ''),
                 color=edge_colors.get((u_gv, v_gv), 'grey'),
                 fontsize='9', fontcolor='black') # Уменьшил шрифт

    try:
        temp_dir = tempfile.gettempdir()
        filename_base = f"branch_map_{story_id}_{branch_name}_{uuid4().hex[:8]}"
        # filepath_dot = os.path.join(temp_dir, filename_base) # render сам добавит .gv
        filepath_png = os.path.join(temp_dir, filename_base + ".png")
        
        # Убрал filepath_dot из render, чтобы он сам создал временный .gv файл и удалил его, если cleanup=True
        # outfile=filepath_png указывает куда сохранить результат рендеринга (PNG)
        dot.render(filename=os.path.join(temp_dir, filename_base), format='png', cleanup=True) 
        # graphviz может создать filename.format, так что если filename был 'map', то будет 'map.png'
        # Проверяем, что файл существует по ожидаемому пути filepath_png
        # Если dot.render(outfile=filepath_png) поддерживается вашей версией, это может быть чище.
        # Но dot.render(filename=..., format=...) обычно создает filename.format.
        # Если он создал filename без .png, переименуем
        rendered_file_path = os.path.join(temp_dir, filename_base) 
        if os.path.exists(rendered_file_path) and not os.path.exists(filepath_png): # если создал без расширения
             os.rename(rendered_file_path, filepath_png)
        elif os.path.exists(rendered_file_path + ".gv") and os.path.exists(rendered_file_path): # Если cleanup=False и он создал и .gv и файл без расширения вместо .png
             if os.path.exists(filepath_png): # Если уже есть .png (например, от предыдущего вызова или он создал и .png и без расширения)
                  if os.path.exists(rendered_file_path) and rendered_file_path != filepath_png: os.remove(rendered_file_path) # удалить тот что без расширения
             else:
                  os.rename(rendered_file_path, filepath_png)


        if not os.path.exists(filepath_png):
             # Если вдруг файл filename.png (например map.png.png)
             expected_output_path = os.path.join(temp_dir, filename_base + ".png")
             if os.path.exists(expected_output_path):
                 filepath_png = expected_output_path
             else: # Если совсем не нашли
                 logger.error(f"Файл карты {filepath_png} (или {filename_base}) не был создан Graphviz.")
                 # Попытка найти файл, который мог создать graphviz
                 found_files = [f for f in os.listdir(temp_dir) if f.startswith(filename_base) and f.endswith(".png")]
                 if found_files:
                     filepath_png = os.path.join(temp_dir, found_files[0])
                     logger.info(f"Найден альтернативный файл карты: {filepath_png}")
                 else:
                     return None


        logger.info(f"Карта ветки '{branch_name}' истории '{story_id}' создана: {filepath_png}")
        return filepath_png
    except Exception as e:
        logger.error(f"Ошибка при рендеринге карты ветки {branch_name} истории {story_id}: {e}", exc_info=True)
        return None


#==========================================================================
#ЛОГИКА ПРОСМОТРА


async def view_public_stories_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    all_data = load_data()
    all_stories = all_data.get("users_story", {})

    public_stories = []
    for user_id, user_stories in all_stories.items():
        for story_id, story_data in user_stories.items():
            if story_data.get("public") and "user_name" in story_data:
                title = story_data.get("title", f"История {story_id[:8]}")
                short_title = title[:25] + ("…" if len(title) > 25 else "")
                author = story_data["user_name"]
                public_stories.append((
                    story_id,
                    user_id,
                    short_title,
                    author
                ))

    if not public_stories:
        await update.callback_query.edit_message_text(
            "Публичных историй пока нет.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Главное меню", callback_data="restart_callback")]
            ])
        )
        return

    keyboard = []
    for story_id, user_id, short_title, author in public_stories:
        callback_play = f"nstartstory_{user_id}_{story_id}_main_1"
        story_button = InlineKeyboardButton(
            f"{short_title} (Автор: {author})",
            callback_data=f"info_{user_id}_{story_id}"
        )
        keyboard.append([
            InlineKeyboardButton("▶️ Играть", callback_data=callback_play),
            story_button
        ])

    # Добавляем кнопку "Главное меню"
    keyboard.append([InlineKeyboardButton("Главное меню", callback_data="restart_callback")])

    message_text = "Публичные истории:"
    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        await update.callback_query.edit_message_text(
            message_text,
            reply_markup=reply_markup
        )
    except BadRequest:
        try:
            await update.callback_query.message.delete()
        except TelegramError:
            pass
        await update.callback_query.message.reply_text(
            message_text,
            reply_markup=reply_markup
        )



STORIES_PER_PAGE = 10  # Количество историй на одной странице

async def view_stories_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id_str = str(update.effective_user.id)
    user_stories_dict = load_all_user_stories(user_id_str)  # заменили ручной доступ

    query_data = update.callback_query.data if update.callback_query else ""
    is_neural_mode = "neural_stories_page_" in query_data or query_data == "view_neural_stories"
    is_coop_mode = "coop_stories_page_" in query_data or query_data == "view_coop_stories"

    if is_neural_mode:
        story_items = [
            (story_id, story_data) for story_id, story_data in user_stories_dict.items()
            if story_data.get("neural")
        ]
    elif is_coop_mode:
        coop_stories_dict = load_all_coop_stories_with_user(user_id_str)
        story_items = list(coop_stories_dict.items())

    else:
        story_items = [
            (story_id, story_data) for story_id, story_data in user_stories_dict.items()
            if not story_data.get("neural")
        ]

    if not story_items:
        if is_neural_mode:
            empty_text = "У вас пока нет нейроисторий."
        elif is_coop_mode:
            empty_text = "У вас пока нет историй для совместного редактирования."
        else:
            empty_text = "У вас пока нет созданных (не-нейросетевых) историй."

        buttons = []

        if is_neural_mode:
            buttons.append([InlineKeyboardButton("⬅️ К обычным историям", callback_data="view_stories_page_1")])
        else:
            buttons.append([InlineKeyboardButton("🧠 Нейроистории", callback_data="view_neural_stories")])

        if is_coop_mode:
            buttons.append([InlineKeyboardButton("⬅️ К обычным историям", callback_data="view_stories_page_1")])
        else:
            buttons.append([InlineKeyboardButton("🤝 Совместные", callback_data="view_coop_stories")])

        buttons.append([InlineKeyboardButton("🏠 Главное меню", callback_data="restart_callback")])

        if update.callback_query:
            try:
                await update.callback_query.edit_message_text(
                    empty_text,
                    reply_markup=InlineKeyboardMarkup(buttons)
                )
            except BadRequest as e:
                print(f"edit_message_text failed: {e}")
                try:
                    await update.callback_query.message.delete()
                except TelegramError as te:
                    print(f"delete failed: {te}")
                await update.callback_query.message.reply_text(
                    empty_text,
                    reply_markup=InlineKeyboardMarkup(buttons)
                )
        elif update.message:
            await update.message.reply_text(
                empty_text,
                reply_markup=InlineKeyboardMarkup(buttons)
            )

        return ConversationHandler.END

    # Определяем текущую страницу
    current_page = 1
    if any(query_data.startswith(prefix) for prefix in ["view_stories_page_", "neural_stories_page_", "coop_stories_page_"]):
        try:
            current_page = int(query_data.split("_")[-1])
        except ValueError:
            current_page = 1

    total_stories = len(story_items)
    total_pages = (total_stories + STORIES_PER_PAGE - 1) // STORIES_PER_PAGE

    start_index = (current_page - 1) * STORIES_PER_PAGE
    end_index = start_index + STORIES_PER_PAGE
    current_items = story_items[start_index:end_index]

    keyboard = []
    for story_id, story_data in current_items:
        title = story_data.get("title", f"История {story_id[:8]}...")
        short_title = title[:25] + ("…" if len(title) > 25 else ":")
        play_callback = f"nstartstory_{user_id_str}_{story_id}_main_1"

        if is_coop_mode:
            keyboard.append([
                InlineKeyboardButton(f"▶️ {short_title}", callback_data=play_callback),
                InlineKeyboardButton("✏️ Редакт.", callback_data=f"edit_story_{user_id_str}_{story_id}")
            ])
        else:
            keyboard.append([
                InlineKeyboardButton(f"▶️ {short_title}", callback_data=play_callback),
                InlineKeyboardButton("✏️ Редакт.", callback_data=f"edit_story_{user_id_str}_{story_id}"),
                InlineKeyboardButton("❌ Удалить", callback_data=f"delete_story_{user_id_str}_{story_id}")
            ])

    # Пагинация
    pagination_buttons = []
    page_prefix = (
        "neural_stories_page_" if is_neural_mode else
        "coop_stories_page_" if is_coop_mode else
        "view_stories_page_"
    )

    if current_page > 1:
        pagination_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"{page_prefix}{current_page - 1}"))
    if current_page < total_pages:
        pagination_buttons.append(InlineKeyboardButton("Вперёд ➡️", callback_data=f"{page_prefix}{current_page + 1}"))
    if pagination_buttons:
        keyboard.append(pagination_buttons)

    # Нижние кнопки
    bottom_buttons = []
    if is_neural_mode:
        keyboard.append([InlineKeyboardButton("🗑 Удалить все нейроистории", callback_data="confirm_delete_all_neural")])
        bottom_buttons.append(InlineKeyboardButton("⬅️ К обычным", callback_data="view_stories_page_1"))
        bottom_buttons.append(InlineKeyboardButton("🤝 Совместные", callback_data="view_coop_stories"))
    elif is_coop_mode:
        bottom_buttons.append(InlineKeyboardButton("⬅️ К обычным", callback_data="view_stories_page_1"))
        bottom_buttons.append(InlineKeyboardButton("🧠 Нейроистории", callback_data="view_neural_stories"))
    else:
        bottom_buttons.append(InlineKeyboardButton("🧠 Нейроистории", callback_data="view_neural_stories"))
        bottom_buttons.append(InlineKeyboardButton("🤝 Совместные", callback_data="view_coop_stories"))

    keyboard.append(bottom_buttons)
    keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data="restart_callback")])

    # Заголовок
    label = "Ваши нейроистории" if is_neural_mode else "Истории с совместным редактированием" if is_coop_mode else "Ваши истории"
    message_text = f"{label} (стр. {current_page}/{total_pages}):"

    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        try:
            await update.callback_query.edit_message_text(message_text, reply_markup=reply_markup)
        except BadRequest as e:
            print(f"edit_message_text failed: {e}")
            try:
                await update.callback_query.message.delete()
            except TelegramError as te:
                print(f"delete failed: {te}")
            await update.callback_query.message.reply_text(message_text, reply_markup=reply_markup)
    elif update.message:
        await update.message.reply_text(message_text, reply_markup=reply_markup)
    return ConversationHandler.END

async def confirm_delete_all_neural(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Да, удалить", callback_data="delete_all_neural_confirmed"),
            InlineKeyboardButton("❌ Отмена", callback_data="view_neural_stories")
        ]
    ])
    await update.callback_query.edit_message_text(
        "Вы уверены, что хотите удалить **все нейроистории**? Это действие необратимо.",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )

async def delete_all_neural_stories_firebase(update: Update, context: ContextTypes.DEFAULT_TYPE):
    
    if not firebase_admin._DEFAULT_APP_NAME:
        logger.error("Firebase приложение не инициализировано.")
        if update.callback_query:
            await update.callback_query.answer("Ошибка: база данных недоступна.", show_alert=True)
        return

    user_id = str(update.effective_user.id)
    user_stories_ref = db.reference(f'users_story/{user_id}')
    all_stories = user_stories_ref.get()

    if not all_stories:
        await update.callback_query.answer("У вас нет историй для удаления.", show_alert=True)
        return

    deleted_any = False
    for story_id, story_data in list(all_stories.items()):
        if story_data.get("neural"):
            user_stories_ref.child(story_id).delete()
            deleted_any = True
            logger.info(f"Удалена нейроистория {story_id} пользователя {user_id}")

    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='view_stories')]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if deleted_any:
        await update.callback_query.edit_message_text(
            "Все нейроистории были успешно удалены из базы данных. 🧠❌",
            reply_markup=reply_markup
        )
    else:
        await update.callback_query.answer("Нейроистории не найдены.", show_alert=True)



async def confirm_delete_story(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data  # delete_story_userid_storyid
    logger.info(f"data {data}.")    
    prefix, user_id_str, story_id = data.rsplit('_', 2)
    logger.info(f"story_id {story_id}.") 
    logger.info(f"user_id_str {user_id_str}.") 
    context.user_data['delete_candidate'] = (user_id_str, story_id)

    story_data = load_user_story(user_id_str, story_id)
    story_title = story_data.get("title", "Без названия")

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Да", callback_data="confirm_delete"),
            InlineKeyboardButton("◀️ Нет, вернуться", callback_data="view_stories")
        ]
    ])
    story_title = html.escape(story_title)
    await query.edit_message_text(
        f"Вы уверены, что хотите удалить историю <b>«{story_title}»</b>?",
        reply_markup=keyboard,
        parse_mode='HTML'
    )






# --- Логика статов ---


MAX_ALERT_LENGTH = 200 # Лимит Telegram для show_alert

# --- Новые вспомогательные функции для Firebase (прогресс пользователя в истории) ---

def get_user_progress_ref_path(story_id: str, user_id: int) -> str:
    """Возвращает путь к данным прогресса пользователя в Firebase."""
    return f'story_settings/{story_id}/{user_id}'

def load_user_story_progress(story_id: str, user_id: int) -> dict:
    """Загружает current_effects и fragment_id пользователя для указанной истории."""
    try:
        
        if not firebase_admin._DEFAULT_APP_NAME: # Проверка инициализации Firebase
            logger.error("Firebase приложение не инициализировано. Невозможно загрузить прогресс пользователя.")
            return {}
        ref = db.reference(get_user_progress_ref_path(story_id, user_id))
        data = ref.get()
        return data if isinstance(data, dict) else {} # Возвращаем dict, даже если null из Firebase
    except Exception as e:
        logger.error(f"Ошибка Firebase при загрузке прогресса пользователя story {story_id}, user {user_id}: {e}")
        return {}

def save_user_story_progress(story_id: str, user_id: int, progress_data: dict) -> None:
    """Сохраняет current_effects и fragment_id пользователя для указанной истории."""
    try:
        
        if not firebase_admin._DEFAULT_APP_NAME:
            logger.error("Firebase приложение не инициализировано. Невозможно сохранить прогресс пользователя.")
            return
        ref = db.reference(get_user_progress_ref_path(story_id, user_id))
        ref.set(progress_data)
    except Exception as e:
        logger.error(f"Ошибка Firebase при сохранении прогресса пользователя story {story_id}, user {user_id}: {e}")

def clear_user_story_complete_progress(story_id: str, user_id: int) -> None:
    """Полностью стирает данные прохождения истории (fragment_id, current_effects) для пользователя."""
    try:
        
        if not firebase_admin._DEFAULT_APP_NAME:
            logger.error("Firebase приложение не инициализировано. Невозможно очистить прогресс пользователя.")
            return
        ref = db.reference(get_user_progress_ref_path(story_id, user_id))
        ref.delete()
        logger.info(f"Полностью очищен прогресс для истории {story_id}, пользователя {user_id}.")
    except Exception as e:
        logger.error(f"Ошибка Firebase при полной очистке прогресса пользователя story {story_id}, user {user_id}: {e}")

# --- Новые вспомогательные функции для обработки эффектов ---

# --- Новые вспомогательные функции для обработки эффектов ---



def _get_random_value_from_range(
    min_val: int, 
    max_val: int, 
    modifiers: Optional[List[Dict[str, Any]]]
) -> int:
    """
    Выбирает случайное число из диапазона с учетом модификаторов вероятности.
    """
    if min_val > max_val:
        min_val, max_val = max_val, min_val # Гарантируем правильный порядок

    all_numbers = list(range(min_val, max_val + 1))
    
    # Если нет модификаторов, используем чистый рандом
    if not modifiers:
        return random.choice(all_numbers)

    # Базовый вес для каждого числа в диапазоне
    weights = [1.0] * len(all_numbers)

    for mod in modifiers:
        val = mod.get("value")
        prob = mod.get("prob")

        # Проверяем, что модификатор корректен и его значение входит в диапазон
        if val is None or prob is None or not (min_val <= val <= max_val):
            continue
        
        # Находим индекс числа, чтобы изменить его вес
        # val - min_val дает нам точный индекс в списке all_numbers/weights
        try:
            index = all_numbers.index(val)
            # Рассчитываем новый вес на основе процента 'prob'
            # +30% -> 1.3, -20% -> 0.8
            weight_multiplier = 1.0 + (prob / 100.0)
            weights[index] *= weight_multiplier
            # Убедимся, что вес не стал отрицательным
            if weights[index] < 0:
                weights[index] = 0
        except (ValueError, IndexError):
            # Этого не должно случиться благодаря проверке `min_val <= val <= max_val`
            continue

    # random.choices возвращает список, поэтому берем первый элемент
    # Если все веса оказались равны 0, random.choices вызовет ошибку,
    # в таком случае возвращаем просто случайное значение.
    if sum(weights) == 0:
        return random.choice(all_numbers)
        
    return random.choices(all_numbers, weights=weights, k=1)[0]



from typing import List, Dict, Any, Tuple, Optional, Union



def _parse_effect_value(value_str: str) -> Tuple[str, Optional[str], Optional[Union[int, Tuple[int, int]]]]:
    """
    Разбирает строку значения эффекта. 
    Теперь возвращает (тип_действия, символ_операции, число_или_диапазон).
    Для диапазонов возвращается кортеж (min, max).
    """
    value_str = str(value_str).strip()

    # Новый парсинг для диапазонов
    # 1. modify с диапазоном: +(7-20) или -(-3-15)
    modify_range_match = re.match(r"^([+\-])\s*\((-?\d+)-(-?\d+)\)$", value_str)
    if modify_range_match:
        op, min_val_str, max_val_str = modify_range_match.groups()
        try:
            return "modify_range", op, (int(min_val_str), int(max_val_str))
        except ValueError:
            logger.warning(f"Не удалось разобрать диапазон в modify: {value_str}")
            return "invalid", None, None

    # 2. set с диапазоном: 3-10 или -7--12
    set_range_match = re.match(r"^(-?\d+)-(-?\d+)$", value_str)
    if set_range_match:
        min_val_str, max_val_str = set_range_match.groups()
        try:
            return "set_range", None, (int(min_val_str), int(max_val_str))
        except ValueError:
            logger.warning(f"Не удалось разобрать диапазон в set: {value_str}")
            return "invalid", None, None

    # Старая логика для конкретных чисел
    if value_str.startswith(('+', '-')):
        op = value_str[0]
        try:
            num = int(value_str[1:])
            return "modify", op, num
        except ValueError:
            logger.warning(f"Не удалось разобрать числовую часть значения эффекта modify: {value_str}")
            return "invalid", None, None
    elif value_str.startswith(('>', '<', '=')):
        op = value_str[0]
        try:
            num = int(value_str[1:])
            return "check", op, num
        except ValueError:
            logger.warning(f"Не удалось разобрать числовую часть значения эффекта check: {value_str}")
            return "invalid", None, None
    else:
        try:
            num = int(value_str)
            return "set", None, num
        except ValueError:
            logger.warning(f"Не удалось разобрать значение эффекта set: {value_str}")
            return "invalid", None, None

async def process_choice_effects_on_click(
    story_id: str,
    user_id: int,
    effects_list: List[Dict[str, Any]],
    query: 'Update.callback_query' # Используем строку, чтобы избежать прямого импорта
) -> Tuple[bool, str, bool]:
    """
    Обрабатывает эффекты при нажатии кнопки выбора.
    Возвращает: (продолжить_переход, текст_уведомления_об_успехе, сигнал_скрыть_кнопку_при_ошибке)
    """
    user_progress = load_user_story_progress(story_id, user_id)
    current_effects_data = user_progress.get("current_effects", {})
    temp_effects_data = dict(current_effects_data)

    success_alert_parts = []

    for effect in effects_list:
        stat_name = effect.get("stat")
        value_str = effect.get("value", "")
        hide_effect = effect.get("hide", False)
        
        # Шаг 1: Парсим значение, которое может быть числом или диапазоном
        parsed_action_type, op_char, parsed_value = _parse_effect_value(value_str)
        
        if parsed_action_type == "invalid" or not stat_name or parsed_value is None:
            logger.warning(f"Пропуск неверного или неполного эффекта: {effect}")
            continue

        # Шаг 2: Определяем конечное числовое значение и тип действия
        final_numeric_val = None
        final_action_type = None

        if parsed_action_type in ("set_range", "modify_range"):
            min_val, max_val = parsed_value
            modifiers = effect.get("modifiers")
            final_numeric_val = _get_random_value_from_range(min_val, max_val, modifiers)
            # Преобразуем тип действия в базовый для дальнейшей обработки
            final_action_type = "set" if parsed_action_type == "set_range" else "modify"
        else:
            final_numeric_val = parsed_value
            final_action_type = parsed_action_type

        # Получаем текущее значение стата, учитывая изменения от предыдущих эффектов
        current_stat_val_for_effect = temp_effects_data.get(stat_name)

        # Шаг 3: Используем существующую логику с финальными значениями
        if final_action_type == "check":
            val_for_check = 0
            if current_stat_val_for_effect is not None:
                try:
                    val_for_check = int(current_stat_val_for_effect)
                except (ValueError, TypeError):
                    logger.warning(f"Стат {stat_name} имеет нечисловое значение '{current_stat_val_for_effect}'. Используется 0 для проверки.")
                    val_for_check = 0
            
            check_passed = False
            if op_char == '>' and val_for_check > final_numeric_val: check_passed = True
            elif op_char == '<' and val_for_check < final_numeric_val: check_passed = True
            elif op_char == '=' and val_for_check == final_numeric_val: check_passed = True

            if not check_passed:
                if hide_effect:
                    return False, "", True
                else:
                    reason = f"Требование: {stat_name} {op_char}{final_numeric_val} (тек: {val_for_check})"
                    if len(reason) > MAX_ALERT_LENGTH: reason = reason[:MAX_ALERT_LENGTH-3]+"..."
                    await query.answer(text=reason, show_alert=True)
                    return False, "", False
        
        elif final_action_type == "set":
            temp_effects_data[stat_name] = final_numeric_val
            user_progress["current_effects"] = temp_effects_data
            save_user_story_progress(story_id, user_id, user_progress)            
            if not hide_effect:
                success_alert_parts.append(f"▫️Ваш атрибут {stat_name} установлен на: {final_numeric_val}")
        
        elif final_action_type == "modify":
            base_for_modification = 0
            if current_stat_val_for_effect is not None:
                try:
                    base_for_modification = int(current_stat_val_for_effect)
                except (ValueError, TypeError):
                    logger.warning(f"Стат {stat_name} имеет нечисловое значение '{current_stat_val_for_effect}'. Используется 0 как база для модификации.")
                    base_for_modification = 0
            
            new_val = (base_for_modification + final_numeric_val) if op_char == '+' else (base_for_modification - final_numeric_val)
            user_progress["current_effects"] = temp_effects_data
            save_user_story_progress(story_id, user_id, user_progress)          
            temp_effects_data[stat_name] = new_val
            
            if not hide_effect:
                action_word = "увеличен" if op_char == '+' else "уменьшен"
                success_alert_parts.append(f"▫️Ваш атрибут {stat_name} {action_word} на {abs(final_numeric_val)}")

    # Сохраняем все накопленные изменения в конце
    user_progress["current_effects"] = temp_effects_data
    save_user_story_progress(story_id, user_id, user_progress)

    alert_text = ""
    if success_alert_parts:
        alert_text = "\n".join(success_alert_parts)
        if len(alert_text) > MAX_ALERT_LENGTH:
            alert_text = alert_text[:MAX_ALERT_LENGTH-3] + "..."
    
    return True, alert_text, False




def evaluate_choice_for_display(
    story_id: str,
    user_id: int,
    effects_list: List[Dict[str, Any]]
) -> Tuple[bool, str]:
    """
    Оценивает эффекты выбора для отображения кнопки (видимость и текст требований).
    Возвращает: (видна_ли_кнопка, текст_требований_для_кнопки)
    """
    user_progress = load_user_story_progress(story_id, user_id)
    current_effects_data = user_progress.get("current_effects", {})
    requirement_parts = [] # Части для текста требований (например, "интеллект > 5")

    for effect in effects_list:
        stat_name = effect.get("stat")
        value_str = effect.get("value", "")
        hide_effect = effect.get("hide", False)

        action_type, op_char, numeric_val = _parse_effect_value(value_str)

        if action_type == "invalid" or not stat_name or numeric_val is None: continue

        if action_type == "check":
            current_stat_val = current_effects_data.get(stat_name)
            val_for_check = 0 # Значение по умолчанию для проверки, если стат не существует
            if current_stat_val is not None:
                try:
                    val_for_check = int(current_stat_val)
                except (ValueError, TypeError):
                    val_for_check = 0
            
            check_passed = False
            if op_char == '>' and val_for_check > numeric_val: check_passed = True
            elif op_char == '<' and val_for_check < numeric_val: check_passed = True
            elif op_char == '=' and val_for_check == numeric_val: check_passed = True

            if not check_passed and hide_effect:
                return False, "" # Кнопка должна быть полностью скрыта

            if not hide_effect: # Добавляем к отображаемому тексту для нескрытых проверок
                requirement_parts.append(f"{stat_name} {op_char}{numeric_val}")
    
    req_text = f" ({', '.join(requirement_parts)})" if requirement_parts else ""
    return True, req_text



def apply_effect_values(base_text, effects_dict):
    def replacer(match):
        key = match.group(1).strip().lower()
        value = effects_dict.get(key)
        if value is not None:
            return f"{key}:{value}"
        else:
            return f"{key}:не найдено"
    
    return re.sub(r"\{\{(.*?)\}\}", replacer, base_text)
# --- Логика создания истории (ConversationHandler) ---

async def show_story_fragment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    message = query.message
    actual_user_id = query.from_user.id # ID пользователя, который нажал кнопку

    logger.info(f"show_story_fragment вызвана query: {query.data} от пользователя {actual_user_id}")
    logger.info(f"Получен callback_query с данными: {Update}")
    data_parts = query.data.split("_", 3)
    logger.info(f"Разобранные data_parts: {data_parts}")

    user_id_from_callback_str: Optional[str] = None
    story_id_from_data: str
    target_fragment_id: str # ID фрагмента, НА КОТОРЫЙ ведет кнопка



    if len(data_parts) == 4: # Формат: play_UID_STORYID_FRAGMENTID
        _, user_id_from_callback_str, story_id_from_data, target_fragment_id = data_parts
    elif len(data_parts) == 3: # Формат: play_STORYID_FRAGMENTID (для приватных чатов)
        _, story_id_from_data, target_fragment_id = data_parts
    else:
        logger.error(f"Ошибка формата callback_data: {query.data}")
        await context.bot.send_message(chat_id=message.chat.id, text="Ошибка формата данных для колбэка.")
        return


    original_target_fragment_id = target_fragment_id
    target_fragment_id_cleaned = re.sub(r'id\d+$', '', target_fragment_id)

    # Проверка для групповых чатов
    if message.chat.type in ("group", "supergroup"):
        if user_id_from_callback_str and int(user_id_from_callback_str) != actual_user_id:
            await query.answer(
                text="⚠️ Данная история вызвана не вами. Используйте кнопку \"Перейти к запуску истории\" чтобы создать своё окно для прохождения этой истории.",
                show_alert=True
            )
            username_display = query.from_user.first_name or "Пользователь"
            story_info = f"📖 История *{story_id_from_data}* ожидает запуска."
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("▶️ Играть", callback_data=f"play_{actual_user_id}_{story_id_from_data}_main_1")]
            ])
            await query.message.reply_text(
                f"🎮 Запуск истории готов для пользователя: {username_display}.\n\n{story_info}\n\nНажмите кнопку ниже, чтобы начать играть:",
                reply_markup=keyboard,
                parse_mode="Markdown" # Используйте ParseMode.MARKDOWN_V2 если предпочитаете
            )
            return

    user_id = query.from_user.id
    chat_id = message.chat.id

    # --- Отмена активных задач для этой истории и чата ---
    # Отмена таймера авто-перехода
    auto_timer_key = f"{user_id}_{story_id_from_data}_{chat_id}"
    if auto_timer_key in active_timers:
        logger.info(f"User action: Cancelling auto-timer {auto_timer_key}")
        active_timers[auto_timer_key].cancel()
        # Удаление из active_timers произойдет в finally блока auto_transition_task

    # Отмена задачи редактирования текста/caption
    edit_task_key = f"edit_{user_id}_{story_id_from_data}_{chat_id}" # Используем ключ без message_id для отмены "общей" задачи редактирования для этого фрагмента
    if edit_task_key in active_edit_tasks:
        logger.info(f"User action: Cancelling timed_edit task {edit_task_key}")
        active_edit_tasks[edit_task_key].cancel()
        # Удаление из active_edit_tasks произойдет в finally блока run_timed_edits

    context.user_data.pop(f"auto_path_{user_id}_{story_id_from_data}_{chat_id}", None)


    # --- Загрузка определения истории ---
    story_data_found = load_user_story(actual_user_id, story_id_from_data)
    if not story_data_found:
        story_data_found = load_story_by_id_fallback(story_id_from_data)
    
    if not story_data_found:
        logging.info(f"История {story_id_from_data} не найдена даже через fallback.")
        await query.answer("История не найдена.", show_alert=True) # Отвечаем на query
        return

    story_owner_id = get_owner_id(story_id_from_data, story_data_found)




    # --- Обработка эффектов и прогресса пользователя ---
    alert_after_effects_processed_text = "" # Сообщение для query.answer после успешной обработки

    if target_fragment_id_cleaned == "main_1":
        clear_user_story_complete_progress(story_id_from_data, actual_user_id)
        logger.info(f"Пользователь {actual_user_id} начал/перезапустил историю {story_id_from_data} с main_1. Прогресс очищен.")
        # Для main_1 нет эффектов от *выбора*, т.к. это действие сброса.
        # Уведомление query.answer() не требуется для самого сброса, если только нет спец. сообщения.
        await query.answer() # Просто подтвердить нажатие кнопки main_1
    else:
        # Это нажатие на обычную кнопку выбора, не "начать историю".
        # Эффекты определены в выборе внутри *исходного* фрагмента.
        user_progress_before_click = load_user_story_progress(story_id_from_data, actual_user_id)
        source_fragment_id = user_progress_before_click.get("fragment_id")

        if source_fragment_id:
            source_fragment_data = story_data_found.get("fragments", {}).get(source_fragment_id)
            logger.info(f"source_fragment_data: {source_fragment_data}")
            if source_fragment_data:
                effects_to_apply_for_choice = []
                
                # Шаг 1: Удаляем суффикс idX, если он есть
                match = re.match(r"^(.*?)(?:id(\d+))?$", original_target_fragment_id)
                if match:
                    base_target_id = match.group(1)
                    suffix_index = int(match.group(2)) if match.group(2) is not None else 0
                else:
                    base_target_id = original_target_fragment_id
                    suffix_index = 0
                
                logger.info(f"Base target id: {base_target_id}, suffix index: {suffix_index}")
                
                # Шаг 2: Считаем совпадения target == base_target_id и ищем нужное по порядку
                count = 0
                for choice_in_source in source_fragment_data.get("choices", []):
                    if choice_in_source.get("target") == base_target_id:
                        if count == suffix_index:
                            effects_to_apply_for_choice = choice_in_source.get("effects", [])
                            logger.info(f"effects_to_apply_for_choice: {effects_to_apply_for_choice}")
                            break
                        count += 1
                
                if effects_to_apply_for_choice:
                    proceed, alert_text_success, hide_button_signal = await process_choice_effects_on_click(
                        story_id_from_data,
                        actual_user_id,
                        effects_to_apply_for_choice,
                        query
                    )
                    if not proceed:
                        return
                    alert_after_effects_processed_text = alert_text_success
                else:
                    await query.answer()
            else:
                logger.warning(f"Исходный фрагмент {source_fragment_id} не найден в данных истории {story_id_from_data}.")
                await query.answer("Ошибка: предыдущий фрагмент не найден.", show_alert=True)
                return
        else:
            logger.info(f"Нет source_fragment_id в прогрессе пользователя для истории {story_id_from_data}, пользователь {actual_user_id}. Эффекты выбора не обработаны.")
            await query.answer()



    # Если есть текст уведомления после успешной обработки эффектов, показываем его.
    # Это произойдет, если target_fragment_id != "main_1" и были успешно применены видимые эффекты.
    if alert_after_effects_processed_text:
        await query.answer(text=alert_after_effects_processed_text, show_alert=True)
    elif target_fragment_id_cleaned != "main_1" and not source_fragment_id : # если не main_1, но и не смогли обработать эффекты
        pass # query.answer() был вызван ранее, или не требуется

    # Обновляем fragment_id пользователя в Firebase на target_fragment_id
    # Это происходит ПОСЛЕ успешной обработки эффектов, или если это main_1.
    # Для main_1 это восстанавливает fragment_id после clear_user_story_complete_progress.
    current_progress_after_effects = load_user_story_progress(story_id_from_data, actual_user_id) # Перезагружаем свежие данные
    logger.info(f"current_progress_after_effects {current_progress_after_effects}")    
    current_progress_after_effects["fragment_id"] = target_fragment_id_cleaned
    save_user_story_progress(story_id_from_data, actual_user_id, current_progress_after_effects)
    logger.info(f"Пользователь {target_fragment_id_cleaned} теперь на фрагменте {target_fragment_id_cleaned} в истории {story_id_from_data}.")








    # --- Загрузка и подготовка контента для *целевого* фрагмента ---
    fragments_dict = story_data_found.setdefault("fragments", {})
    target_fragment_data = fragments_dict.get(target_fragment_id_cleaned)

    if not target_fragment_data:
        if story_data_found.get("neuro_fragments", False): # Обработка нейро-фрагментов
            logger.info(f"Создание пустого нейро-фрагмента '{target_fragment_id_cleaned}' для истории {story_id_from_data}")
            fragments_dict[target_fragment_id_cleaned] = {"text": "", "media": [], "choices": []}
            save_story_data(str(story_owner_id), story_id_from_data, story_data_found) # Сохраняем обновленную структуру истории
            target_fragment_data = fragments_dict[target_fragment_id_cleaned]
        else:
            logger.error(f"Целевой фрагмент '{target_fragment_id_cleaned}' не найден и не является нейро-фрагментом.")
            # Ответ пользователю уже был дан через query.answer() ранее, если это была ошибка эффектов
            # Если это новая ошибка (фрагмент просто отсутствует), то:
            await context.bot.send_message(chat_id=message.chat.id, text=f"Фрагмент '{target_fragment_id_cleaned}' не найден.")
            return

    fragment_text_content = target_fragment_data.get("text", "")
    # base_text_for_display и edit_steps как в вашем коде
    base_text_for_display = re.split(r"(\[\[[-+]\d+\]\]|\(\([-+]\d+\)\))", fragment_text_content, 1)[0].strip()
    current_effects = current_progress_after_effects.get("current_effects", {})
    base_text_for_display = apply_effect_values(base_text_for_display, current_effects)    
    edit_steps = parse_timed_edits(fragment_text_content)

    await render_fragment(
        context=context,
        user_id=actual_user_id,
        owner_id=story_owner_id, 
        story_id=story_id_from_data,
        fragment_id=target_fragment_id_cleaned, # Рендерим целевой фрагмент
        message_to_update=message,
        story_data=story_data_found, 
        chat_id=message.chat.id,
        current_auto_path=[], 
        base_text_for_display=base_text_for_display,
        edit_steps_for_text=edit_steps
    )


def normalize_fragments(fragments: Dict[str, Any]) -> Dict[str, Any]:
    normalized = {}

    for key, value in fragments.items():
        # Если значение — словарь с одним ключом, совпадающим с внешним ключом
        if isinstance(value, dict) and len(value) == 1 and key in value:
            inner_value = value[key]
            if isinstance(inner_value, dict):
                normalized[key] = inner_value
                continue

        # Рекурсивно нормализуем, если внутри словарь (например, в случае нескольких вложенных уровней)
        if isinstance(value, dict):
            normalized[key] = normalize_fragments(value)
        else:
            normalized[key] = value

    return normalized






# Глобальный словарь для хранения активных таймеров (если хочешь поддерживать несколько одновременно)
active_timers: Dict[str, asyncio.Task] = {}
active_edit_tasks: Dict[str, asyncio.Task] = {}

async def render_fragment(
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    owner_id: int,
    story_id: str,
    fragment_id: str,
    message_to_update: Optional[Message],
    story_data: Dict[str, Any],
    chat_id: int,
    current_auto_path: List[str],
    base_text_for_display: str,
    edit_steps_for_text: List[Dict]
):
    logger.info(
        "render_fragment called with:\n"
        # ... (логирование параметров) ...
        f"base_text_for_display='{base_text_for_display[:30]}...', edit_steps_count={len(edit_steps_for_text)}"
    )

    fragment = story_data.get("fragments", {}).get(fragment_id)
    neuro_mode = story_data.get("neuro_fragments", False)

    # Если base_text_for_display пуст, пытаемся его получить из фрагмента (может быть полезно при прямом вызове render_fragment)
    if not base_text_for_display and fragment:
         raw_text = fragment.get("text", "")
         base_text_for_display = re.split(r"(\[\[[-+]\d+\]\]|\(\([-+]\d+\)\))", raw_text, 1)[0].strip()
         if not edit_steps_for_text and raw_text: # Пересчитываем edit_steps, если текст изменился
             edit_steps_for_text = parse_timed_edits(raw_text)

    if not fragment or (not fragment.get("text") and not fragment.get("media")):
        if neuro_mode:
            logger.info(f"Фрагмент {fragment_id} отсутствует или пуст, инициируем генерацию через ИИ для пользователя {owner_id}.")
            generation_status_message = await context.bot.send_message(chat_id, "Фрагмент не создан автором и будет сгенерирован, ожидайте…")

            async def background_generation_fragment():
                new_story_data_local = None # Для рекурсивного вызова render_fragment
                generated_fragment_text_local = "" # Для рекурсивного вызова
                try:
                    # Убедитесь, что generate_gemini_fragment, normalize_fragments, save_story_data, load_data определены
                    raw_response = await generate_gemini_fragment(owner_id, story_id, fragment_id)
                    logger.debug(f"Сырой ответ от generate_gemini_fragment: {raw_response}")
                    if not raw_response:
                        logger.error(f"generate_gemini_fragment вернул пустой ответ для пользователя {owner_id}, история {story_id}, фрагмент {fragment_id}.")
                        await context.bot.send_message(chat_id, "Ошибка: ИИ вернул пустой ответ. Попробуйте позже.")
                        try:
                            await generation_status_message.delete()
                        except Exception:
                            pass
                        return



                    start = raw_response.find('{')
                    end = raw_response.rfind('}') + 1

                    if start == -1 or end <= start:
                        logger.error(f"Невозможно найти JSON в ответе: {raw_response}")
                        await context.bot.send_message(chat_id, "Ошибка: ИИ вернул некорректный ответ.")
                        try:
                            await generation_status_message.delete()
                        except Exception:
                            pass
                        return

                    cleaned_json_str = raw_response[start:end]
                    generated_fragment = json.loads(cleaned_json_str)
                    logger.info(f"Сгенерированный фрагмент: {generated_fragment}")

                    if "fragments" in generated_fragment:
                        generated_fragment["fragments"] = normalize_fragments(generated_fragment["fragments"])
                        for fid, frag in generated_fragment["fragments"].items():
                            story_data.setdefault("fragments", {})[fid] = frag
                    else:
                        if isinstance(generated_fragment, dict) and len(generated_fragment) == 1:
                            fid, frag_data = list(generated_fragment.items())[0]
                            if isinstance(frag_data, dict) and fid in frag_data:
                                frag_data = frag_data[fid]
                            story_data.setdefault("fragments", {})[fid] = frag_data
                        else:
                            story_data.setdefault("fragments", {})[fragment_id] = generated_fragment
                    
                    # Сохраняем данные и пытаемся их перезагрузить для актуальности
                    save_story_data(str(owner_id), story_id, story_data)
                    new_story_data_local = load_user_story(owner_id, story_id) # load_data должна вернуть актуальные данные


                    if not new_story_data_local:
                        logger.error(f"Ошибка: не удалось загрузить сгенерированный фрагмент для пользователя {owner_id}, история {story_id}.")
                        await context.bot.send_message(chat_id, "Ошибка: не удалось загрузить сгенерированный фрагмент.")
                        try:
                            await generation_status_message.delete()
                        except Exception: pass
                        return

                    # Извлечение текста для base_text_for_display в рекурсивном вызове
                    # Предполагаем, что fragment_id это ключ к основному сгенерированному тексту
                    current_generated_frag_data = new_story_data_local.get("fragments", {}).get(fragment_id, {})
                    generated_fragment_text_local = current_generated_frag_data.get("text", "")
                    base_text_for_display = re.split(r"(\[\[[-+]\d+\]\]|\(\([-+]\d+\)\))", generated_fragment_text_local, 1)[0].strip()
                    edit_steps = parse_timed_edits(generated_fragment_text_local)
                    try: # Удаляем сообщение "генерируется"
                        await generation_status_message.delete()
                    except Exception: pass
                    
                    # Рекурсивный вызов для отображения сгенерированного фрагмента
                    await render_fragment(
                        context=context, user_id=user_id, owner_id=owner_id, story_id=story_id, fragment_id=fragment_id,
                        message_to_update=None, # Отправляем как новое сообщение
                        story_data=new_story_data_local, chat_id=chat_id, current_auto_path=current_auto_path,
                        base_text_for_display=base_text_for_display, # Текст конкретного фрагмента
                        edit_steps_for_text=edit_steps
                    )

                except asyncio.CancelledError:
                    logger.info(f"Генерация фрагмента для пользователя {user_id} была отменена.")
                    try:
                        await generation_status_message.edit_text("Генерация фрагмента была отменена.")
                    except Exception:
                        try: # Если редактирование не удалось, отправляем новое сообщение
                           await context.bot.send_message(chat_id, "Генерация фрагмента была отменена.")
                        except Exception as e_send: logger.error(f"Не удалось отправить сообщение об отмене генерации фрагмента: {e_send}")
                except Exception as e:
                    logger.exception(f"Ошибка при генерации фрагмента для пользователя {user_id}: {e}")
                    try:
                        await generation_status_message.edit_text("Не удалось сгенерировать фрагмент.")
                    except Exception:
                        try: # Если редактирование не удалось, отправляем новое сообщение
                            await context.bot.send_message(chat_id, "Не удалось сгенерировать фрагмент.")
                        except Exception as e_send: logger.error(f"Не удалось отправить сообщение об ошибке генерации фрагмента: {e_send}")
            
            # Создаем и сохраняем задачу
            task = asyncio.create_task(background_generation_fragment())
            user_tasks_set = context.user_data.setdefault('user_tasks', set())
            user_tasks_set.add(task)
            task.add_done_callback(lambda t: _remove_task_from_context(t, context.user_data))
            return # Прерываем выполнение до завершения генерации
        else:
            error_text = "Фрагмент не найден." if not fragment else "Фрагмент пуст."
            if message_to_update:
                try:
                    await context.bot.delete_message(chat_id=chat_id, message_id=message_to_update.message_id)
                except (BadRequest, TelegramError): # Убедитесь, что BadRequest и TelegramError импортированы
                    logger.warning(f"render_fragment: Could not delete message_to_update {message_to_update.message_id}")
                await context.bot.send_message(chat_id, error_text)
            else:
                await context.bot.send_message(chat_id, error_text)
            return





    # --- Подготовка кнопок и оценка эффектов для отображения ---
    choices_data = fragment.get("choices", []) if fragment else []
    inline_buttons = []
    auto_transition_timer_delay = float('inf')
    auto_transition_target_fragment_id = None
    visible_button_count = 0


    target_counter = defaultdict(int)

    for i, choice in enumerate(choices_data):
        text = choice.get("text")
        target = choice.get("target")
        effects = choice.get("effects", [])
        
        # Пропускаем авто-переходы
        try:
            delay = float(text)
            if 0 < delay < auto_transition_timer_delay:
                auto_transition_timer_delay = delay
                auto_transition_target_fragment_id = target
            continue
        except (ValueError, TypeError):
            pass

        # Оцениваем видимость кнопки
        is_button_visible, requirement_text = evaluate_choice_for_display(
            story_id,
            user_id,
            effects
        )
        if not is_button_visible:
            continue

        # Считаем, сколько раз этот target уже встречался
        count = target_counter[target]
        target_counter[target] += 1

        # Добавляем #N, если выбор не уникален
        if count > 0:
            target_with_index = f"{target}id{count}"
        else:
            target_with_index = target
        logger.info(f"target_with_index: {target_with_index}")
        button_display_text = text + requirement_text
        button_callback_data = f"play_{user_id}_{story_id}_{target_with_index}"
        inline_buttons.append([InlineKeyboardButton(button_display_text, callback_data=button_callback_data)])
        visible_button_count += 1
    
    reply_markup = InlineKeyboardMarkup(inline_buttons) if inline_buttons else None

    # --- Логика "тупика" ---
    # Используем временный список для частей текста, чтобы избежать частых конкатенаций строк
    current_display_text_parts = [base_text_for_display if base_text_for_display else ""] # Убедимся, что base_text_for_display не None

    if choices_data and visible_button_count == 0 and not auto_transition_target_fragment_id:
        if not neuro_mode: # Предполагаем, что в нейро-режиме тупиков быть не должно, или они обрабатываются иначе
            dead_end_suffix = "\n\n(похоже вы зашли в тупик)"
            current_display_text_parts.append(dead_end_suffix)

    final_base_text_for_display = "".join(current_display_text_parts)
    
    # Проверка длины (особенно для caption медиа) должна выполняться непосредственно перед отправкой/редактированием.
    # final_base_text_for_display будет использоваться ниже.

    # --- Очистка предыдущих сообщений (существующая логика из вашего кода) ---
    last_messages_key = f"last_story_messages_{user_id}_{story_id}_{chat_id}"
    previous_message_ids = context.user_data.pop(last_messages_key, [])
    message_id_to_keep_for_editing = message_to_update.message_id if message_to_update else None
    
    for mid in previous_message_ids:
        if mid == message_id_to_keep_for_editing:
            continue
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=mid)
        except (BadRequest, TelegramError):
            logger.warning(f"Не удалось удалить старое сообщение {mid} для user {user_id}, chat {chat_id}.")
        except Exception as e: # Более общая ошибка
            logger.error(f"Неожиданная ошибка при удалении сообщения {mid}: {e}", exc_info=True)





    # text_content больше не используется напрямую для отображения, используем base_text_for_display
    # fragment.get("text", "") все еще нужен для parse_timed_edits, но это делается в show_story_fragment
    media_content = fragment.get("media", [])

    # --- 1. Очистка предыдущих сообщений ---
    last_messages_key = f"last_story_messages_{user_id}_{story_id}_{chat_id}"
    previous_message_ids = context.user_data.pop(last_messages_key, [])
    message_id_to_keep_for_editing = message_to_update.message_id if message_to_update else None
    
    for mid in previous_message_ids:
        if mid == message_id_to_keep_for_editing:
            continue
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=mid)
        except (BadRequest, TelegramError): # Добавил TelegramError для общности
            logger.warning(f"Failed to delete old message {mid} for user {user_id}, chat {chat_id}.")
        except Exception as e:
            logger.error(f"Unexpected error deleting message {mid}: {e}", exc_info=True)

    
    reply_markup = InlineKeyboardMarkup(inline_buttons) if inline_buttons else None

    # --- 3. Детекция циклов для авто-перехода ---
    is_auto_transition_planned = False
    if auto_transition_target_fragment_id is not None:
        if auto_transition_target_fragment_id in current_auto_path:
            logger.warning(
                f"Loop detected for user {user_id}, story {story_id}, chat {chat_id}. "
                f"Target '{auto_transition_target_fragment_id}' already in path {current_auto_path}. Halting auto-transition."
            )
            auto_transition_target_fragment_id = None
            context.user_data.pop(f"auto_path_{user_id}_{story_id}_{chat_id}", None)
        else:
            is_auto_transition_planned = True

    # --- 4. Отправка/редактирование контента ---
    newly_sent_message_object: Optional[Message] = None
    # `first_media_message_for_caption_edit` для случая с медиагруппой, чтобы к нему применить timed_edit
    first_media_message_for_caption_edit: Optional[Message] = None 
    final_message_ids_sent: List[int] = []
    
    # Ключ для задачи редактирования этого конкретного сообщения/фрагмента
    # Важно: Этот ключ должен быть уникальным для текущего активного сообщения с редактированием.
    # Если show_story_fragment отменяет по "общему" ключу, а render_fragment создает по "общему" ключу,
    # то новый render_fragment отменит старую задачу редактирования перед запуском новой.
    edit_task_key = f"edit_{user_id}_{story_id}_{chat_id}"

    try:
        if media_content:
            if len(media_content) > 1: # Медиа-группа
                media_group_to_send = []
                for i, m_item in enumerate(media_content):
                    m_type = m_item.get("type")
                    file_id = m_item.get("file_id")
                    spoiler = m_item.get("spoiler", False) # Убедимся, что есть значение по умолчанию
                    
                    # base_text_for_display используется для caption первого элемента
                    caption_for_item = final_base_text_for_display if i == 0 and final_base_text_for_display else None
                    
                    if m_type == "photo":
                        media_group_to_send.append(InputMediaPhoto(media=file_id, caption=caption_for_item, parse_mode=ParseMode.HTML if caption_for_item else None, has_spoiler=spoiler))
                    elif m_type == "video":
                        media_group_to_send.append(InputMediaVideo(media=file_id, caption=caption_for_item, parse_mode=ParseMode.HTML if caption_for_item else None, has_spoiler=spoiler))
                    elif m_type == "animation":
                         media_group_to_send.append(InputMediaAnimation(media=file_id, caption=caption_for_item, parse_mode=ParseMode.HTML if caption_for_item else None, has_spoiler=spoiler))
                    # Добавьте другие типы, если нужно (audio, document)

                if message_to_update: # Если есть старое сообщение, удаляем его, т.к. медиагруппу нельзя отредактировать в другое сообщение
                    try:
                        await context.bot.delete_message(chat_id, message_to_update.message_id)
                        message_to_update = None # Оно больше не актуально для редактирования
                    except (BadRequest, TelegramError): pass
                
                if media_group_to_send:
                    sent_media_messages = await context.bot.send_media_group(chat_id=chat_id, media=media_group_to_send)
                    if sent_media_messages:
                        newly_sent_message_object = sent_media_messages[0] # Для ссылок и ID
                        first_media_message_for_caption_edit = sent_media_messages[0] # Для timed_edit caption
                        final_message_ids_sent.extend([msg.message_id for msg in sent_media_messages])
                    # Отправляем кнопки отдельным сообщением после медиагруппы, если они есть
                    if reply_markup:
                        markup_msg = await context.bot.send_message(chat_id, "Кнопки выбора:", reply_markup=reply_markup) # Невидимый символ для отправки только клавиатуры
                        final_message_ids_sent.append(markup_msg.message_id)

            else: # Одиночное медиа
                item = media_content[0]
                media_type = item.get("type")
                file_id = item.get("file_id")
                spoiler = item.get("spoiler", False)

                can_edit_media = False
                if message_to_update:
                    # Попытка отредактировать медиа (если тип совпадает и есть file_id)
                    input_media_for_edit = None
                    current_caption = final_base_text_for_display if final_base_text_for_display else None
                    if media_type == "photo" and message_to_update.photo:
                        input_media_for_edit = InputMediaPhoto(media=file_id, caption=current_caption, parse_mode=ParseMode.HTML if current_caption else None, has_spoiler=spoiler)
                    elif media_type == "video" and message_to_update.video:
                        input_media_for_edit = InputMediaVideo(media=file_id, caption=current_caption, parse_mode=ParseMode.HTML if current_caption else None, has_spoiler=spoiler)
                    elif media_type == "animation" and message_to_update.animation:
                        input_media_for_edit = InputMediaAnimation(media=file_id, caption=current_caption, parse_mode=ParseMode.HTML if current_caption else None, has_spoiler=spoiler)
                    # Добавьте другие типы, если нужно (audio)
                    # Для audio edit_media не существует, можно только edit_message_caption

                    if input_media_for_edit:
                        try:
                            newly_sent_message_object = await message_to_update.edit_media(media=input_media_for_edit, reply_markup=reply_markup)
                            can_edit_media = True
                        except BadRequest: # Не получилось отредактировать (например, тип не совпал или др. ошибка)
                            can_edit_media = False 
                    elif media_type == "audio" and message_to_update.audio: # Аудио нельзя edit_media, только caption
                        try:
                            newly_sent_message_object = await message_to_update.edit_caption(caption=current_caption, parse_mode=ParseMode.HTML if current_caption else None, reply_markup=reply_markup)
                            can_edit_media = True # Технически это редактирование caption, но сообщение сохранено
                        except BadRequest:
                            can_edit_media = False


                if not can_edit_media:
                    if message_to_update: # Удаляем старое, если не смогли отредактировать
                        try: await context.bot.delete_message(chat_id, message_to_update.message_id)
                        except (BadRequest, TelegramError): pass
                    
                    # Отправляем новое медиа
                    caption_to_send = final_base_text_for_display if final_base_text_for_display else None
                    if media_type == "photo":
                        newly_sent_message_object = await context.bot.send_photo(chat_id, photo=file_id, caption=caption_to_send, parse_mode=ParseMode.HTML if caption_to_send else None, reply_markup=reply_markup, has_spoiler=spoiler)
                    elif media_type == "video":
                        newly_sent_message_object = await context.bot.send_video(chat_id, video=file_id, caption=caption_to_send, parse_mode=ParseMode.HTML if caption_to_send else None, reply_markup=reply_markup, has_spoiler=spoiler)
                    elif media_type == "animation":
                        newly_sent_message_object = await context.bot.send_animation(chat_id, animation=file_id, caption=caption_to_send, parse_mode=ParseMode.HTML if caption_to_send else None, reply_markup=reply_markup, has_spoiler=spoiler)
                    elif media_type == "audio":
                        newly_sent_message_object = await context.bot.send_audio(chat_id, audio=file_id, caption=caption_to_send, parse_mode=ParseMode.HTML if caption_to_send else None, reply_markup=reply_markup)
                    else:
                        newly_sent_message_object = await context.bot.send_message(chat_id, f"{final_base_text_for_display}\n(Медиа не поддерживается или ошибка)", reply_markup=reply_markup, parse_mode=ParseMode.HTML)
            
            if newly_sent_message_object and newly_sent_message_object.message_id not in final_message_ids_sent : # если это не медиагруппа, где уже добавили
                 final_message_ids_sent.append(newly_sent_message_object.message_id)

        elif final_base_text_for_display: # Только текст
            can_edit_text = False
            if message_to_update and (message_to_update.text is not None or message_to_update.caption is not None): # Можно редактировать если есть текст ИЛИ caption
                 # Если у message_to_update было медиа, edit_text не сработает. Нужно edit_caption.
                 # Но если мы здесь, значит media_content пуст, т.е. у старого сообщения не должно быть медиа для edit_text.
                 # Однако, если старое было медиа, а новое - текст, то старое надо удалить.
                if message_to_update.text is not None: # Только если старое сообщение текстовое
                    try:
                        newly_sent_message_object = await message_to_update.edit_text(final_base_text_for_display, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
                        can_edit_text = True
                    except BadRequest:
                        can_edit_text = False
                # Если can_edit_text все еще False, значит либо старое не текстовое, либо ошибка. Удаляем и шлем новое.

            if not can_edit_text:
                if message_to_update:
                    try: await context.bot.delete_message(chat_id, message_to_update.message_id)
                    except (BadRequest, TelegramError): pass
                newly_sent_message_object = await context.bot.send_message(chat_id, final_base_text_for_display, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
            
            if newly_sent_message_object: final_message_ids_sent.append(newly_sent_message_object.message_id)

        elif reply_markup: # Нет текста, нет медиа, но есть кнопки (например, фрагмент только с выбором)
            placeholder_text = "Выберите действие:" # Или другой подходящий текст
            if message_to_update: 
                try: 
                    # Если у старого сообщения был текст/медиа, лучше удалить и отправить новое только с кнопками
                    if message_to_update.text or message_to_update.photo or message_to_update.video or message_to_update.animation:
                        await message_to_update.delete()
                        newly_sent_message_object = await context.bot.send_message(chat_id, text=placeholder_text, reply_markup=reply_markup)
                    else: # Если старое сообщение тоже было только с кнопками (или пустое)
                        newly_sent_message_object = await message_to_update.edit_text(text=placeholder_text, reply_markup=reply_markup) # или edit_reply_markup
                except (BadRequest, TelegramError): 
                    try: await message_to_update.delete() # Попытка удалить если редактирование не удалось
                    except (BadRequest, TelegramError): pass
                    newly_sent_message_object = await context.bot.send_message(chat_id, text=placeholder_text, reply_markup=reply_markup)
            else:
                newly_sent_message_object = await context.bot.send_message(chat_id, text=placeholder_text, reply_markup=reply_markup)
        
        else: # Пустой фрагмент
            empty_text = "Фрагмент пуст."
            can_edit_empty = False
            if message_to_update and message_to_update.text is not None: # Только если старое сообщение текстовое
                try:
                    newly_sent_message_object = await message_to_update.edit_text(empty_text, reply_markup=reply_markup) # reply_markup может быть None
                    can_edit_empty = True
                except BadRequest:
                    can_edit_empty = False
            
            if not can_edit_empty:
                if message_to_update:
                    try: await context.bot.delete_message(chat_id, message_to_update.message_id)
                    except (BadRequest, TelegramError): pass
                newly_sent_message_object = await context.bot.send_message(chat_id, empty_text, reply_markup=reply_markup)
            
            if newly_sent_message_object: final_message_ids_sent.append(newly_sent_message_object.message_id)

        # --- Запуск задачи timed_edits ---
        # Определяем, для какого сообщения и какой его части (текст/caption) запускать редактирование
        message_to_apply_timed_edits = first_media_message_for_caption_edit if first_media_message_for_caption_edit else newly_sent_message_object

        if message_to_apply_timed_edits and edit_steps_for_text:
            # Отменяем предыдущую задачу редактирования, если она была для этого ключа
            if edit_task_key in active_edit_tasks:
                logger.info(f"render_fragment: Cancelling existing timed_edit task {edit_task_key} before starting new one.")
                active_edit_tasks[edit_task_key].cancel()
                # Ожидать завершения не будем, новая задача просто перезапишет или finally старой отработает

            is_caption_edit = (message_to_apply_timed_edits.caption is not None) or \
                              (message_to_apply_timed_edits.photo or \
                               message_to_apply_timed_edits.video or \
                               message_to_apply_timed_edits.animation or \
                               message_to_apply_timed_edits.audio) # Если есть медиа, то это caption

            # base_text_for_display уже является текстом без тегов [[...]]
            # run_timed_edits должен использовать его как основу
            text_for_timed_run = final_base_text_for_display

            logger.info(f"Scheduling timed_edits for msg {message_to_apply_timed_edits.message_id} with key {edit_task_key}. is_caption={is_caption_edit}")
            active_edit_tasks[edit_task_key] = asyncio.create_task(
                run_timed_edits_full( # Используем новую функцию
                    bot=context.bot,
                    chat_id=chat_id,
                    message_id=message_to_apply_timed_edits.message_id,
                    original_text=text_for_timed_run,
                    steps=edit_steps_for_text,
                    is_caption=is_caption_edit,
                    reply_markup_to_preserve=reply_markup,
                    task_key_to_manage=edit_task_key
                )
            )
    except Exception as e:
        logger.error(f"Error rendering fragment {fragment_id} for user {user_id}: {e}", exc_info=True)
        try:
            # Попытка удалить message_to_update, если оно не было обработано и заменено
            if message_to_update and (not newly_sent_message_object or (newly_sent_message_object and message_to_update.message_id != newly_sent_message_object.message_id)):
                try:
                    await context.bot.delete_message(chat_id, message_to_update.message_id)
                except (BadRequest, TelegramError):
                    pass # Игнорируем, если не удалось
            error_msg_obj = await context.bot.send_message(chat_id, "Произошла ошибка при отображении фрагмента.")
            final_message_ids_sent.append(error_msg_obj.message_id)
        except Exception as ie:
            logger.error(f"Critical error: Failed to even send error message to user {user_id}: {ie}")

    context.user_data[last_messages_key] = final_message_ids_sent

    # --- 5. Планирование авто-перехода ---
    auto_timer_key = f"{user_id}_{story_id}_{chat_id}" # Ключ для таймера авто-перехода
    if auto_timer_key in active_timers: # Отменяем предыдущий таймер авто-перехода (если есть)
        active_timers[auto_timer_key].cancel()
        # Удаление произойдет в finally старой задачи

    if is_auto_transition_planned and auto_transition_target_fragment_id:
        next_auto_path = current_auto_path + [fragment_id]
        context.user_data[f"auto_path_{user_id}_{story_id}_{chat_id}"] = next_auto_path
        logger.info(
            f"Scheduling auto-transition for user {user_id}, story {story_id}, chat {chat_id} "
            f"from '{fragment_id}' to '{auto_transition_target_fragment_id}' in {auto_transition_timer_delay}s. "
            f"Current auto path: {next_auto_path}"
        )
        
        # Сообщение, которое таймер попытается обновить.
        # Если timed_edits запущены, они будут редактировать это же сообщение.
        # Таймер авто-перехода должен будет "перехватить" это сообщение.
        message_id_for_timer_to_use = None
        if newly_sent_message_object : # Если было отправлено/отредактировано основное сообщение
             message_id_for_timer_to_use = newly_sent_message_object.message_id
        elif first_media_message_for_caption_edit: # Если это медиагруппа, используем первое сообщение
             message_id_for_timer_to_use = first_media_message_for_caption_edit.message_id
        elif final_message_ids_sent: # Если были отправлены только кнопки или сообщение об ошибке
            message_id_for_timer_to_use = final_message_ids_sent[0]


        active_timers[auto_timer_key] = asyncio.create_task(
            auto_transition_task(
                context=context,
                user_id=user_id,
                owner_id=owner_id,
                story_id=story_id,
                target_fragment_id=auto_transition_target_fragment_id,
                delay_seconds=auto_transition_timer_delay,
                story_data=story_data, # Передаем полные данные истории
                chat_id=chat_id,
                # message_id_to_update_by_timer должен быть ID сообщения, которое будет заменено/отредактировано на "..."
                # Это может быть то же сообщение, что и для timed_edits
                message_id_to_update_by_timer=message_id_for_timer_to_use,
                path_taken_for_auto_transition=next_auto_path
            )
        )
    else: # Авто-переход не запланирован
        if not is_auto_transition_planned: # Если это из-за цикла или отсутствия таймера
             context.user_data.pop(f"auto_path_{user_id}_{story_id}_{chat_id}", None)


async def auto_transition_task(
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    story_id: str,
    owner_id: str,
    target_fragment_id: str,
    delay_seconds: float,
    story_data: Dict[str, Any], # Принимаем полные story_data
    chat_id: int,
    message_id_to_update_by_timer: Optional[int],
    path_taken_for_auto_transition: List[str]
):
    auto_timer_key = f"{user_id}_{story_id}_{chat_id}" # Ключ для текущей задачи авто-перехода
    logger.debug(f"Auto-transition task {auto_timer_key} started for {target_fragment_id}, delay {delay_seconds}s.")

    try:
        await asyncio.sleep(delay_seconds)
        # После сна задача могла быть отменена
        logger.info(f"Auto-Timer fired for {auto_timer_key}. Transitioning to {target_fragment_id}.")

        # --- Отмена активной задачи редактирования для текущего (старого) фрагмента ---
        # Ключ задачи редактирования, которая могла быть активна для предыдущего фрагмента
        edit_task_key_to_cancel = f"edit_{user_id}_{story_id}_{chat_id}"
        if edit_task_key_to_cancel in active_edit_tasks:
            logger.info(f"Auto-Timer: Cancelling timed_edit task {edit_task_key_to_cancel} before auto-transition.")
            active_edit_tasks[edit_task_key_to_cancel].cancel()
            # Удаление из active_edit_tasks произойдет в finally блока run_timed_edits

        message_for_next_render: Optional[Message] = None
        if message_id_to_update_by_timer:
            try:
                # Редактируем сообщение на временный текст "..."
                # Это также "захватывает" объект Message для передачи в render_fragment
                temp_message = await context.bot.edit_message_text( # или edit_message_caption если это было медиа
                    chat_id=chat_id,
                    message_id=message_id_to_update_by_timer,
                    text="...",
                    reply_markup=None # Убираем кнопки на время перехода
                )
                message_for_next_render = temp_message
            except BadRequest: # Сообщение могло быть удалено или изменено
                logger.warning(f"Auto-Timer: Message {message_id_to_update_by_timer} to update with '...' was gone or not text. Sending new placeholder.")
                # Если не удалось отредактировать, отправляем новое временное
                try:
                    message_for_next_render = await context.bot.send_message(chat_id, "...")
                except Exception as send_e: # Если даже это не удалось
                    logger.error(f"Auto-Timer: Failed to send placeholder message: {send_e}")
                    # Если не можем отправить/отредактировать сообщение, дальнейший рендер может быть проблематичен.
                    # Можно просто вернуться, прервав авто-переход.
                    return
            except Exception as e:
                 logger.error(f"Auto-Timer: Error preparing message for update: {e}", exc_info=True)
                 # Попробуем отправить новое временное сообщение в качестве фолбэка
                 try:
                    message_for_next_render = await context.bot.send_message(chat_id, "...")
                 except Exception as send_e:
                    logger.error(f"Auto-Timer: Failed to send fallback placeholder message: {send_e}")
                    return # Прерываем авто-переход
        else: # Если не было ID сообщения для обновления (например, предыдущий фрагмент был пуст или только кнопки)
            try:
                message_for_next_render = await context.bot.send_message(chat_id, "...") # Временное сообщение
            except Exception as send_e:
                logger.error(f"Auto-Timer: Failed to send placeholder message when no message_id_to_update: {send_e}")
                return # Прерываем авто-переход

        # Получаем данные для нового фрагмента (текст для парсинга timed_edits)
        target_fragment_data = story_data.get("fragments", {}).get(target_fragment_id)
        if not target_fragment_data:
            logger.error(f"Auto-Timer: Target fragment {target_fragment_id} not found in story_data for story {story_id}.")
            # Можно отправить сообщение об ошибке пользователю или просто прервать
            if message_for_next_render: # Если есть временное сообщение, отредактировать его на ошибку
                try: await message_for_next_render.edit_text("Ошибка: следующий фрагмент не найден.")
                except BadRequest: await context.bot.send_message(chat_id, "Ошибка: следующий фрагмент не найден.")
            else: # Иначе отправить новое
                await context.bot.send_message(chat_id, "Ошибка: следующий фрагмент не найден.")
            return

        target_fragment_text_content = target_fragment_data.get("text", "")
        base_text_for_next_fragment = re.split(r"(\[\[[-+]\d+\]\]|\(\([-+]\d+\)\))", target_fragment_text_content, 1)[0].strip()
        edit_steps_for_next_fragment = parse_timed_edits(target_fragment_text_content)

        await render_fragment(
            context=context,
            user_id=user_id,
            owner_id=owner_id,
            story_id=story_id,
            fragment_id=target_fragment_id,
            message_to_update=message_for_next_render, # Передаем временное сообщение (или None)
            story_data=story_data,
            chat_id=chat_id,
            current_auto_path=path_taken_for_auto_transition, # Передаем текущий авто-путь
            base_text_for_display=base_text_for_next_fragment,
            edit_steps_for_text=edit_steps_for_next_fragment
        )
    except asyncio.CancelledError:
        logger.info(f"Auto-transition task {auto_timer_key} to {target_fragment_id} was cancelled.")
    except Exception as e:
        logger.error(f"Error in auto_transition_task ({auto_timer_key} to {target_fragment_id}): {e}", exc_info=True)
        try:
            await context.bot.send_message(chat_id, "Произошла ошибка во время автоматического перехода.")
        except Exception: # Если даже это не удалось
            pass
    finally:
        current_task = asyncio.current_task()
        if auto_timer_key in active_timers and active_timers[auto_timer_key] is current_task:
            del active_timers[auto_timer_key]
            logger.debug(f"Auto-transition task {auto_timer_key} removed from active_timers.")
        # Другие случаи (перезаписан, не найден) аналогичны run_timed_edits.



async def run_timed_edits_full(
    bot: Bot,  # Используем Bot для type hinting
    chat_id: int,
    message_id: int,
    original_text: str,  # Это base_text (текст до первого тега [[...]])
    steps: List[Dict],
    is_caption: bool,
    reply_markup_to_preserve: Optional[InlineKeyboardMarkup],
    task_key_to_manage: str,  # Ключ для удаления из active_edit_tasks
):
    """
    Выполняет пошаговое редактирование сообщения для полного проигрывания истории.

    original_text: Базовый текст (до тегов).
    steps: Список шагов, где каждый шаг содержит "text" для добавления/замены суффикса.
    """
    logger.debug(
        f"Starting run_timed_edits_full for msg {message_id} with key {task_key_to_manage}. "
        f"Original base text: '{original_text[:50]}...'"
    )

    # dynamic_suffix будет содержать часть текста, которая изменяется после original_text
    dynamic_suffix = ""
    current_full_text = original_text.strip() # Начальный текст - это просто базовый текст

    # Первоначальная отправка/редактирование может быть уже сделана в render_fragment.
    # Эта функция только применяет *последующие* правки.
    # Если original_text пуст, а первый шаг - это "-", то суффикс станет этим текстом.

    try:
        for i, step in enumerate(steps):
            await asyncio.sleep(step["delay"])  # Может вызвать CancelledError

            step_text_segment = step.get("text", "")

            if step["mode"] == "+":
                if dynamic_suffix and step_text_segment: # Добавляем пробел, если уже есть суффикс и добавляемый текст не пуст
                    dynamic_suffix += " " + step_text_segment
                elif step_text_segment: # Если суффикса не было, или добавляемый текст не пуст
                    dynamic_suffix += step_text_segment
            elif step["mode"] == "-":
                dynamic_suffix = step_text_segment # Заменяем весь суффикс

            # Собираем полный текст для отображения
            if step["mode"] == "-":
                dynamic_suffix = step_text_segment
                current_full_text = dynamic_suffix
                original_text = ""  # 💥 Это ключевой момент!
            elif original_text.strip() and dynamic_suffix:
                current_full_text = original_text.rstrip() + " " + dynamic_suffix
            elif dynamic_suffix: # Если базовый текст пустой
                current_full_text = dynamic_suffix
            else: # Если и суффикс пустой (например, после [[-]] без текста)
                current_full_text = original_text.strip()
            
            # На случай, если и original_text и dynamic_suffix пусты
            if not current_full_text.strip() and original_text.strip(): # Если все стало пустым, но был ориг. текст, оставим его
                 current_full_text = original_text.strip()
            elif not current_full_text.strip(): # Если все действительно пусто
                 current_full_text = " " # Отправка пустого сообщения может вызвать ошибку, отправляем пробел

            logger.debug(f"Step {i+1} for msg {message_id}: mode='{step['mode']}', segment='{step_text_segment[:30]}...'. New full text: '{current_full_text[:50]}...'")

            try:
                if is_caption:
                    await bot.edit_message_caption(
                        chat_id=chat_id,
                        message_id=message_id,
                        caption=current_full_text,
                        parse_mode=ParseMode.HTML,
                        reply_markup=reply_markup_to_preserve,
                    )
                else:
                    await bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=message_id,
                        text=current_full_text,
                        parse_mode=ParseMode.HTML,
                        reply_markup=reply_markup_to_preserve,
                    )
            except BadRequest as e:
                if "message to edit not found" in str(e).lower() or \
                   "message is not modified" in str(e).lower() or \
                   "message can't be edited" in str(e).lower():
                    logger.warning(
                        f"run_timed_edits_full: Message {message_id} not found, not modified, or can't be edited. "
                        f"Stopping edits for task {task_key_to_manage}. Error: {e}"
                    )
                elif "message text is empty" in str(e).lower() and current_full_text == " ":
                    logger.warning(
                        f"run_timed_edits_full: Attempted to edit to empty message for msg {message_id}. "
                        f"Consider handling this case if a truly empty message is intended."
                    )
                else:
                    logger.error(
                        f"run_timed_edits_full: BadRequest during API call for msg {message_id}, task {task_key_to_manage}. Error: {e}"
                    )
                break  # Прерываем цикл редактирования при ошибке API
            except TelegramError as e:
                logger.error(
                    f"run_timed_edits_full: TelegramError during API call for msg {message_id}, task {task_key_to_manage}. Error: {e}"
                )
                break
            except Exception as e:
                logger.error(
                    f"run_timed_edits_full: Unexpected error during API call for msg {message_id}, task {task_key_to_manage}. Error: {e}",
                    exc_info=True
                )
                break

    except asyncio.CancelledError:
        logger.info(f"run_timed_edits_full task {task_key_to_manage} (msg: {message_id}) was cancelled.")
    except Exception as e:
        logger.error(
            f"Unexpected error in run_timed_edits_full task {task_key_to_manage} (msg: {message_id}): {e}",
            exc_info=True
        )
    finally:
        # Удаляем задачу из активных таймеров после завершения или отмены
        # Убедимся, что active_edit_tasks доступен в этой области видимости (глобальный или переданный)
        global active_edit_tasks # Если active_edit_tasks - глобальная переменная
        
        current_async_task = asyncio.current_task() # Получаем текущую задачу asyncio
        if task_key_to_manage in active_edit_tasks and active_edit_tasks[task_key_to_manage] is current_async_task:
            del active_edit_tasks[task_key_to_manage]
            logger.debug(f"run_timed_edits_full task {task_key_to_manage} removed from active_edit_tasks.")
        elif task_key_to_manage in active_edit_tasks:
            logger.warning(
                f"run_timed_edits_full task {task_key_to_manage} was in active_edit_tasks "
                f"but was not the current task upon completion. This might indicate a quick restart or overwrite."
            )
        else:
            logger.debug(
                f"run_timed_edits_full task {task_key_to_manage} not found in active_edit_tasks upon completion "
                f"(possibly already removed, cancelled and removed by new task, or never added)."
            )











async def finish_story_creation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Сохраняет созданную историю в JSON и завершает диалог."""
    query = update.callback_query
    # await query.answer() # Ответ уже был в add_content_callback_handler

    if not context.user_data.get('story_id'):
        await query.edit_message_text("Не найдено активной сессии создания истории.")
        return ConversationHandler.END # Просто выходим

    user_id_str = context.user_data.get('user_id_str')
    story_data = context.user_data.get('current_story')
    story_id = context.user_data.get('story_id')
    story_title = story_data.get('title', 'Без названия') if story_data else 'Без названия'

    # Финальное сохранение перед очисткой
    save_current_story_from_context(context)

    logger.info(f"Завершение создания истории '{story_title}' (ID: {story_id}) пользователем {user_id_str}.")

    # Проверяем, что есть хотя бы начальный фрагмент
    if not user_id_str or not story_data or not story_id or "main_1" not in story_data.get("fragments", {}):
        error_text = "Ошибка: Не удалось сохранить историю. Убедитесь, что вы добавили контент хотя бы для начального фрагмента ('1')."
        if query:
            await query.edit_message_text(error_text)
        # Очищаем user_data в любом случае при ошибке завершения
        context.user_data.clear()
        return ConversationHandler.END

    success_text = f"История '{story_title}' успешно сохранена!"
    if query:
         await query.edit_message_text(success_text)
    else: # На случай если finish вызывается не из callback (хотя сейчас это не так)
        await context.bot.send_message(update.effective_chat.id, success_text)

    # Очищаем временные данные пользователя ПОСЛЕ успешного сохранения
    context.user_data.clear()
    logger.info(f"user_data для {user_id_str} очищен после сохранения истории.")


    # Можно снова показать главное меню после сохранения
    keyboard = [
        [InlineKeyboardButton("🌠Создать ещё историю🌠", callback_data='create_story_start')],
        [InlineKeyboardButton("✏️Редактировать истории✏️", callback_data='view_stories')],
        [InlineKeyboardButton("🌟Посмотреть общие истории🌟", callback_data='public_stories')], # Добавляем и сюда
        [InlineKeyboardButton("🌃 В главное меню", callback_data='finish_story')],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await context.bot.send_message(
         chat_id=update.effective_chat.id,
         text='Что делаем дальше?',
         reply_markup=reply_markup
    )

    return ConversationHandler.END # Завершаем диалог создания

async def cancel_creation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отменяет процесс создания истории."""
    user_id = update.effective_user.id
    logger.info(f"Пользователь {user_id} отменил создание истории через /cancel.")
    await update.message.reply_text('Создание истории отменено. Временные данные очищены.', reply_markup=ReplyKeyboardRemove())

    # Очищаем временные данные
    context.user_data.clear()

    # Показываем главное меню
    await start(update, context) # Используем start для отображения меню

    return ConversationHandler.END


# --- Основная функция ---

#==========================================================================
#GPT

async def neural_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    question = update.message.text
    user_id = update.effective_user.id
    story_id = context.user_data.get('neuro_story_id')
    fragment_id = context.user_data.get('neuro_fragment_id')
    full_story = context.user_data.get('neuro_full_story')

    if not all([story_id, fragment_id, full_story]):
        await update.message.reply_text("Не удалось получить контекст истории. Попробуйте снова.")
        return ConversationHandler.END

    waiting_message = await update.message.reply_text("⌛ Думаю над ответом...")

    async def background_answer():
        try:
            # Логирование
            logging.info(f"User {user_id} asked: {question}")
            logging.info(f"Fragment ID: {fragment_id}")
            logging.info(f"Full story: {full_story}")

            response_text = await generate_gemini_response(
                query=question,
                full_story=full_story,
                current_fragment=fragment_id
            )

            # Редактируем ожидание — это основной ответ
            await waiting_message.edit_text(response_text)

            # Затем отправляем дополнительное сообщение с кнопкой
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("✏️Назад к списку", callback_data='view_stories')]
            ])
            await update.message.reply_text(
                "Введите ещё один запрос или вернитесь к списку историй",
                reply_markup=keyboard
            )
            return NEURAL_INPUT    
        except asyncio.CancelledError:
            logger.info(f"Фоновая задача ответа нейросети для пользователя {user_id} была отменена.")
            try:
                await waiting_message.edit_text("Действие было отменено.")
            except Exception as e_edit:
                logger.warning(f"Не удалось изменить сообщение ожидания при отмене (neural_handler): {e_edit}")
        except Exception as e:
            logger.error(f"Ошибка при генерации ответа нейросети: {e}")
            try:
                await waiting_message.edit_text("⚠️ Не удалось получить ответ от нейросети. Попробуйте позже.")
            except Exception as e_edit:
                logger.warning(f"Не удалось изменить сообщение ожидания при ошибке (neural_handler): {e_edit}")

    task = asyncio.create_task(background_answer())
    user_tasks_set = context.user_data.setdefault('user_tasks', set())
    user_tasks_set.add(task)
    task.add_done_callback(lambda t: _remove_task_from_context(t, context.user_data))

    return NEURAL_INPUT

async def generate_gemini_response(query, full_story, current_fragment):
    """
    Генерирует ответ от Gemini для визуальной новеллы.
    Вход:
        - query: запрос пользователя (что нужно сделать)
        - full_story: полная версия истории
        - current_fragment: текущий обрабатываемый фрагмент
    """
    system_instruction = (
        "Ты — нейросеть, помогающая писать визуальную новеллу или интерактивную историю.\n"
        "В ответах не используй формат JSON, код или служебную разметку — только чистый текст, как для обычного пользователя.\n\n"
        f"Вот полная история:\n{full_story}\n\n"            
    )
    context = (
        f"Вот текущий фрагмент, над которым рабоатет пользователь:\n{current_fragment}\n\n"
        f"Вот, что хочет пользователь:\n{query}\n\n"
        "Помоги с выполнением запроса, действуя как соавтор. Пиши живо, с воображением."
    )

    try:
        google_search_tool = Tool(google_search=GoogleSearch())
        response = await client.aio.models.generate_content(
            model='gemini-2.5-flash',
            contents=context,
            config=types.GenerateContentConfig(
                system_instruction=system_instruction,                
                temperature=1.4,
                top_p=0.95,
                top_k=25,
                max_output_tokens=7000,
                tools=[google_search_tool],
                safety_settings=[
                    types.SafetySetting(category='HARM_CATEGORY_HATE_SPEECH', threshold='BLOCK_NONE'),
                    types.SafetySetting(category='HARM_CATEGORY_HARASSMENT', threshold='BLOCK_NONE'),
                    types.SafetySetting(category='HARM_CATEGORY_SEXUALLY_EXPLICIT', threshold='BLOCK_NONE'),
                    types.SafetySetting(category='HARM_CATEGORY_DANGEROUS_CONTENT', threshold='BLOCK_NONE')
                ]
            )
        )

        if response.candidates and response.candidates[0].content.parts:
            return "".join(
                part.text for part in response.candidates[0].content.parts
                if part.text and not getattr(part, "thought", False)
            ).strip()
        else:
            return "Извините, я не смогла придумать подходящий ответ."
    except Exception as e:
        logger.error("Ошибка при генерации ответа от Gemini: %s", e)
        return "Произошла ошибка. Попробуйте позже."


async def generate_gemini_fragment(user_id, story_id, fragment_id):

    story = load_user_story(user_id, story_id)

    if not story:
        return "История не найдена."

    title = story.get("title", "Без названия")
    fragments = story.get("fragments", {})

    system_instruction = (
        "Ты — нейросеть, создающая интерактивные фрагменты для текстовых историй для телеграм-бота в строго заданном JSON-формате.\n"
        "Каждый фрагмент должен содержать следующие поля:\n"
        "- 'text': основной текст фрагмента (от третьего лица, описывает действия и окружение персонажа)\n"
        "- 'media': список ссылок на изображения (может быть пустым)\n"
        "- 'choices': список словарей вида [{\"text\": \"текст кнопки\", \"target\": \"название_следующего_фрагмента\"}]\n\n"
        "Формат ответа: только JSON-фрагмент. Без пояснений, без обёрток, без текста вне JSON.\n"
        "Крайне важное условие: максимальная длина текста кнопки (text) — 25 символов, значения target — 15. Лучше использовать меньше символов, особенно для значения. Это нужно для того чтобы не превысить максимальную длину битов которые можно передать в кнопку телеграм. Значение не должно иметь пробелы и максимум одно нижнее подчёркивание перед цифрой указывающей номер данного события в той или иной ветке. Только латинские буквы или кириллицу, а также цифры.\n"
        "Любой фрагмент может ссылаться на любой иной старый фрагмент через choices. Либо сослаться на новый, ещё не созданный, чтобы продолжить историю. Максимальное число выборов на один фрагмент — 10.\n"
        "Для удобства нумеруй события одной ветки, например GoToForest_1, GoToForest_2 и т.д.\n"
    )

    context = (
        f"История называется: {title}\n"
        f"Существующие фрагменты:\n"
        f"{json.dumps(fragments, ensure_ascii=False, indent=2)}\n\n"
        f"Нужно сгенерировать новый фрагмент с именем '{fragment_id}' в таком же формате.\n"
        f"Убедись, что этот фрагмент логически связан с предыдущими.\n"
        f"Если на него есть ссылка из других фрагментов — развей сюжет логично.\n"
        f"У нового фрагмента должно быть от 1 до 10 choices, минимум один.\n"        
    )

    try:
        response = await client.aio.models.generate_content(
            model='gemini-2.5-flash',
            contents=context,
            config=types.GenerateContentConfig(
                system_instruction=system_instruction,
                temperature=1.2,
                top_p=0.95,
                top_k=25,
                safety_settings=[
                    types.SafetySetting(category='HARM_CATEGORY_HATE_SPEECH', threshold='BLOCK_NONE'),
                    types.SafetySetting(category='HARM_CATEGORY_HARASSMENT', threshold='BLOCK_NONE'),
                    types.SafetySetting(category='HARM_CATEGORY_SEXUALLY_EXPLICIT', threshold='BLOCK_NONE'),
                    types.SafetySetting(category='HARM_CATEGORY_DANGEROUS_CONTENT', threshold='BLOCK_NONE')
                ]
            )
        )

        if response.candidates and response.candidates[0].content.parts:
            raw_text = "".join(
                part.text for part in response.candidates[0].content.parts
                if part.text and not getattr(part, "thought", False)
            ).strip()

            # Попытка вычленить JSON из строки
            try:
                start = raw_text.find('{')
                end = raw_text.rfind('}') + 1
                fragment_json_str = raw_text[start:end]
                fragment_data = json.loads(fragment_json_str)

                # Оборачиваем в словарь с нужным ключом
                return json.dumps({fragment_id: fragment_data}, ensure_ascii=False, indent=4)
            except Exception as e:
                logger.warning(f"Не удалось разобрать JSON из ответа: {raw_text}")
                return f"Ошибка при обработке ответа: {e}"

        return "Gemini не вернул результат."
    except Exception as e:
        logger.error("Ошибка при генерации ответа от Gemini: %s", e)
        return "Произошла ошибка. Попробуйте позже."


async def generate_neural_story_full(query):
    """
    Генерирует ответ от нейросети для визуальной новеллы.
    Вход:
        - query: тема истории
    Возвращает:
        - JSON в формате интерактивной истории
    """

    system_instruction = (
        "Ты — нейросеть, создающая интерактивные текстовые истории в формате JSON для Telegram-бота. Истории имеют разветвлённую структуру с учётом пользовательских атрибутов. Вот строгие правила, которым ты должен следовать:\n\n"
        
        "1. Структура JSON:\n"
        "   - Ключ \"title\": строка с названием истории.\n"
        "   - Ключ \"fragments\": словарь фрагментов, где каждый ключ — уникальное имя фрагмента (например, main_1, rest_2).\n"
        "   - Первый фрагмент ВСЕГДА должен иметь ключ 'main_1'.\n"
        "   - Имена фрагментов: максимум 15 символов, разрешены кириллица, латиница и цифры, одно подчёркивание перед числом в конце — допустимо. Примеры: fight_1, сон_3, market_5.\n"
        "   - Имена фрагментов должны логично отражать суть сцены.\n"
        
        "2. Каждый фрагмент содержит:\n"
        "   - \"text\": текст фрагмента, который увидит пользователь. Внутри текста можно использовать вставку атрибутов в формате {{атрибут}} (например: 'Ваша сила: {{сила}}').\n"
        "   - \"media\": всегда указывай пустой список: []\n"
        "   - \"choices\": список вариантов (не более 10), каждый из которых — объект со следующими полями:\n"
        "       - \"text\": подпись кнопки (до 25 символов).\n"
        "       - \"target\": имя следующего фрагмента, на который ведёт выбор (обязательно должен существовать, без пробелов, максимум одно подчёркивание перед номером).\n"
        "       - \"effects\": список эффектов (опционально). Каждый эффект — объект с:\n"
        "           - \"stat\": имя атрибута (например, сила, мотивация, письмо и т.д.).\n"
        "           - \"value\": может быть:\n"
        "               - числом без знака (например, 3) — установить значение атрибута в 3;\n"
        "               - со знаком + или - (например, +2, -5) — увеличить или уменьшить значение атрибута;\n"
        "               - проверкой: >N, <N или =N — доступность выбора зависит от проверки. Например, '>5' означает, что выбор будет доступен только если значение атрибута больше 5.\n"
        "           - \"hide\": true или false. Если true, то:\n"
        "               - пользователь не увидит изменения атрибутов;\n"
        "               - кнопка скрыта, если не проходят проверки (например, сила < 5).\n"
        "             Если false:\n"
        "               - пользователь видит сообщение об изменении атрибутов;\n"
        "               - кнопка отображается всегда, но недоступна при несоответствии условий (можно нажимать несколько раз, пока не наберётся нужное значение).\n"

        "3. Поведение эффектов с проверками:\n"
        "   - Эффекты применяются СТРОГО ПО ПОРЯДКУ. Сначала начисления, потом проверки. Это позволяет реализовать накопительные действия (например, жать на кнопку, чтобы набрать силу).\n"
        "   - Если сначала идёт проверка (>5), а потом начисление (+2), то кнопка НЕ БУДЕТ работать до выполнения условия.\n"

        "4. Общие требования:\n"
        "   - История должна быть логичной, интересной, с разветвлениями и вариативностью.\n"
        "   - Разные фрагменты могут вести к одним и тем же целям.\n"
        "   - Не создавай 'мертвые' переходы: каждый target должен вести к реально существующему фрагменту (если пользователь не просил иначе).\n"
        "   - Разрешается делать незавершённые истории, если пользователь захочет их дописать позже, но только если явно указано.\n"
        "   - Придерживайся указанного количества фрагментов или вариативно близкого (например, 12-15 при просьбе «на 13 сцен»).\n"
        "   - Не выходи за пределы JSON — никакого текста вне структуры, пояснений или комментирования.\n\n"

        "ВЫВОДИ ТОЛЬКО JSON. Пример структуры:\n"
        "{\n"
        "  \"title\": \"Пример истории\",\n"
        "  \"fragments\": {\n"
        "    \"main_1\": {\n"
        "      \"text\": \"Вы проснулись утром. Ваша сила: {{сила}}\",\n"
        "      \"media\": [],\n"
        "      \"choices\": [\n"
        "        {\n"
        "          \"text\": \"Встать с кровати\",\n"
        "          \"target\": \"start_day_2\",\n"
        "          \"effects\": [\n"
        "            {\"stat\": \"сила\", \"value\": \"+2\", \"hide\": false}\n"
        "          ]\n"
        "        },\n"
        "        {\n"
        "          \"text\": \"Полежать ещё\",\n"
        "          \"target\": \"lazy_2\",\n"
        "          \"effects\": [\n"
        "            {\"stat\": \"мотивация\", \"value\": \"-1\", \"hide\": true}\n"
        "          ]\n"
        "        }\n"
        "      ]\n"
        "    },\n"
        "    ...\n"
        "  }\n"
        "}"
    )

    context = (
        f"Тема истории: {query}\n"
        "Пожалуйста, создай законченную (или логично начатую) историю с богатой вариативностью и фрагментами, связанными между собой.\n"
        "Начни с 'main_1'. Следи за логикой атрибутов и используй 'effects' и проверки, чтобы усилить погружение."
    )
    try:
        google_search_tool = Tool(google_search=GoogleSearch())
        response = await client.aio.models.generate_content(
            model='gemini-2.5-flash',
            contents=context,
            config=types.GenerateContentConfig(
                system_instruction=system_instruction,                
                temperature=1.7,
                top_p=0.95,
                top_k=25,
                tools=[google_search_tool],
                safety_settings=[
                    types.SafetySetting(category='HARM_CATEGORY_HATE_SPEECH', threshold='BLOCK_NONE'),
                    types.SafetySetting(category='HARM_CATEGORY_HARASSMENT', threshold='BLOCK_NONE'),
                    types.SafetySetting(category='HARM_CATEGORY_SEXUALLY_EXPLICIT', threshold='BLOCK_NONE'),
                    types.SafetySetting(category='HARM_CATEGORY_DANGEROUS_CONTENT', threshold='BLOCK_NONE')
                ]
            )
        )

        if response.candidates and response.candidates[0].content.parts:
            return "".join(
                part.text for part in response.candidates[0].content.parts
                if part.text and not getattr(part, "thought", False)
            ).strip()
        else:
            return "Извините, я не смогла придумать подходящий ответ."
    except Exception as e:
        logger.error("Ошибка при генерации ответа от Gemini: %s", e)
        return "Произошла ошибка. Попробуйте позже."



async def generate_neural_story(query):
    """
    Генерирует ответ от нейросети для визуальной новеллы.
    Вход:
        - query: тема истории
    Возвращает:
        - JSON в формате интерактивной истории
    """

    system_instruction = (
        "Ты — нейросеть, создающая интерактивные текстовые истории для телеграм-бота в строго заданном JSON-формате. Ниже приведены правила структуры:\n\n"
        "1. История представляется в виде JSON с двумя ключами: \"title\" (название истории) и \"fragments\" (словарь фрагментов истории). Первый фрагмент всегда строго main_1, остальные имеют любое название, но не превышающее длину 17 символов, это крайне важно. Так же фрагменты относящиеся к одной логической цепочке событий(к одной ветке), имеют в конце число через нижнее подчёркивание. Например GoToForest_1, GoToForest_2 и тд.\n"
        "2. Каждый фрагмент в \"fragments\" содержит:\n"
        "   - \"text\": основной текст для показа пользователю.\n"
        "   - \"media\": массив с медиа (всегда указывай его пустым, поскольку ты не можешь добавлять изображения).\n"
        "   - \"choices\": список объектов с ключами:\n"
        "       - \"text\": надпись на кнопке (максимум 25 символов);\n"
        "       - \"target\": имя следующего фрагмента (максимум 20 символов, без пробелов, допускается одно нижнее подчёркивание перед номером ветки, только латиница/кириллица и цифры).\n"
        "Крайне важное условие: значение \"target\" не должно иметь пробелы и максимум одно нижнее подчёркивание перед цифрой, указывающей номер данного события в той или иной ветке.\n"
        "Любой фрагмент может ссылаться на любой иной фрагмент через choices. Максимальное число выборов на один фрагмент — 10.\n"
        "Для удобства нумеруй логически связанные события одной ветки, например GoToForest_1, GoToForest_2 и т.д.\n"
        "3. Внутри \"text\" можно использовать спец-тэги:\n"
        "   - [[+N]] — где N — число в секундах (принимаются исключительно целые числа больше трёх). Это вставка: текст до тега сохраняется, и к нему добавляется текст, следующий за ним и до следующего тега или до конца текста.\n"
        "4. Названия фрагментов и кнопок должны быть уникальны, понятны и соответствовать смыслу происходящего.\n"
        "5. История может быть с юмором, элементами фэнтези или драмы, но всегда логична и последовательна.\n"
        "6. Выводи только JSON, без лишнего текста, комментариев и пояснений. Результат должен быть валидным JSON с ключами \"title\" и \"fragments\".\n\n"
        "Будь внимателен. Нарушение структуры приведёт к невозможности обработки истории в Telegram-боте."
        "Пользователь укажет тебе в запросе желаемое число фрагментов(fragments) истории, ориентируйся на него и сгенерируй связную законченную историю без ссылок на пустые не созданные фрагменты, но при этом достаточно варьиативную, ветвистую и интересную для прохождения."
        "История может быть не законченной и ссылаться на пустые фрагменты, в таком случае их будет генерировать иная функция."
    )

    context = (
        f"Тема истории: {query}\n\n"
        "Сгенерируй историю по этой теме, начиная с первого фрагмента с именем 'main_1'."
    )
    try:
        google_search_tool = Tool(google_search=GoogleSearch())
        response = await client.aio.models.generate_content(
            model='gemini-2.5-flash',
            contents=context,
            config=types.GenerateContentConfig(
                system_instruction=system_instruction,                
                temperature=1.7,
                top_p=0.95,
                top_k=25,
                tools=[google_search_tool],
                safety_settings=[
                    types.SafetySetting(category='HARM_CATEGORY_HATE_SPEECH', threshold='BLOCK_NONE'),
                    types.SafetySetting(category='HARM_CATEGORY_HARASSMENT', threshold='BLOCK_NONE'),
                    types.SafetySetting(category='HARM_CATEGORY_SEXUALLY_EXPLICIT', threshold='BLOCK_NONE'),
                    types.SafetySetting(category='HARM_CATEGORY_DANGEROUS_CONTENT', threshold='BLOCK_NONE')
                ]
            )
        )

        if response.candidates and response.candidates[0].content.parts:
            return "".join(
                part.text for part in response.candidates[0].content.parts
                if part.text and not getattr(part, "thought", False)
            ).strip()
        else:
            return "Извините, я не смогла придумать подходящий ответ."
    except Exception as e:
        logger.error("Ошибка при генерации ответа от Gemini: %s", e)
        return "Произошла ошибка. Попробуйте позже."







async def delete_last(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.message.chat_id)
    replied_message = update.message.reply_to_message  # Проверяем, есть ли reply

    if replied_message and replied_message.from_user.id == context.bot.id:
        # Удаляем сообщение, на которое отвечает пользователь, если оно от бота
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=replied_message.message_id)
        except Exception as e:
            await update.message.reply_text("Ошибка при удалении сообщения.")
            logger.error("Ошибка при удалении сообщения: %s", e)
    elif chat_id in bot_message_ids and bot_message_ids[chat_id]:
        # Удаляем последнее отправленное ботом сообщение из списка
        try:
            message_id = bot_message_ids[chat_id].pop()
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)

            if not bot_message_ids[chat_id]:  # Если очередь пуста, удаляем запись
                del bot_message_ids[chat_id]
        except Exception as e:
            await update.message.reply_text("Ошибка при удалении сообщения.")
            logger.error("Ошибка при удалении сообщения: %s", e)
    else:
        await update.message.reply_text("Нет сообщений для удаления.")



def main() -> None:
    """Запуск бота."""


    application = Application.builder().token(BOT_TOKEN).build()

    # Определяем состояния (убедитесь, что все константы импортированы/определены)
    # ASK_TITLE, ADD_CONTENT, ASK_CONTINUE_TEXT, ASK_BRANCH_TEXT, EDIT_STORY_MAP, ASK_LINK_TEXT, SELECT_LINK_TARGET = range(7) # Пример

    # 1. Создаем ConversationHandler для рассылки
    send_conv_handler = ConversationHandler(
        entry_points=[CommandHandler('startsend', startsend_command)],
        states={
            COLLECTING_MEDIA: [
                # Команды должны идти первыми, чтобы они не считались за медиа
                CommandHandler('nsend', endsend_command),
                CommandHandler('cancelsend', cancelsend_command),
                
                # Обработчик для сбора любых сообщений (текст, фото, видео и т.д.)
                MessageHandler(filters.ALL & ~filters.COMMAND, collect_media_handler)
            ]
        },
        fallbacks=[
            CommandHandler('cancelsend', cancelsend_command),
            CommandHandler('start', start) # Позволяет прервать рассылку командой /start
        ],
        # allow_reentry=True # Не обязательно, но может быть полезно
    )

    # 2. ДОБАВЛЯЕМ НАШ НОВЫЙ ОБРАБОТЧИК В ПРИЛОЖЕНИЕ (ПЕРЕД ОСНОВНЫМ!)
    application.add_handler(send_conv_handler)
    application.add_handler(InlineQueryHandler(inlinequery))   
    conv_handler = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(button_handler, pattern='^create_story_start$'),
            CallbackQueryHandler(button_handler, pattern=r'^edit_story_\d+_[\w-]+$'), # Обрабатывается в button_handler
            CallbackQueryHandler(button_handler, pattern=r'^view_stories$'),
            CallbackQueryHandler(button_handler, pattern=r'^e_f_[\w]+_[\w\.-]+$'), # Навигация по карте к фрагменту
            CallbackQueryHandler(button_handler, pattern=r'^goto_[\w\.-]+$'),
            CallbackQueryHandler(delete_message_callback, pattern="^delete_this_message$"),   
            CallbackQueryHandler(handle_coop_add, pattern=r"^coop_add_"), 
            CallbackQueryHandler(handle_coop_remove, pattern=r"^coop_remove_"),   
            CommandHandler('adminupload', admin_upload_command),                # Переход к фрагменту по кнопке выбора
            CommandHandler('start', start),
        ],
        states={
            COOP_ADD_USER: [
                MessageHandler(filters.TEXT | filters.FORWARDED, receive_coop_user_id),
                CommandHandler("cancel", cancel_coop_add),
                CommandHandler('restart', restart)
            ],
            COOP_DELETE_USER: [
                MessageHandler(filters.TEXT | filters.FORWARDED, receive_coop_user_id_to_remove),
                CommandHandler("cancel", cancel_coop_add),
                CommandHandler('restart', restart)
            ],
            ADMIN_UPLOAD: [
                MessageHandler(filters.Document.ALL & ~filters.COMMAND, handle_admin_json_file)
            ],            
            ASK_TITLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_title_handler)],
            ADD_CONTENT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_content_handler),
                MessageHandler(filters.PHOTO | filters.VIDEO | filters.ANIMATION | filters.AUDIO, add_content_handler),
                # CallbackQueryHandler обрабатывает ВСЕ нажатия кнопок в этом состоянии
                # Добавляем обработку новых паттернов
                CallbackQueryHandler(confirm_replace_handler, pattern=r"^(confirm_replace|cancel_replace)"),                
                CallbackQueryHandler(add_content_callback_handler, pattern='^(continue_linear|add_branch|link_to_previous|finish_story)$'),
                CallbackQueryHandler(button_handler, pattern=r'^show_branches_'), 
                CallbackQueryHandler(button_handler, pattern=r'^neurohelper_'),                                
                CallbackQueryHandler(handle_edit_choice_start, pattern=r'^edit_choice_start_[\w\.-]+$'), # <<< НОВЫЙ МАРШРУТ
                CallbackQueryHandler(button_handler, pattern=r'^preview_fragment_[\w\.-]+$'),                
                CallbackQueryHandler(button_handler, pattern=r'^edit_story_'), # Кнопка карты 
                CallbackQueryHandler(button_handler, pattern=r'^goto_'), # Кнопки существующих переходов
                CallbackQueryHandler(lambda u, c: c.answer(), pattern='^noop$'),
                CallbackQueryHandler(handle_prev_fragment, pattern=r'^prev_fragment_'),
                CallbackQueryHandler(button_handler, pattern='^main_menu_'),
                CallbackQueryHandler(select_choice_to_delete, pattern=r"^d_c_s_"),
                CallbackQueryHandler(dellink_cancel, pattern=r"^dellink_cancel_"),              
                CallbackQueryHandler(confirm_delete_choice, pattern=r"^c_d_c_"),
                CallbackQueryHandler(toggle_story_public_status, pattern=f"^{MAKE_PUBLIC_PREFIX}|{MAKE_PRIVATE_PREFIX}"),
                CallbackQueryHandler(button_handler, pattern=f"^{ENABLE_NEURO_MODE_PREFIX}[\w\.-]+_[\w\.-]+$"),
                CallbackQueryHandler(button_handler, pattern=f"^{DISABLE_NEURO_MODE_PREFIX}[\w\.-]+_[\w\.-]+$"),                
                CallbackQueryHandler(download_story_handler, pattern=f"^{DOWNLOAD_STORY_PREFIX}"),

                # Пустая кнопка-разделитель
                # >>> НОВЫЙ ОБРАБОТЧИК ДЛЯ ЗАПУСКА ИЗМЕНЕНИЯ ПОРЯДКА <<<
                CallbackQueryHandler(reorder_choices_start, pattern=f"^{REORDER_CHOICES_START_PREFIX}[\w\.-]+$"),
                
                # Можно добавить общий CallbackQueryHandler(add_content_callback_handler) в конец,
                # если add_content_callback_handler умеет обрабатывать все остальные случаи
                # или если button_handler обрабатывает всё, что не подошло выше.
                # Важно, чтобы более специфичные паттерны шли первыми.
            ],
            ASK_BRANCH_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_branch_text_handler)],
            ASK_CONTINUE_TEXT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, ask_continue_text_handler),
                CallbackQueryHandler(add_content_callback_handler, pattern='^back_to_fragment_actions$')
            ],
            # >>> НОВЫЕ СОСТОЯНИЯ ДЛЯ ИЗМЕНЕНИЯ ПОРЯДКА <<<
            REORDER_CHOICE_SELECT_ITEM: [
                CallbackQueryHandler(reorder_choice_select_position_prompt, pattern=f"^{REORDER_CHOICE_ITEM_PREFIX}\\d+$"),
                CallbackQueryHandler(reorder_choice_cancel, pattern=f"^{REORDER_CHOICE_CANCEL}$")
            ],
            REORDER_CHOICE_SELECT_POSITION: [
                CallbackQueryHandler(reorder_choice_execute, pattern=f"^{REORDER_CHOICE_POSITION_PREFIX}(top|up|asis|down|bottom)$"),
                CallbackQueryHandler(reorder_choice_cancel, pattern=f"^{REORDER_CHOICE_CANCEL}$")
            ],           
            ASK_NEW_BRANCH_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, ask_new_branch_name_handler),
                CallbackQueryHandler(add_content_callback_handler, pattern='^back_to_fragment_actions$')
            ],          
            ASK_LINK_TEXT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, ask_link_text_handler),
                CallbackQueryHandler(ask_link_text_handler, pattern='^link_cancel$')
            ],
            SELECT_LINK_TARGET: [
                CallbackQueryHandler(select_link_target_handler, pattern=f'^{"select_link_target_"}')
            ],
            NEURAL_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, neural_handler)
            ],
            # !!! НОВЫЕ СОСТОЯНИЯ И ИХ ОБРАБОТЧИКИ !!!
            SELECT_CHOICE_TO_EDIT: [
                 CallbackQueryHandler(handle_select_choice_to_edit, pattern=r'^edit_choice_select_\d+$'),
                 CallbackQueryHandler(handle_select_choice_to_edit, pattern=r'^edit_choice_cancel$') # Обработка отмены
            ],
            AWAITING_NEW_CHOICE_TEXT: [
                 MessageHandler(filters.TEXT & ~filters.COMMAND, handle_new_choice_text)
                 # Здесь можно добавить обработчик для /cancel, если нужно прервать ввод текста
            ],

            EDIT_STORY_MAP: [
                # Обработчик для подтверждения удаления (вызывается кнопкой 🗑️)
                CallbackQueryHandler(handle_delete_fragment_confirm, pattern=f'^{DELETE_FRAGMENT_CONFIRM_PREFIX}_'),

                # Обработчик для выполнения удаления (вызывается кнопкой "✅ Да, удалить")
                CallbackQueryHandler(handle_delete_fragment_execute, pattern=f'^{DELETE_FRAGMENT_EXECUTE_PREFIX}_'),

                # Обработчик для кнопки "❌ Нет, отмена" или обновления карты/списка
                # Он уже должен обрабатываться существующим button_handler или специфичным
                # обработчиком для edit_story_...
                CallbackQueryHandler(button_handler, pattern=r'^edit_story_map_'), # Убедитесь, что этот паттерн есть

                # Существующий обработчик для выбора фрагмента и пагинации
                CallbackQueryHandler(button_handler) # Оставьте его как общий обработчик для этого состояния
            ],
        },
        fallbacks=[
            CommandHandler('cancel', cancel_creation),
            CommandHandler('start', start),
            # Важно: Добавляем обработку /cancel и для новых состояний
            CommandHandler('cancel', cancel_creation_from_edit), # Нужна функция отмены из новых состояний
            CallbackQueryHandler(restart, pattern='^restart_callback$'),
            CommandHandler('restart', restart),
            CallbackQueryHandler(show_story_fragment, pattern=r"^play_\d+_[a-f0-9]+_[\w\d._]+$"),
            CallbackQueryHandler(handle_neuralstart_story_callback, pattern=r"^nstartstory_[\w\d]+_[\w\d]+$"),
            CallbackQueryHandler(view_stories_list, pattern="^view_neural_stories$"),
            CallbackQueryHandler(cancel_coop_add, pattern="^cancel_coop_add$"),
            CallbackQueryHandler(view_stories_list, pattern="^view_coop_stories$"),
        ],
        allow_reentry=True
    )
    application.add_handler(conv_handler)

    # Добавляем обработчики
    application.add_handler(CallbackQueryHandler(handle_inline_play, pattern=r"^inlineplay_"))
    application.add_handler(CommandHandler("start", start))
    application.add_handler(conv_handler) # Добавляем обработчик диалога
    application.add_handler(CommandHandler("nstory", handle_nstory_command))  
    application.add_handler(CommandHandler("nfullstory", handle_full_nstory_command))  

    application.add_handler(CommandHandler("nd", delete_last)) 
    application.add_handler(CommandHandler("help", mainhelp_callback))  
    application.add_handler(CallbackQueryHandler(linkhelp_callback, pattern='^linkhelp$'))
    # Добавление пользователя
    application.add_handler(CallbackQueryHandler(handle_coop_add, pattern=r"^coop_add_"))

    # Удаление пользователя
    application.add_handler(CallbackQueryHandler(handle_coop_remove, pattern=r"^coop_remove_"))

    # Открытие меню совместного редактирования
    application.add_handler(CallbackQueryHandler(build_coop_edit_keyboard, pattern=r"^coop_edit_menu_"))

    application.add_handler(CallbackQueryHandler(cancel_coop_add, pattern="^cancel_coop_add$"))
    application.add_handler(CallbackQueryHandler(handle_inline_play, pattern=r"^inlineplay_"))
    application.add_handler(CallbackQueryHandler(handle_set_vote_threshold, pattern=r"^setthreshold_"))
    application.add_handler(CallbackQueryHandler(handle_poll_vote, pattern=r"^vote_"))
    #application.add_handler(CallbackQueryHandler(handle_single_choice_selection, pattern=r"^selectsingle_"))       
    application.add_handler(CallbackQueryHandler(button_handler, pattern='^view_stories$'))
    application.add_handler(CallbackQueryHandler(view_stories_list, pattern=r'^view_stories_page_\d+$'))  
    application.add_handler(CallbackQueryHandler(view_stories_list, pattern="^view_stories_page_"))
    application.add_handler(CallbackQueryHandler(view_stories_list, pattern="^neural_stories_page_"))
    application.add_handler(CallbackQueryHandler(view_stories_list, pattern="^view_neural_stories$"))   
    application.add_handler(CallbackQueryHandler(view_stories_list, pattern="^view_coop_stories$"))       
    application.add_handler(CallbackQueryHandler(button_handler, pattern=r'^edit_story_\d+_[\w-]+$'))   # Кнопка просмотра историй
    application.add_handler(CallbackQueryHandler(button_handler, pattern=r'^p_f_\d+_[\w-]+$')) 
    application.add_handler(CallbackQueryHandler(button_handler, pattern=r'^page_info_\d+_[\w-]+$'))         
    application.add_handler(CallbackQueryHandler(button_handler, pattern=r'^e_f_[\w]+_[\w\.-]+$'))      
    application.add_handler(CallbackQueryHandler(button_handler, pattern=r'^show_map_[\w-]+$')) 
    application.add_handler(CallbackQueryHandler(button_handler, pattern=r'^mapreq_[\w-]+$'))     
    application.add_handler(CallbackQueryHandler(delete_message_callback, pattern="^delete_this_message$"))
    application.add_handler(CallbackQueryHandler(confirm_delete_story, pattern=r"^delete_story_\d+_.+"))
    application.add_handler(CallbackQueryHandler(delete_story_confirmed, pattern=r"^confirm_delete$"))    
    #application.add_handler(CallbackQueryHandler(toggle_story_public_status, pattern=f"^{MAKE_PUBLIC_PREFIX}|{MAKE_PRIVATE_PREFIX}"))
    #application.add_handler(CallbackQueryHandler(download_story_handler, pattern=f"^{DOWNLOAD_STORY_PREFIX}"))
    application.add_handler(CallbackQueryHandler(view_public_stories_list, pattern='^public_stories$'))
    application.add_handler(CallbackQueryHandler(confirm_delete_all_neural, pattern="^confirm_delete_all_neural$"))
    application.add_handler(CallbackQueryHandler(delete_all_neural_stories_firebase, pattern="^delete_all_neural_confirmed$"))                
    # Добавить сюда обработчик для кнопок вида 'play_{user_id}_start' для запуска просмотра
    # application.add_handler(CallbackQueryHandler(play_story_handler, pattern='^play_'))
    application.add_handler(CallbackQueryHandler(show_story_fragment, pattern=r"^play_\d+_[a-f0-9]+_[\w\d._]+$"))
    application.add_handler(CallbackQueryHandler(restart, pattern='^restart_callback$')) # <-- ДОБАВЛЕНО: Обработчик кнопки рестарта вне диалога    
    # Запуск бота
    application.add_handler(CallbackQueryHandler(handle_neuralstart_story_callback, pattern=r"^nstartstory_[\w\d]+_[\w\d]+$"))
    application.add_handler(CommandHandler("restart", restart)) 
    application.add_handler(CommandHandler("delete", delete_inline_stories))
    # ⬇️ Важно: обработчик любого текста вне диалога, вызывает start
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, start))

    init_firebase()
    keep_alive()#запускаем flask-сервер в отдельном потоке. Подробнее ниже...
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()
