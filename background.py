from flask import Flask, request, send_from_directory, jsonify
from threading import Thread
import os
import re
import requests
import logging

# --- КОНФИГУРАЦИЯ ---
BUILD_FOLDER = os.path.join(os.path.dirname(__file__), 'client', 'build')
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN не задан в переменных окружения")

app = Flask(__name__, static_folder=BUILD_FOLDER, static_url_path='')
logging.getLogger("httpx").setLevel(logging.WARNING) # Уменьшает спам от http запросов
logger = logging.getLogger(__name__)
# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
from concurrent.futures import ThreadPoolExecutor
# --- BREAKER STATE ---
CATBOX_BREAKER = {
    "is_dead": False,
    "failures": 0,
    "THRESHOLD": 3
}

# Используем 0x0.st как надежный фоллбэк с прямыми ссылками
POMF_BREAKER = {
    "is_dead": False,
    "failures": 0,
    "THRESHOLD": 3
}

def record_failure(breaker, name):
    breaker["failures"] += 1
    if breaker["failures"] >= breaker["THRESHOLD"]:
        breaker["is_dead"] = True
        logging.warning(f"⚠️ {name} marked as DEAD. Uploads skipped.")

def reset_breaker(breaker):
    if breaker["failures"] > 0:
        breaker["failures"] = 0
        breaker["is_dead"] = False

# --- CATBOX (ОСНОВНОЙ) ---
def upload_to_catbox_helper(filename, file_bytes):
    """Попытка загрузки на Catbox"""
    if CATBOX_BREAKER["is_dead"]:
        return None

    try:
        url = "https://catbox.moe/user/api.php"
        files = {
            'reqtype': (None, 'fileupload'),
            'userhash': (None, '1ba6da315df23e3bd01fe524c'), # Ваш хеш, если есть, или пустой для анонима
            'fileToUpload': (filename, file_bytes)
        }
        
        # User-Agent важен, некоторые хосты блокируют дефолтный python-requests
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}

        response = requests.post(url, files=files, headers=headers, timeout=(3, 30))

        if response.status_code == 200:
            result = response.text.strip()
            if result.startswith('http'):
                logging.info(f"✅ Catbox upload success: {result}")
                reset_breaker(CATBOX_BREAKER)
                return result
        
        record_failure(CATBOX_BREAKER, "Catbox")

    except Exception as e:
        logging.info(f"❌ Catbox upload failed: {e}")
        record_failure(CATBOX_BREAKER, "Catbox")
        
    return None


# --- ФУНКЦИЯ ЗАГРУЗКИ НА 0x0.st (НОВАЯ) ---
def upload_to_pomf_helper(filename, file_bytes):
    """
    Загрузка на pomf.lain.la (Fallback).
    В БД возвращается ПРЯМАЯ ссылка на файл.
    """
    if POMF_BREAKER["is_dead"]:
        logging.warning("⛔ Pomf breaker is DEAD. Upload skipped.")
        return None

    try:
        url = "https://pomf.lain.la/upload.php"

        files = {
            "files[]": (filename, file_bytes)
        }

        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"
        }

        response = requests.post(
            url,
            files=files,
            headers=headers,
            timeout=(5, 60)
        )

        if response.status_code == 200:
            data = response.json()

            if data.get("success") and data.get("files"):
                file_info = data["files"][0]
                direct_url = file_info.get("url")

                if direct_url and direct_url.startswith("http"):
                    logging.info(f"✅ Pomf upload success: {direct_url}")
                    reset_breaker(POMF_BREAKER)
                    return direct_url

        logging.warning(
            f"⚠️ Pomf bad response {response.status_code}: {response.text[:200]}"
        )
        record_failure(POMF_BREAKER, "Pomf")

    except Exception as e:
        logging.error(f"❌ Pomf upload failed: {e}")
        record_failure(POMF_BREAKER, "Pomf")

    return None

def upload_chain_helper(filename, file_bytes):
    """
    1. Catbox (основной)
    2. pomf.lain.la (fallback)
    """
    # 1. Catbox
    url = upload_to_catbox_helper(filename, file_bytes)
    if url:
        return url

    # 2. Pomf
    logging.info("🔄 Switching to Fallback service (pomf.lain.la)...")
    return upload_to_pomf_helper(filename, file_bytes)


def upload_to_telegram_helper(user_id, filename, file_bytes, mime_type, send_method, field, force_document):
    """Обертка для логики загрузки в Telegram (вынесли из upload_media для чистоты)"""
    # URL
    url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{send_method}'
    
    # Files payload
    files_kwargs = {field: (filename, file_bytes, mime_type)} if mime_type else {field: (filename, file_bytes)}
    data_payload = {'chat_id': user_id}

    try:
        response = requests.post(url, files=files_kwargs, data=data_payload, timeout=60)
        result = response.json()
        
        if not result.get('ok'):
            logging.error(f'TG Error: {result}')
            return None, None

        # Достаем file_id (логика поиска объекта)
        file_obj = result['result'].get(field)
        if isinstance(file_obj, list): file_obj = file_obj[-1]
        
        if not file_obj:
            for potential_field in ['document', 'sticker', 'video', 'audio', 'voice', 'animation']:
                if potential_field in result['result']:
                    file_obj = result['result'][potential_field]
                    if isinstance(file_obj, list): file_obj = file_obj[-1]
                    break
        
        if file_obj:
            return file_obj.get('file_id'), file_obj
            
    except Exception as e:
        logging.exception('TG Upload Exception')
    
    return None, None







def validate_fragment_name(name):
    if name == "main_1":
        return False, "Фрагмент main_1 является началом и не может быть переименован."
        
    if len(name) > 17:
        return False, "Название не должно быть длиннее 17 символов."
    if not re.match(r'^[a-zA-Zа-яА-Я0-9_]+$', name):
        return False, "Название может содержать только латиницу, кириллицу, цифры и нижнее подчеркивание. Максимум 15 символов. Например ИдтиВперёд_3"
    if name.count('_') > 1 or (name.count('_') == 1 and not re.search(r'_[0-9]+$', name)):
        return False, "Допускается только одно нижнее подчеркивание перед цифрой в конце (например, GoLeft_6)."
    return True, ""

# --- API МАРШРУТЫ ---




from flask import send_from_directory
# Импорт функции выше
# from export_service import init_html_export_settings
BASE_FILE_DIR = "files"
@app.route('/api/html/prepare/<user_id>/<story_id>', methods=['POST'])
def prepare_html(user_id, story_id):
    from novel import load_user_story # Импорт из твоего файла
    from novel import init_html_export_settings
    # 1. Загружаем саму историю
    story_data = load_user_story(user_id, story_id)
    if not story_data:
        return jsonify({"error": "История не найдена"}), 404
        
    try:
        # 2. Запускаем скачивание и создание записи в HTMLexport
        # В идеале это делать в фоновом потоке (Celery/Thread), но пока синхронно
        export_data = init_html_export_settings(user_id, story_id, story_data)
        
        return jsonify({"status": "ok", "data": export_data})
    except Exception as e:
        logging.error(f"Export error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/html/data/<user_id>/<story_id>', methods=['GET'])
def get_html_data(user_id, story_id):
    from novel import load_html_export_settings, load_user_story, ensure_assets_exist
    
    # 1. Загружаем данные
    export_data = load_html_export_settings(user_id, story_id)
    story_data = load_user_story(user_id, story_id)
    
    if not story_data:
         return jsonify(export_data)

    # --- ИЗМЕНЕНИЕ: ОТКЛЮЧАЕМ СЕРВЕРНУЮ ПРОВЕРКУ ---
    # Мы переносим эту нагрузку на клиент, чтобы показывать прогресс-бар.
    # try:
    #     _, updated_export_data = ensure_assets_exist(user_id, story_id, story_data, export_data)
    #     return jsonify(updated_export_data)
    # except Exception as e:
    #     logging.error(f"Error ensuring assets: {e}")
    #     return jsonify(export_data)
    
    # Просто возвращаем данные как есть, клиент сам проверит файлы
    return jsonify(export_data)

@app.route('/api/html/data/<user_id>/<story_id>', methods=['POST'])
def save_html_data(user_id, story_id):
    from novel import save_html_export_settings
    
    new_data = request.get_json()
    save_html_export_settings(user_id, story_id, new_data)

    return jsonify({"status": "ok"})

# Раздача скачанных файлов локально для предпросмотра
@app.route('/files/<path:filename>')
def serve_files(filename):
    response = send_from_directory(BASE_FILE_DIR, filename)
    # Разрешаем доступ к файлам из любого источника (нужно для fetch + Cache API)
    response.headers['Access-Control-Allow-Origin'] = '*'
    # Разрешаем кэширование
    response.headers['Cache-Control'] = 'public, max-age=31536000'
    return response

TYPE_TO_SYS_FOLDER = {
    "photo": "sys_backgrounds",
    "image": "sys_backgrounds",
    "video": "sys_videos",
    "animation": "sys_backgrounds",
    "document": "sys_objects",
    "audio": "sys_audio",
    "voice": "sys_audio",
    "font": "sys_fonts"  # <--- РАСКОММЕНТИРУЙ ИЛИ ДОБАВЬ ЭТУ СТРОКУ
}

    
DISPLAY_NAME_TO_SYS_FOLDER = {
    'Backgrounds': 'sys_backgrounds',
    'Characters': 'sys_characters',
    'Textures': 'sys_textures',
    'Objects': 'sys_objects',
    'Audio': 'sys_audio',
    'Fonts': 'sys_fonts',
    'Videos': 'sys_videos',
    # Добавляем маппинг "сам на себя", чтобы sys_audio оставалось sys_audio
    'sys_backgrounds': 'sys_backgrounds',
    'sys_characters': 'sys_characters',
    'sys_textures': 'sys_textures',
    'sys_objects': 'sys_objects',
    'sys_audio': 'sys_audio',
    'sys_fonts': 'sys_fonts',
    'sys_videos': 'sys_videos'
}


@app.route('/api/html/ensure_local/<user_id>/<story_id>', methods=['POST'])
def ensure_local_file_route(user_id, story_id):
    from novel import download_file_from_telegram
    
    data = request.get_json()
    file_id = data.get('file_id')
    media_type = data.get('type') 
    raw_folder_name = data.get('target_folder') # Например: "sys_audio/Music/Battle"
    extension = data.get('extension', '') 

    if not file_id or not media_type:
        return jsonify({"error": "Missing file_id or type"}), 400

    try:
        target_folder_name = None
        
        # ЛОГИКА ОПРЕДЕЛЕНИЯ ПУТИ (ИСПРАВЛЕНА)
        if raw_folder_name:
            # Разбиваем путь на части
            parts = raw_folder_name.strip('/').split('/')
            root_part = parts[0]
            
            # Проверяем, является ли корень "красивым именем" (Audio -> sys_audio)
            # Если это уже sys_audio, оно останется sys_audio благодаря обновленному словарю
            sys_root = DISPLAY_NAME_TO_SYS_FOLDER.get(root_part, root_part)
            
            # Заменяем корень на системный
            parts[0] = sys_root
            
            # Собираем путь обратно: sys_audio/Music/Battle
            target_folder_name = "/".join(parts)
        else:
            # Дефолт, если папка не указана
            target_folder_name = TYPE_TO_SYS_FOLDER.get(media_type, "sys_backgrounds")

        # Скачиваем
        relative_path = download_file_from_telegram(
            file_id, 
            media_type, 
            story_id, 
            target_folder_name=target_folder_name,
            extension=extension
        )
        
        if not relative_path:
            # Возвращаем 500, но с сообщением, чтобы клиент видел ошибку
            logging.error(f"Failed to download {file_id} to {target_folder_name}")
            return jsonify({"error": "Failed to download file from Telegram"}), 500
            
        return jsonify({"status": "ok", "local_path": relative_path})

    except Exception as e:
        logging.error(f"Ensure local error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/api/html/play-data/<user_id>/<story_id>', methods=['GET'])
def get_play_data_api(user_id, story_id):
    from novel import load_user_story, load_html_export_settings

    # 1. Загружаем логику
    story_data = load_user_story(user_id, story_id)
    if not story_data:
        return jsonify({"error": "Story not found"}), 404
        
    # 2. Загружаем визуал
    export_data = load_html_export_settings(user_id, story_id)
    
    # 3. ИСПРАВЛЕНИЕ: Убираем ensure_assets_exist
    # Мы НЕ запускаем проверку файлов на диске сервера, чтобы не задерживать старт.
    # Клиент (AssetLoader) сам решит: грузить с Catbox (быстро) или стучаться на сервер.
    
    response_data = {
        "story": story_data,
        "visuals": export_data, 
        "assets_map": {} # Отправляем пустым, клиент построит карту сам (см. HtmlGamePlayer.js)
    }
    
    return jsonify(response_data)




@app.route('/api/html_story_settings/<story_id>/<player_id>', methods=['GET', 'POST'])
def handle_html_story_progress(story_id, player_id):
    # ИМПОРТИРУЕМ НОВЫЕ ФУНКЦИИ (обратите внимание на _html_)
    from novel import save_html_story_progress, load_html_story_progress
    
    # Загрузка
    if request.method == 'GET':
        data = load_html_story_progress(story_id, player_id)
        return jsonify(data if data else {})
        
    # Сохранение
    if request.method == 'POST':
        progress_data = request.get_json()
        if not progress_data:
            return jsonify({"error": "No data provided"}), 400
            
        save_html_story_progress(story_id, player_id, progress_data)
        return jsonify({"status": "ok"})



@app.route('/api/html/progress/<story_id>/<player_id>', methods=['DELETE'])
def reset_html_progress(story_id, player_id):
    """Сброс прогресса (Новая игра)"""
    from novel import delete_html_story_progress
    delete_html_story_progress(story_id, player_id)
    return jsonify({"status": "ok", "message": "Progress deleted"})

@app.route('/api/html/saves/<story_id>/<player_id>', methods=['GET', 'POST'])
def handle_html_saves(story_id, player_id):
    from novel import get_html_save_slots, save_html_game_slot
    
    # Получить список сохранений
    if request.method == 'GET':
        saves = get_html_save_slots(story_id, player_id)
        return jsonify(saves)
    
    # Создать сохранение
    if request.method == 'POST':
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data"}), 400
        
        save_id = save_html_game_slot(story_id, player_id, data)
        return jsonify({"status": "ok", "save_id": save_id})

@app.route('/api/html/saves/<story_id>/<player_id>/<save_id>', methods=['DELETE'])
def delete_html_save(story_id, player_id, save_id):
    from novel import delete_html_save_slot
    delete_html_save_slot(story_id, player_id, save_id)
    return jsonify({"status": "ok"})


















@app.route('/api/story/<user_id_str>/<story_id>', methods=['GET'])
def get_story(user_id_str, story_id):
    # ИЗМЕНЕНИЕ: Используем load_user_story вместо load_all
    from novel import load_user_story
    story = load_user_story(user_id_str, story_id)
    
    if story:
        return jsonify(story)
    return jsonify({"error": "История не найдена"}), 404

@app.route('/api/story/<user_id_str>/<story_id>/fragment/<fragment_id>/text', methods=['POST'])
def update_fragment_text(user_id_str, story_id, fragment_id):
    # ИЗМЕНЕНИЕ: Загружаем только одну историю
    from novel import load_user_story, save_story_data
    data = request.get_json()
    new_text = data.get("text", "").strip()

    logger.info(f"new_text: {new_text}")
    
    story = load_user_story(user_id_str, story_id)
    
    # Проверка на пустоту (load_user_story возвращает {} если нет истории)
    if not story or "fragments" not in story or fragment_id not in story["fragments"]:
        return jsonify({"error": "Фрагмент не найден"}), 404

    story["fragments"][fragment_id]["text"] = new_text
    save_story_data(user_id_str, story_id, story)
    return jsonify({"status": "ok"})


@app.route('/api/story/<user_id_str>/<story_id>/fragment/<fragment_id>', methods=['DELETE'])
def delete_fragment(user_id_str, story_id, fragment_id):
    # ИЗМЕНЕНИЕ: Загружаем только одну историю
    from novel import load_user_story, save_story_data
    
    story = load_user_story(user_id_str, story_id)

    if not story or "fragments" not in story or fragment_id not in story["fragments"]:
        return jsonify({"error": "Фрагмент не найден"}), 404

    del story["fragments"][fragment_id]

    for frag_id in story["fragments"]:
        if "choices" in story["fragments"][frag_id]:
            story["fragments"][frag_id]["choices"] = [
                choice for choice in story["fragments"][frag_id]["choices"]
                if choice.get("target") != fragment_id
            ]

    save_story_data(user_id_str, story_id, story)
    return jsonify({"status": "ok"})



@app.route('/api/story/<user_id_str>/<story_id>/fragments/delete', methods=['POST'])
def delete_multiple_fragments(user_id_str, story_id):
    # ИЗМЕНЕНИЕ: Загружаем только одну историю
    from novel import load_user_story, save_story_data
    data = request.get_json()
    fragment_ids = data.get("fragment_ids", [])

    if not isinstance(fragment_ids, list):
        return jsonify({"error": "Неверный формат fragment_ids"}), 400

    story = load_user_story(user_id_str, story_id)

    if not story or "fragments" not in story:
        return jsonify({"error": "История не найдена"}), 404

    for fragment_id in fragment_ids:
        if fragment_id in story["fragments"]:
            del story["fragments"][fragment_id]

    # Удалим все ссылки на эти фрагменты из choice-ов
    for frag_id in story["fragments"]:
        if "choices" in story["fragments"][frag_id]:
            story["fragments"][frag_id]["choices"] = [
                choice for choice in story["fragments"][frag_id]["choices"]
                if choice.get("target") not in fragment_ids
            ]

    save_story_data(user_id_str, story_id, story)
    return jsonify({"status": "ok", "deleted": fragment_ids})



@app.route('/api/story/<user_id_str>/<story_id>/fragment/<old_name>/rename', methods=['POST'])
def rename_fragment(user_id_str, story_id, old_name):
    # ИЗМЕНЕНИЕ: Загружаем только одну историю
    from novel import load_user_story, save_story_data
    data = request.get_json()
    new_name = data.get("newName")

    # 🚫 Блокируем попытку переименовать main_1
    if old_name == "main_1":
        return jsonify({"error": "Нельзя переименовывать начальный фрагмент main_1"}), 400

    if not new_name:
        return jsonify({"error": "Новое имя не предоставлено"}), 400

    is_valid, error_message = validate_fragment_name(new_name)
    if not is_valid:
        return jsonify({"error": error_message}), 400

    story = load_user_story(user_id_str, story_id)
    
    if not story or "fragments" not in story:
        return jsonify({"error": "История не найдена"}), 404

    if old_name not in story["fragments"]:
        return jsonify({"error": "Исходный фрагмент не найден"}), 404

    if new_name in story["fragments"] and old_name != new_name:
        return jsonify({"error": "Фрагмент с таким именем уже существует"}), 409

    story["fragments"][new_name] = story["fragments"].pop(old_name)

    for fragment in story["fragments"].values():
        if "choices" in fragment:
            for choice in fragment["choices"]:
                if choice.get("target") == old_name:
                    choice["target"] = new_name

    save_story_data(user_id_str, story_id, story)
    return jsonify({"status": "ok", "story": story})


@app.route('/api/story/<user_id_str>/<story_id>/connect', methods=['POST'])
def connect_fragments(user_id_str, story_id):
    # ИЗМЕНЕНИЕ: Загружаем только одну историю
    from novel import load_user_story, save_story_data
    data = request.get_json()
    source_id = data.get("source")
    target_id = data.get("target")
    text = data.get("text")

    if not all([source_id, target_id, text]):
        return jsonify({"error": "Недостаточно данных"}), 400

    story = load_user_story(user_id_str, story_id)
    
    if not story or source_id not in story["fragments"] or target_id not in story["fragments"]:
        return jsonify({"error": "Фрагмент не найден"}), 404

    source_fragment = story["fragments"][source_id]
    if "choices" not in source_fragment:
        source_fragment["choices"] = []
    
    source_fragment["choices"].append({"target": target_id, "text": text})
    save_story_data(user_id_str, story_id, story)
    return jsonify({"status": "ok", "story": story})


@app.route('/api/story/<user_id_str>/<story_id>/create_and_connect', methods=['POST'])
def create_and_connect_fragment(user_id_str, story_id):
    # ИЗМЕНЕНИЕ: Загружаем только одну историю
    from novel import load_user_story, save_story_data
    data = request.get_json()
    source_id = data.get("source")
    new_name = data.get("newName")
    choice_text = data.get("choiceText")

    if not all([source_id, new_name, choice_text]):
        return jsonify({"error": "Недостаточно данных"}), 400

    is_valid, error_message = validate_fragment_name(new_name)
    if not is_valid:
        return jsonify({"error": error_message}), 400

    story = load_user_story(user_id_str, story_id)
    
    if not story or source_id not in story["fragments"]:
        return jsonify({"error": "Исходный фрагмент не найден"}), 404
        
    if new_name in story["fragments"]:
        return jsonify({"error": "Фрагмент с таким именем уже существует"}), 409

    story["fragments"][new_name] = {"text": "(пусто)", "choices": []}

    source_fragment = story["fragments"][source_id]
    if "choices" not in source_fragment:
        source_fragment["choices"] = []
    source_fragment["choices"].append({"target": new_name, "text": choice_text})

    save_story_data(user_id_str, story_id, story)
    return jsonify({"status": "ok", "story": story})

@app.route('/api/tgfile/<file_id>')
def get_telegram_file(file_id):
    if not TELEGRAM_BOT_TOKEN:
        return jsonify({'error': 'Bot token not set'}), 500

    # 1. Получаем путь к файлу (этот запрос легкий)
    try:
        getfile_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getFile"
        resp = requests.get(getfile_url, params={"file_id": file_id}, timeout=5)
        file_data = resp.json()
    except Exception as e:
        logger.error(f"TG API Error: {e}")
        return jsonify({'error': 'Telegram API timeout'}), 504

    if not file_data.get("ok"):
        return jsonify({'error': 'Invalid file_id'}), 404

    file_path = file_data["result"]["file_path"]
    file_url = f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{file_path}"
    
    # 2. Запрашиваем файл у Телеграма как поток
    tg_response = requests.get(file_url, stream=True)
    
    if tg_response.status_code != 200:
        return jsonify({'error': 'Failed to fetch file'}), 502

    # Определяем Content-Type
    content_type = tg_response.headers.get("Content-Type", "application/octet-stream")
    file_size = tg_response.headers.get("Content-Length")

    # 3. Формируем ответ с заголовками КЭШИРОВАНИЯ
    # stream_with_context нужен, чтобы Flask не держал соединение открытым дольше нужного
    from flask import stream_with_context
    
    response = app.response_class(
        stream_with_context(tg_response.iter_content(chunk_size=4096)),
        content_type=content_type
    )

    # === ГЛАВНОЕ: ЗАГОЛОВКИ ===
    # Говорим браузеру: "Сохрани этот файл у себя в кэше на 1 год (31536000 сек)"
    response.headers['Cache-Control'] = 'public, max-age=31536000, immutable'
    
    # Если известен размер, сообщаем его (важно для видео и прогресс-баров)
    if file_size:
        response.headers['Content-Length'] = file_size
        
    # Добавляем поддержку Range запросов (перемотка видео)
    # Это сложнее реализовать "в трубе", но для простых ассетов этого достаточно.

    return response


# Эндпоинт для обновления конкретного choice (связи)

@app.route('/api/story/<user_id_str>/<story_id>/choice', methods=['PUT'])
def update_choice(user_id_str, story_id):
    # ИЗМЕНЕНИЕ: Загружаем только одну историю
    from novel import load_user_story, save_story_data
    data = request.get_json()
    source_id = data.get("source")
    
    try:
        choice_index = int(data.get("choiceIndex"))
    except (ValueError, TypeError):
        return jsonify({"error": "Некорректный index"}), 400
        
    new_text = data.get("text")
    new_effects = data.get("effects")

    if source_id is None:
        return jsonify({"error": "Необходим source_id"}), 400

    story = load_user_story(user_id_str, story_id)
    
    if not story or "fragments" not in story or source_id not in story["fragments"]:
        return jsonify({"error": "Фрагмент не найден"}), 404

    source_fragment = story["fragments"][source_id]

    if "choices" not in source_fragment or len(source_fragment["choices"]) <= choice_index:
        return jsonify({"error": f"Связь не найдена (index {choice_index} out of bounds)"}), 404

    # Обновляем данные
    if new_text is not None:
        source_fragment["choices"][choice_index]["text"] = new_text
    if new_effects is not None:
        source_fragment["choices"][choice_index]["effects"] = new_effects

    save_story_data(user_id_str, story_id, story)

    return jsonify({"status": "ok", "updatedFragment": source_fragment})


@app.route('/api/story/<user_id_str>/<story_id>/choice', methods=['DELETE'])
def delete_choice(user_id_str, story_id):
    # ИЗМЕНЕНИЕ: Загружаем только одну историю
    from novel import load_user_story, save_story_data
    data = request.get_json()
    source_id = data.get("source")
    choice_index = data.get("choiceIndex")

    if source_id is None or choice_index is None:
        return jsonify({"error": "Необходим source_id и choice_index"}), 400

    story = load_user_story(user_id_str, story_id)
    
    if not story or source_id not in story["fragments"]:
        return jsonify({"error": "Фрагмент не найден"}), 404
        
    source_fragment = story["fragments"][source_id]
    if "choices" in source_fragment and len(source_fragment["choices"]) > choice_index:
        del source_fragment["choices"][choice_index]
        save_story_data(user_id_str, story_id, story)
        return jsonify({"status": "ok"})
    
    return jsonify({"error": "Связь не найдена"}), 404


@app.route('/api/story/<user_id_str>/<story_id>/positions', methods=['GET'])
def get_positions(user_id_str, story_id):
    """
    Отдает клиенту сохраненные позиции узлов для указанной истории.
    """
    from novel import load_node_positions
    positions = load_node_positions(user_id_str, story_id)
    # Если позиций нет, это не ошибка. Просто возвращаем пустой объект.
    if positions:
        return jsonify(positions)
    return jsonify({})

@app.route('/api/story/<user_id_str>/<story_id>/positions', methods=['POST'])
def save_positions(user_id_str, story_id):
    """
    Получает от клиента и сохраняет в Firebase актуальные позиции узлов.
    """
    from novel import save_node_positions
    positions = request.get_json()
    if not positions:
        return jsonify({"error": "Данные о позициях не предоставлены"}), 400
    
    save_node_positions(user_id_str, story_id, positions)
    return jsonify({"status": "ok"})

# --- React App Routing ---


import logging

# Убедитесь, что логирование настроено
logging.basicConfig(level=logging.INFO)

    

@app.route('/api/upload_media', methods=['POST'])
def upload_media():
    file = request.files.get('file')
    user_id = request.form.get('user_id')
    force_document = request.form.get('force_document') == 'true'

    if not file:
        return jsonify({'error': 'Файл не передан'}), 400
    if not user_id:
        return jsonify({'error': 'user_id не передан'}), 400

    file_bytes = file.read()
    file_size = len(file_bytes)
    
    filename = file.filename.lower()
    _, file_extension = os.path.splitext(filename)
    
    logging.info(f'Загрузка: {filename} ({file_size} байт)')

    # --- НАСТРОЙКА ТИПОВ (Оставляем ваш код определения mime/type без изменений) ---
    send_method = 'sendDocument'
    field = 'document'
    media_type = 'document'
    mime_type = None
    filename_to_send = file.filename 

    # 0. GIF FIX (Самый приоритетный блок)
    if filename.endswith('.gif'):
        # Чтобы Telegram не сжал GIF в MP4, меняем расширение на фейковое сервисное
        send_method = 'sendDocument'
        field = 'document'
        
        # Для движка мы говорим, что это animation (чтобы попало в sys_backgrounds/sys_videos)
        # или 'image', смотря как у вас настроен фронт. Обычно GIF - это animation или image.
        media_type = 'animation' 
        
        # Маскируем расширение
        filename_to_send = filename + ".gif_raw"
        mime_type = 'application/octet-stream'

    # 1. КАРТИНКИ (Остальные)
    elif filename.endswith(('.jpg', '.jpeg', '.png', '.webp', '.avif', '.svg', '.heic', '.bmp')):
        
        # Форматы, которые ВСЕГДА шлем как документы для сохранности (вектор, прозрачность, новые кодеки)
        is_complex_image = filename.endswith(('.avif', '.svg', '.heic', '.gif'))
        # PNG и WebP шлем документом, если просит юзер (force_document) или если это сложные форматы
        should_be_doc = force_document or is_complex_image

        if should_be_doc:
            if file_size > 49 * 1024 * 1024: # Лимит 50МБ для ботов
                return jsonify({'error': 'Файл слишком большой для отправки ботом.'}), 400
            
            send_method = 'sendDocument'
            field = 'document'
            media_type = 'document' 

            # Явные MIME
            if filename.endswith('.webp'): mime_type = 'image/webp'
            elif filename.endswith('.avif'): mime_type = 'image/avif'
            elif filename.endswith('.svg'): mime_type = 'image/svg+xml'
            elif filename.endswith('.png'): mime_type = 'image/png'
        else:
            # Обычные JPG/PNG -> sendPhoto (Telegram сожмет)
            send_method = 'sendPhoto'
            field = 'photo'
            media_type = 'photo'
            mime_type = None

    # 2. ВИДЕО
    elif filename.endswith(('.mp4', '.mov', '.avi', '.mkv', '.webm')):
        if filename.endswith('.webm') or force_document:
            send_method = 'sendDocument'
            field = 'document'
            media_type = 'video' 
            mime_type = 'video/webm' if filename.endswith('.webm') else None
        else:
            send_method = 'sendVideo'
            field = 'video'
            media_type = 'video'

    # 3. АУДИО
    elif filename.endswith(('.mp3', '.ogg', '.wav', '.m4a', '.flac', '.aac', '.wma')):
        send_method = 'sendAudio'
        field = 'audio'
        media_type = 'audio'
        
    # 4. ШРИФТЫ
    elif filename.endswith(('.ttf', '.otf', '.woff', '.woff2')):
        send_method = 'sendDocument'
        field = 'document'
        media_type = 'font' 
        if filename.endswith('.woff2'): mime_type = 'font/woff2'
        elif filename.endswith('.woff'): mime_type = 'font/woff'
        elif filename.endswith('.ttf'): mime_type = 'font/ttf'
        elif filename.endswith('.otf'): mime_type = 'font/otf'

    # === ПАРАЛЛЕЛЬНАЯ ЗАГРУЗКА ===
    catbox_url = None
    tg_file_id = None
    
    with ThreadPoolExecutor(max_workers=2) as executor:
        # Запускаем Telegram (обязательно)
        future_tg = executor.submit(upload_to_telegram_helper, user_id, filename_to_send, file_bytes, mime_type, send_method, field, force_document)
        
        # Запускаем Catbox (если он жив, проверка внутри хелпера)
        future_cat = executor.submit(upload_chain_helper, filename, file_bytes)
        
        try:
            # Ждем результат Telegram (критично)
            tg_file_id, _ = future_tg.result(timeout=65) 
            
            # Ждем результат Catbox. 
            # Благодаря timeout=(3, 30) внутри requests, он не зависнет навечно.
            # Если Catbox помечен как Dead, функция вернется мгновенно.
            catbox_url = future_cat.result()
        except Exception as e:
            logging.error(f"Upload thread error: {e}")

    if not tg_file_id:
         return jsonify({'error': 'Не удалось загрузить файл в Telegram'}), 500

    return jsonify({
        'file_id': tg_file_id, 
        'type': media_type, 
        'extension': file_extension,
        'direct_url': catbox_url # Будет null, если Catbox лежит
    })


@app.route('/api/story/<user_id_str>/<story_id>/fragment/<fragment_id>/add_media', methods=['POST'])
def add_media(user_id_str, story_id, fragment_id):
    # ИЗМЕНЕНИЕ: Загружаем только одну историю
    from novel import load_user_story, save_story_data
    data = request.get_json()

    file_id = data.get("file_id")
    media_type = data.get("type")
    if not file_id or not media_type:
        return jsonify({"error": "file_id и type обязательны"}), 400

    story = load_user_story(user_id_str, story_id)
    
    if not story or "fragments" not in story or fragment_id not in story["fragments"]:
        return jsonify({"error": "Фрагмент не найден"}), 404

    media_entry = {"file_id": file_id, "type": media_type}
    story["fragments"][fragment_id].setdefault("media", []).append(media_entry)
    save_story_data(user_id_str, story_id, story)

    return jsonify({"status": "ok"})




# 👇 НОВАЯ ФУНКЦИЯ, КОТОРУЮ НУЖНО ДОБАВИТЬ
@app.route('/api/story/<user_id_str>/<story_id>/fragment/<fragment_id>/choices', methods=['PUT'])
def update_choices(user_id_str, story_id, fragment_id):
    # ИЗМЕНЕНИЕ: Загружаем только одну историю
    from novel import load_user_story, save_story_data
    data = request.get_json()
    choices_array = data.get("choices")

    if choices_array is None:
        return jsonify({"error": "Массив choices обязателен"}), 400

    story = load_user_story(user_id_str, story_id)
    
    if not story or "fragments" not in story or fragment_id not in story["fragments"]:
        return jsonify({"error": "Фрагмент не найден"}), 404

    story["fragments"][fragment_id]["choices"] = choices_array
    save_story_data(user_id_str, story_id, story)

    return jsonify({"status": "ok", "updatedFragment": story["fragments"][fragment_id]})
    
@app.route('/api/story/<user_id_str>/<story_id>/fragment/<fragment_id>/media', methods=['PUT'])
def update_media(user_id_str, story_id, fragment_id):
    # ИЗМЕНЕНИЕ: Загружаем только одну историю
    from novel import load_user_story, save_story_data
    data = request.get_json()
    media_array = data.get("media")

    if media_array is None:
        return jsonify({"error": "Массив media обязателен"}), 400

    story = load_user_story(user_id_str, story_id)
    
    if not story or "fragments" not in story or fragment_id not in story["fragments"]:
        return jsonify({"error": "Фрагмент не найден"}), 404

    story["fragments"][fragment_id]["media"] = media_array
    save_story_data(user_id_str, story_id, story)

    return jsonify({"status": "ok"})


# --- НОВЫЙ ЭНДПОИНТ: Создание пустого фрагмента ---
@app.route('/api/story/<user_id_str>/<story_id>/create_fragment', methods=['POST'])
def create_standalone_fragment(user_id_str, story_id):
    # ИЗМЕНЕНИЕ: Загружаем только одну историю
    from novel import load_user_story, save_story_data
    data = request.get_json()
    new_name = data.get("newName")

    if not new_name:
        return jsonify({"error": "Имя нового фрагмента не предоставлено"}), 400
    is_valid, error_message = validate_fragment_name(new_name)
    if not is_valid:
        return jsonify({"error": error_message}), 400
    
    story = load_user_story(user_id_str, story_id)
    
    if not story:
        return jsonify({"error": "История не найдена"}), 404
        
    if new_name in story["fragments"]:
        return jsonify({"error": f"Фрагмент с именем '{new_name}' уже существует"}), 409

    # Создаем новый пустой фрагмент
    story["fragments"][new_name] = {
        "text": "(пусто)",
        "choices": [],
        "media": []
    }
    
    save_story_data(user_id_str, story_id, story)
    return jsonify({"status": "ok", "story": story})


# --- ЭНДПОИНТ: Сохранение заметки (ОБНОВЛЕНО) ---
@app.route('/api/story/<user_id_str>/<story_id>/bookmarks', methods=['POST'])
def add_note_bookmark(user_id_str, story_id):
    from novel import save_story_bookmark # Импортируем новую функцию
    data = request.get_json()
    note_text = data.get("text")
    position = data.get("position")

    if not note_text or position is None:
        return jsonify({"error": "Недостаточно данных для создания заметки"}), 400

    bookmark_data = {"text": note_text, "position": position}
    
    new_bookmark = save_story_bookmark(user_id_str, story_id, bookmark_data)
    
    if new_bookmark:
        return jsonify({"status": "ok", "bookmark": new_bookmark})
    else:
        return jsonify({"error": "Не удалось сохранить заметку"}), 500

# --- НОВЫЙ ЭНДПОИНТ: Загрузка всех заметок ---
@app.route('/api/story/<user_id_str>/<story_id>/bookmarks', methods=['GET'])
def get_story_bookmarks(user_id_str, story_id):
    from novel import load_story_bookmarks
    bookmarks = load_story_bookmarks(user_id_str, story_id)
    if bookmarks is not None:
        return jsonify(bookmarks)
    return jsonify({}) # Возвращаем пустой объект, если заметок нет или произошла ошибка


# --- НОВЫЙ ЭНДПОИНТ: Обновление заметки ---
@app.route('/api/story/<user_id_str>/<story_id>/bookmarks/<note_id>', methods=['PUT'])
def update_note_bookmark(user_id_str, story_id, note_id):
    from novel import update_story_bookmark # Импортируем новую функцию
    data = request.get_json()
    new_text = data.get("text")

    if new_text is None:
        return jsonify({"error": "Отсутствует текст для обновления"}), 400

    if update_story_bookmark(user_id_str, story_id, note_id, new_text):
        return jsonify({"status": "ok"})
    else:
        return jsonify({"error": "Не удалось обновить заметку"}), 500

# --- НОВЫЙ ЭНДПОИНТ: Удаление заметки ---
@app.route('/api/story/<user_id_str>/<story_id>/bookmarks/<note_id>', methods=['DELETE'])
def delete_note_bookmark(user_id_str, story_id, note_id):
    from novel import delete_story_bookmark # Импортируем новую функцию

    if delete_story_bookmark(user_id_str, story_id, note_id):
        return jsonify({"status": "ok"})
    else:
        return jsonify({"error": "Не удалось удалить заметку"}), 500











@app.route('/api/story/<user_id_str>/<story_id>/effects', methods=['GET'])
def get_story_effects(user_id_str, story_id):
    # ИЗМЕНЕНИЕ: Загружаем только одну историю
    from novel import load_user_story
    
    story = load_user_story(user_id_str, story_id)
    
    logging.info(f'story: {story}')
    if not story:
        return jsonify({"error": "История не найдена"}), 404

    used_effects = set()

    fragments = story.get("fragments", {})
    for fragment_id, fragment_data in fragments.items():
        if not isinstance(fragment_data, dict):
            continue
        choices = fragment_data.get("choices", [])
        for choice in choices:
            for effect in choice.get("effects", []):
                stat = effect.get("stat")
                if stat:
                    used_effects.add(stat.strip()) 

    logging.info(f'used_effects: {used_effects}')
    return jsonify(sorted(list(used_effects)))


@app.route('/api/stories/<user_id_str>', methods=['GET'])
def get_story_list(user_id_str):
    """
    Получает список историй (только метаданные) для пользователя.
    """
    from novel import load_all_user_stories
    
    try:
        all_stories = load_all_user_stories(user_id_str)
        result = []

        for story_id, story_data in all_stories.items():
            result.append({
                "id": story_id,
                "title": story_data.get("title", "Без названия"),
                "public": story_data.get("public", False),
                "user_name": story_data.get("user_name", None),
                # --- ДОБАВЛЯЕМ ВОТ ЭТУ СТРОКУ ---
                "neural": story_data.get("neural", False) 
                # --------------------------------
            })

        return jsonify(result)
    except Exception as e:
        # Лучше использовать logging вместо print/logger если не настроен, но оставим как было
        print(f"Ошибка при получении списка историй для {user_id_str}: {e}")
        return jsonify({"error": "Не удалось загрузить список историй"}), 500
import uuid # <-- 1. ДОБАВЬТЕ ЭТОТ ИМПОРТ
# <-- 3. ДОБАВЬТЕ ЭТОТ НОВЫЙ МАРШРУТ (после get_stories_list) -->
@app.route('/api/stories/<user_id_str>/create', methods=['POST'])
def create_new_story(user_id_str):
    """
    Создает новую пустую историю.
    """
    from novel import save_story_data # Импортируем здесь
    try:
        data = request.get_json()
        title = data.get("title")
        user_name = data.get("user_name") # Получаем из запроса
        author_name = data.get("author_name", user_name) # Используем user_name или отдельное поле

        if not title:
            return jsonify({"error": "Название истории обязательно"}), 400

        # Генерация уникального ID для истории
        story_id = uuid.uuid4().hex[:10] # 10-значный ID

        # Создание базовой структуры истории
        new_story = {
            "title": title,
            "owner_id": user_id_str,
            "user_name": user_name,
            "author": author_name,
            "public": False, # По умолчанию не публичная
            "fragments": {
                "main_1": {
                    "text": "Начало вашей новой истории...",
                    "choices": []
                }
            }
            # Можете добавить другие поля по умолчанию, если нужно
        }

        # Сохраняем новую историю
        save_story_data(user_id_str, story_id, new_story)

        return jsonify({
            "status": "ok", 
            "story_id": story_id, 
            "title": title
        }), 201

    except Exception as e:
        logger.error(f"Ошибка при создании истории для {user_id_str}: {e}")
        return jsonify({"error": "Не удалось создать историю"}), 500



@app.route('/api/stories/<user_id_str>/<story_id>/delete', methods=['DELETE'])
def delete_story(user_id_str, story_id):
    """
    Удаляет историю по user_id_str и story_id.
    """
    from firebase_admin import db
    try:
        ref = db.reference(f'users_story/{user_id_str}/{story_id}')
        if ref.get() is None:
            return jsonify({"error": "История не найдена"}), 404

        ref.delete()
        return jsonify({"status": "deleted", "story_id": story_id}), 200

    except Exception as e:
        logger.error(f"Ошибка при удалении истории {story_id} для {user_id_str}: {e}")
        return jsonify({"error": "Не удалось удалить историю"}), 500


@app.route('/api/story/<user_id_str>/<story_id>/public', methods=['POST'])
def update_story_public_status(user_id_str, story_id):
    # ИЗМЕНЕНИЕ: Загружаем только одну историю
    from novel import load_user_story, save_story_data

    try:
        data = request.get_json()
        new_status = bool(data.get("public"))
        user_name = data.get("user_name")

        story = load_user_story(user_id_str, story_id)
        
        if not story:
            return jsonify({"error": "История не найдена"}), 404

        story["public"] = new_status

        if new_status and user_name:
            story["user_name"] = user_name
        elif not new_status:
            story.pop("user_name", None)

        save_story_data(user_id_str, story_id, story)

        return jsonify({
            "status": "ok",
            "public": new_status,
            "user_name": story.get("user_name")
        })
    except Exception as e:
        logger.error(f"Ошибка при обновлении статуса публичности: {e}")
        return jsonify({"error": "Не удалось обновить статус"}), 500


# 4. CATCH-ALL маршрут (Ловит все остальные запросы и статику)
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve_react_app(path):
    if path != "" and os.path.exists(os.path.join(app.static_folder, path)):
        # Если запрашивают реальный файл (картинку, js, css) - отдаем его
        return send_from_directory(app.static_folder, path)
    else:
        # Для всех остальных путей - отдаем React (index.html)
        return send_from_directory(app.static_folder, 'index.html')

# 1. Специфичный маршрут для редактора HTML
@app.route('/<user_id>_<story_id>/html')
def html_editor_route(user_id, story_id):
    # Просто отдаем index.html, дальше разберется React
    return send_from_directory(app.static_folder, 'index.html')

# СТАЛО (Исправленная версия):
@app.route('/<user_id>_<story_id>/html/play')
def serve_play_page(user_id, story_id):
    # Используем app.static_folder, который указывает на 'client/build'
    return send_from_directory(app.static_folder, 'index.html')



# 3. Маршрут для обычного редактора /userid_storyid
@app.route('/<string:user_story>')
def react_router_entry(user_story):
    # Проверяем, похоже ли это на ID истории (например 123_abc)
    if re.match(r'^(\d+)_([a-zA-Z0-9]+)$', user_story):
        return send_from_directory(app.static_folder, 'index.html')
    else:
        # Если это не история, пытаемся найти такой файл (на случай конфликтов)
        full_path = os.path.join(app.static_folder, user_story)
        if os.path.exists(full_path):
            return send_from_directory(app.static_folder, user_story)
        else:
            return send_from_directory(app.static_folder, 'index.html')




def run():
    app.run(host='0.0.0.0', port=80)

def keep_alive():
    t = Thread(target=run)
    t.start()
