from flask import Flask, request, send_from_directory, jsonify
from threading import Thread
import os
import re
import requests
import logging

# --- КОНФИГУРАЦИЯ ---
BUILD_FOLDER = os.path.join(os.path.dirname(__file__), 'client', 'build')
TELEGRAM_BOT_TOKEN = "7553491252:AAFwKa2WzZ6wKMVUIGt18oxCGPNqvSo5oRA"

app = Flask(__name__, static_folder=BUILD_FOLDER, static_url_path='')
logging.getLogger("httpx").setLevel(logging.WARNING) # Уменьшает спам от http запросов
logger = logging.getLogger(__name__)
# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

def validate_fragment_name(name):
    if name != "main_1" and name.startswith("main_1"):
        return False, "Фрагмент main_1 является началом и не может быть переименован."

    if len(name) > 17:
        return False, "Название не должно быть длиннее 17 символов."
    if not re.match(r'^[a-zA-Zа-яА-Я0-9_]+$', name):
        return False, "Название может содержать только латиницу, кириллицу, цифры и нижнее подчеркивание."
    if name.count('_') > 1 or (name.count('_') == 1 and not re.search(r'_[0-9]+$', name)):
        return False, "Допускается только одно нижнее подчеркивание перед цифрой в конце (например, GoLeft_6)."
    return True, ""

# --- API МАРШРУТЫ ---

@app.route('/api/story/<user_id_str>/<story_id>', methods=['GET'])
def get_story(user_id_str, story_id):
    from novel import load_all_user_stories
    all_stories = load_all_user_stories(user_id_str)
    story = all_stories.get(story_id)
    if story:
        return jsonify(story)
    return jsonify({"error": "История не найдена"}), 404

@app.route('/api/story/<user_id_str>/<story_id>/fragment/<fragment_id>/text', methods=['POST'])
def update_fragment_text(user_id_str, story_id, fragment_id):
    from novel import load_all_user_stories, save_story_data
    data = request.get_json()
    new_text = data.get("text", "").strip()

    all_stories = load_all_user_stories(user_id_str)
    story = all_stories.get(story_id)
    if not story or "fragments" not in story or fragment_id not in story["fragments"]:
        return jsonify({"error": "Фрагмент не найден"}), 404

    story["fragments"][fragment_id]["text"] = new_text
    save_story_data(user_id_str, story_id, story)
    return jsonify({"status": "ok"})

@app.route('/api/story/<user_id_str>/<story_id>/fragment/<fragment_id>', methods=['DELETE'])
def delete_fragment(user_id_str, story_id, fragment_id):
    from novel import load_all_user_stories, save_story_data
    all_stories = load_all_user_stories(user_id_str)
    story = all_stories.get(story_id)
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

@app.route('/api/story/<user_id_str>/<story_id>/fragment/<old_name>/rename', methods=['POST'])
def rename_fragment(user_id_str, story_id, old_name):
    from novel import load_all_user_stories, save_story_data
    data = request.get_json()
    new_name = data.get("newName")

    if not new_name:
        return jsonify({"error": "Новое имя не предоставлено"}), 400

    is_valid, error_message = validate_fragment_name(new_name)
    if not is_valid:
        return jsonify({"error": error_message}), 400

    all_stories = load_all_user_stories(user_id_str)
    story = all_stories.get(story_id)
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
    from novel import load_all_user_stories, save_story_data
    data = request.get_json()
    source_id = data.get("source")
    target_id = data.get("target")
    text = data.get("text")

    if not all([source_id, target_id, text]):
        return jsonify({"error": "Недостаточно данных"}), 400

    all_stories = load_all_user_stories(user_id_str)
    story = all_stories.get(story_id)
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
    from novel import load_all_user_stories, save_story_data
    data = request.get_json()
    source_id = data.get("source")
    new_name = data.get("newName")
    choice_text = data.get("choiceText")

    if not all([source_id, new_name, choice_text]):
        return jsonify({"error": "Недостаточно данных"}), 400

    is_valid, error_message = validate_fragment_name(new_name)
    if not is_valid:
        return jsonify({"error": error_message}), 400

    all_stories = load_all_user_stories(user_id_str)
    story = all_stories.get(story_id)
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

    getfile_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getFile"
    resp = requests.get(getfile_url, params={"file_id": file_id})
    file_data = resp.json()

    if not file_data.get("ok"):
        return jsonify({'error': 'Invalid file_id'}), 404

    file_path = file_data["result"]["file_path"]
    file_url = f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{file_path}"
    
    tg_response = requests.get(file_url, stream=True)
    if tg_response.status_code != 200:
        return jsonify({'error': 'Failed to fetch file'}), 502

    content_type = tg_response.headers.get("Content-Type", "application/octet-stream")
    return app.response_class(
        tg_response.iter_content(chunk_size=4096),
        content_type=content_type
    )



# Эндпоинт для обновления конкретного choice (связи)
@app.route('/api/story/<user_id_str>/<story_id>/choice', methods=['PUT'])
def update_choice(user_id_str, story_id):
    from novel import load_all_user_stories, save_story_data
    data = request.get_json()
    source_id = data.get("source")
    # Для идентификации choice нам нужен либо target, либо его индекс
    choice_index = data.get("choiceIndex") 
    new_text = data.get("text")
    new_effects = data.get("effects")

    if source_id is None or choice_index is None:
        return jsonify({"error": "Необходим source_id и choice_index"}), 400

    all_stories = load_all_user_stories(user_id_str)
    story = all_stories.get(story_id)
    if not story or source_id not in story["fragments"]:
        return jsonify({"error": "Фрагмент не найден"}), 404

    source_fragment = story["fragments"][source_id]
    if "choices" not in source_fragment or len(source_fragment["choices"]) <= choice_index:
        return jsonify({"error": "Связь не найдена"}), 404

    # Обновляем данные
    if new_text is not None:
        source_fragment["choices"][choice_index]["text"] = new_text
    if new_effects is not None:
        source_fragment["choices"][choice_index]["effects"] = new_effects

    save_story_data(user_id_str, story_id, story)
    # Возвращаем обновленный фрагмент, чтобы фронтенд мог обновить состояние
    return jsonify({"status": "ok", "updatedFragment": source_fragment}) 

# Эндпоинт для удаления choice (связи)
@app.route('/api/story/<user_id_str>/<story_id>/choice', methods=['DELETE'])
def delete_choice(user_id_str, story_id):
    from novel import load_all_user_stories, save_story_data
    data = request.get_json()
    source_id = data.get("source")
    choice_index = data.get("choiceIndex")

    if source_id is None or choice_index is None:
        return jsonify({"error": "Необходим source_id и choice_index"}), 400

    all_stories = load_all_user_stories(user_id_str)
    story = all_stories.get(story_id)
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

@app.route('/<string:user_story>')
def react_router_entry(user_story):
    if re.match(r'^(\d+)_([a-zA-Z0-9]+)$', user_story):
        return send_from_directory(app.static_folder, 'index.html')
    else:
        full_path = os.path.join(app.static_folder, user_story)
        if os.path.exists(full_path):
            return send_from_directory(app.static_folder, user_story)
        else:
            return send_from_directory(app.static_folder, 'index.html')


import logging

# Убедитесь, что логирование настроено
logging.basicConfig(level=logging.INFO)

@app.route('/api/upload_media', methods=['POST'])
def upload_media():
    file = request.files.get('file')
    if not file:
        logging.warning('Файл не передан в запросе')
        return jsonify({'error': 'Файл не передан'}), 400

    file_bytes = file.read()
    filename = file.filename.lower()
    logging.info(f'Загружен файл: {filename}, размер: {len(file_bytes)} байт')

    # Определим тип медиа
    if filename.endswith(('.jpg', '.jpeg', '.png', '.webp')):
        send_method = 'sendPhoto'
        field = 'photo'
        media_type = 'photo'
    elif filename.endswith(('.mp4', '.mov', '.webm')):
        send_method = 'sendVideo'
        field = 'video'
        media_type = 'video'
    elif filename.endswith(('.mp3', '.ogg', '.wav', '.m4a')):
        send_method = 'sendAudio'
        field = 'audio'
        media_type = 'audio'
    elif filename.endswith(('.gif')):
        send_method = 'sendAnimation'
        field = 'animation'
        media_type = 'animation'
    else:
        logging.error(f'Неподдерживаемый формат файла: {filename}')
        return jsonify({'error': 'Неподдерживаемый формат файла'}), 400

    # Отправим файл себе (боту) в "немой" чат
    url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{send_method}'
    files = {field: (file.filename, file_bytes)}
    user_id = request.form.get('user_id')
    if not user_id:
        return jsonify({'error': 'user_id не передан'}), 400

    data = {'chat_id': user_id}  

    try:
        logging.info(f'Отправка запроса в Telegram: {send_method}')
        response = requests.post(url, files=files, data=data)
        logging.info(f'Telegram ответил статусом {response.status_code}')
        logging.debug(f'Ответ Telegram: {response.text}')

        result = response.json()
        if not result.get('ok'):
            logging.error(f'Ошибка Telegram API: {result}')
            return jsonify({'error': 'Не удалось отправить медиа в Telegram', 'details': result}), 500

        file_obj = result['result'][field] if isinstance(result['result'][field], dict) else result['result'][field][-1]
        file_id = file_obj['file_id']

        logging.info(f'Успешно получен file_id: {file_id}')
        return jsonify({'file_id': file_id, 'type': media_type})
    except Exception as e:
        logging.exception('Исключение при отправке медиа')
        return jsonify({'error': str(e)}), 500


@app.route('/api/story/<user_id_str>/<story_id>/fragment/<fragment_id>/add_media', methods=['POST'])
def add_media(user_id_str, story_id, fragment_id):
    from novel import load_all_user_stories, save_story_data
    data = request.get_json()

    file_id = data.get("file_id")
    media_type = data.get("type")
    if not file_id or not media_type:
        return jsonify({"error": "file_id и type обязательны"}), 400

    all_stories = load_all_user_stories(user_id_str)
    story = all_stories.get(story_id)
    if not story or "fragments" not in story or fragment_id not in story["fragments"]:
        return jsonify({"error": "Фрагмент не найден"}), 404

    media_entry = {"file_id": file_id, "type": media_type}
    story["fragments"][fragment_id].setdefault("media", []).append(media_entry)
    save_story_data(user_id_str, story_id, story)

    return jsonify({"status": "ok"})
# 👇 НОВАЯ ФУНКЦИЯ, КОТОРУЮ НУЖНО ДОБАВИТЬ
@app.route('/api/story/<user_id_str>/<story_id>/fragment/<fragment_id>/choices', methods=['PUT'])
def update_choices(user_id_str, story_id, fragment_id):
    from novel import load_all_user_stories, save_story_data
    data = request.get_json()
    choices_array = data.get("choices")

    # Проверяем, что массив choices был передан
    if choices_array is None:
        return jsonify({"error": "Массив choices обязателен"}), 400

    all_stories = load_all_user_stories(user_id_str)
    story = all_stories.get(story_id)
    if not story or "fragments" not in story or fragment_id not in story["fragments"]:
        return jsonify({"error": "Фрагмент не найден"}), 404

    # Находим нужный фрагмент и полностью заменяем его массив кнопок
    story["fragments"][fragment_id]["choices"] = choices_array
    save_story_data(user_id_str, story_id, story)

    return jsonify({"status": "ok", "updatedFragment": story["fragments"][fragment_id]})

    
@app.route('/api/story/<user_id_str>/<story_id>/fragment/<fragment_id>/media', methods=['PUT'])
def update_media(user_id_str, story_id, fragment_id):
    from novel import load_all_user_stories, save_story_data
    data = request.get_json()
    media_array = data.get("media")

    if media_array is None:
        return jsonify({"error": "Массив media обязателен"}), 400

    all_stories = load_all_user_stories(user_id_str)
    story = all_stories.get(story_id)
    if not story or "fragments" not in story or fragment_id not in story["fragments"]:
        return jsonify({"error": "Фрагмент не найден"}), 404

    story["fragments"][fragment_id]["media"] = media_array
    save_story_data(user_id_str, story_id, story)

    return jsonify({"status": "ok"})
@app.route('/')
def index():
    return send_from_directory(app.static_folder, 'index.html')

def run():
    app.run(host='0.0.0.0', port=80)

def keep_alive():
    t = Thread(target=run)
    t.start()
