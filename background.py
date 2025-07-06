from flask import Flask, request, send_from_directory, jsonify
from threading import Thread
import os
import re
import requests
# Абсолютный путь к папке client/build (где лежит index.html и static/)
BUILD_FOLDER = os.path.join(os.path.dirname(__file__), 'client', 'build')

app = Flask(__name__, static_folder=BUILD_FOLDER, static_url_path='')

@app.route('/api/story/<user_id_str>/<story_id>', methods=['GET'])
def get_story(user_id_str, story_id):
    from novel import load_all_user_stories
    all_stories = load_all_user_stories(user_id_str)
    story = all_stories.get(story_id)
    if story:
        return jsonify(story)
    return jsonify({"error": "Story not found"}), 404

# Этот маршрут обрабатывает адреса вида /6217936347_d98f2f97c7
@app.route('/<string:user_story>')
def react_router_entry(user_story):
    match = re.match(r'^(\d+)_([a-zA-Z0-9]+)$', user_story)
    if match:
        # Это путь вида user_id_story_id — возвращаем index.html
        return send_from_directory(app.static_folder, 'index.html')
    else:
        # Если путь не соответствует, пробуем отдать файл (например, static/js/bundle.js)
        full_path = os.path.join(app.static_folder, user_story)
        if os.path.exists(full_path):
            return send_from_directory(app.static_folder, user_story)
        else:
            # Иначе снова отдаём index.html (например, для любых других React маршрутов)
            return send_from_directory(app.static_folder, 'index.html')
            
TELEGRAM_BOT_TOKEN = "7923930676:AAEkCg6-E35fyRnAzvxqoZvgEo8o8KTT8EU" 

@app.route('/api/tgfile/<file_id>')
def get_telegram_file(file_id):
    if not TELEGRAM_BOT_TOKEN:
        return jsonify({'error': 'Bot token not set'}), 500

    # Получаем file_path через getFile
    getfile_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getFile"
    resp = requests.get(getfile_url, params={"file_id": file_id})
    file_data = resp.json()

    if not file_data.get("ok"):
        return jsonify({'error': 'Invalid file_id'}), 404

    file_path = file_data["result"]["file_path"]
    file_url = f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{file_path}"

    # Проксируем файл пользователю
    tg_response = requests.get(file_url, stream=True)
    if tg_response.status_code != 200:
        return jsonify({'error': 'Failed to fetch file'}), 502

    # Определяем тип файла
    content_type = tg_response.headers.get("Content-Type", "application/octet-stream")

    return app.response_class(
        tg_response.iter_content(chunk_size=4096),
        content_type=content_type
    )
@app.route('/')
def index():
    return send_from_directory(app.static_folder, 'index.html')

def run():
    app.run(host='0.0.0.0', port=80)

def keep_alive():
    t = Thread(target=run)
    t.start()
