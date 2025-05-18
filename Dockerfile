# Используйте официальный образ Python
FROM python:3.9-slim

# Установите системные зависимости, включая Graphviz
RUN apt-get update && \
    apt-get install -y --no-install-recommends graphviz && \
    rm -rf /var/lib/apt/lists/*

# Установите рабочую директорию в контейнере
WORKDIR /app

# Скопируйте файл зависимостей и установите их
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Скопируйте остальной код вашего приложения
COPY . .

# Укажите команду для запуска вашего бота (замените your_bot_script.py на имя вашего главного файла)
CMD ["python", "otlnovel.py"]
