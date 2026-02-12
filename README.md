# Excel Analyzer Pro — Web Edition

Веб-версия приложения для анализа баскетбольной статистики.

## Архитектура

```
┌─────────────────┐         ┌──────────────────────┐
│  React Frontend │ ◄─────► │  FastAPI Backend      │
│  (Vite + Antd)  │  /api   │  (Python + Pandas)   │
│  Port: 3000     │         │  Port: 8000           │
└─────────────────┘         └──────────┬───────────┘
                                       │
                            ┌──────────▼───────────┐
                            │  SQLite Databases     │
                            │  data/halfs.db        │
                            │  data/royka.db        │
                            │  data/cyber_bases.db  │
                            └──────────────────────┘
```

## Быстрый старт (разработка)

### 1. Backend

```bash
cd backend
python -m venv venv
venv\Scripts\activate       # Windows
# source venv/bin/activate  # Linux/Mac
pip install -r requirements.txt

# Запуск
cd ..
python -m uvicorn backend.app.main:app --reload --host 0.0.0.0 --port 8000
```

API документация: http://localhost:8000/docs

### 2. Frontend

```bash
cd frontend
npm install
npm run dev
```

Приложение: http://localhost:3000

## Деплой на VDS (Docker)

### 1. Установите Docker на VDS

```bash
# Ubuntu/Debian
sudo apt update
sudo apt install -y docker.io docker-compose-plugin
sudo systemctl enable docker
sudo systemctl start docker
```

### 2. Загрузите код на VDS

```bash
# На локальном компьютере — залейте в GitHub
cd E:\Work\Halfs
git init
git add .
git commit -m "Initial commit"
git remote add origin https://github.com/ВАШ_ЛОГИН/halfs-app.git
git branch -M main
git push -u origin main

# На VDS — клонируйте
git clone https://github.com/ВАШ_ЛОГИН/halfs-app.git
cd halfs-app
```

### 3. Настройте .env

```bash
cp .env.example .env
nano .env
# Укажите нужные настройки
```

### 4. Запустите

```bash
docker compose up -d --build
```

Приложение будет доступно по IP вашего VDS на порту 80.

### 5. Обновление

```bash
git pull
docker compose up -d --build
```

## Перенос существующих баз данных

Если у вас уже есть базы данных от десктопной версии, скопируйте их в папку `data/`:

```bash
# На Windows — найдите базы по пути:
# %APPDATA%\..\Local\ExcelAnalyzer\
#   halfs.db
#   royka.db
#   cyber_bases.db

# Скопируйте в папку data/ проекта
mkdir data
copy %LOCALAPPDATA%\ExcelAnalyzer\halfs.db data\
copy %LOCALAPPDATA%\ExcelAnalyzer\royka.db data\
copy %LOCALAPPDATA%\ExcelAnalyzer\cyber_bases.db data\
```

## API Endpoints

### Halfs (База половин)
| Метод  | URL                              | Описание                        |
|--------|----------------------------------|---------------------------------|
| GET    | `/api/halfs/matches`             | Список матчей                   |
| POST   | `/api/halfs/matches/import`      | Импорт из текста                |
| DELETE | `/api/halfs/matches`             | Удалить по ID                   |
| DELETE | `/api/halfs/matches/all`         | Очистить базу                   |
| GET    | `/api/halfs/tournaments`         | Список турниров                 |
| GET    | `/api/halfs/statistics`          | Статистика                      |
| GET    | `/api/halfs/team-stats/{tournament}` | Статистика команд          |
| GET    | `/api/halfs/summary`             | Сводная таблица                 |

### Royka (Ройка)
| Метод  | URL                              | Описание                        |
|--------|----------------------------------|---------------------------------|
| GET    | `/api/royka/matches`             | Список матчей                   |
| POST   | `/api/royka/matches`             | Добавить матчи                  |
| DELETE | `/api/royka/matches`             | Удалить по ID                   |
| GET    | `/api/royka/tournaments`         | Список турниров                 |
| GET    | `/api/royka/statistics`          | Статистика базы                 |
| GET    | `/api/royka/analysis/{tournament}` | Анализ турнира               |
| GET    | `/api/royka/analysis`            | Анализ всех турниров            |

### Cybers (Cybers Bases / Cyber LIVE)
| Метод  | URL                              | Описание                        |
|--------|----------------------------------|---------------------------------|
| GET    | `/api/cybers/matches`            | Все матчи базы                  |
| POST   | `/api/cybers/matches/import`     | Импорт из TSV                   |
| POST   | `/api/cybers/matches`            | Добавить матчи                  |
| DELETE | `/api/cybers/matches`            | Удалить по ID                   |
| DELETE | `/api/cybers/matches/all`        | Очистить базу                   |
| GET    | `/api/cybers/tournaments`        | Турниры                         |
| GET    | `/api/cybers/summary`            | Сводная статистика              |
| POST   | `/api/cybers/predict`            | Рассчитать прогноз              |
| GET    | `/api/cybers/live`               | Live матчи с расчётами          |
| POST   | `/api/cybers/live`               | Добавить live матч              |
| PUT    | `/api/cybers/live/{id}`          | Обновить live матч              |
| DELETE | `/api/cybers/live/{id}`          | Удалить live матч               |
| DELETE | `/api/cybers/live`               | Очистить все live матчи         |

## Интеграция с другим приложением

Используйте API endpoints для интеграции. Пример на Python:

```python
import requests

API = "http://ваш-сервер:8000/api"

# Получить прогноз на матч
response = requests.post(f"{API}/cybers/predict", json={
    "tournament": "EL",
    "team1": "Real",
    "team2": "Maccabi TA"
})
data = response.json()
print(f"Predict: {data['predict']}, TEMP: {data['temp']}")
print(f"IT1: {data['it1']}, IT2: {data['it2']}")

# Получить все live матчи с расчётами
response = requests.get(f"{API}/cybers/live")
for match in response.json():
    print(f"{match['team1']} vs {match['team2']}: {match['predict']}")
```

## Структура проекта

```
├── backend/                  # FastAPI бэкенд
│   ├── app/
│   │   ├── main.py           # Точка входа
│   │   ├── config.py         # Настройки
│   │   ├── database/         # Инициализация БД
│   │   ├── services/         # Бизнес-логика
│   │   ├── routers/          # API эндпоинты
│   │   └── schemas/          # Pydantic модели
│   ├── requirements.txt
│   └── Dockerfile
├── frontend/                 # React фронтенд
│   ├── src/
│   │   ├── api/client.js     # API клиент
│   │   ├── pages/            # Страницы
│   │   ├── App.jsx           # Главный компонент
│   │   └── main.jsx          # Точка входа
│   ├── package.json
│   └── Dockerfile
├── data/                     # SQLite базы данных
├── docker-compose.yml
├── .env.example
├── .gitignore
└── README.md
```
