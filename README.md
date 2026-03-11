# 🎤 UK Rap Analytics · Spotify BI Dashboard

> Учебный BI-проект: SQL-витрина данных + ETL-пайплайн + интерактивный дашборд  
> для анализа аудитории британских рэперов на основе публичных датасетов Spotify.


---

## Структура проекта

```
spotify-uk-rap-bi/
├── README.md
├── requirements.txt
├── etl_pipeline.py          ← главный ETL-скрипт
├── sql/
│   └── schema.sql           ← DDL: raw → dw → mart слои
├── dashboard/
│   └── index.html           ← интерактивный BI-дашборд
└── data/
    ├── raw/                 ← сырые CSV с Kaggle (gitignore)
    ├── stage/               ← объединённый датасет
    └── mart/                ← mart-таблицы для BI
        ├── mart_top_tracks.csv
        ├── mart_artist_profile.csv
        ├── mart_genre_profile.csv
        └── mart_explicit_analysis.csv
```

---

## Источники данных

| # | Датасет | Строк | Ключевые поля |
|---|---------|-------|---------------|
| 1 | [Spotify Tracks Dataset](https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset) | 114K | track_id, artists, popularity, danceability, energy, valence, tempo |
| 2 | [Most Streamed Songs 2024](https://www.kaggle.com/datasets/nelgiriyewithana/most-streamed-spotify-songs-2024) | ~4K | track, artist, spotify_streams, release_date |


---

## 🚀 Быстрый старт

### 1. Клонировать репозиторий
```bash
git clone https://github.com/YOUR_USERNAME/spotify-uk-rap-bi.git
cd spotify-uk-rap-bi
```

### 2. Установить зависимости
```bash
pip install -r requirements.txt
```

### 3. Настроить Kaggle API
```bash
# Шаг 1: Зайди на kaggle.com → Account → API → Create New Token
# Шаг 2: Скачанный kaggle.json положи в:
mkdir -p ~/.kaggle
mv ~/Downloads/kaggle.json ~/.kaggle/
chmod 600 ~/.kaggle/kaggle.json   # Mac/Linux
```

### 4. Запустить ETL
```bash
# Полный запуск (скачивание + трансформация + загрузка)
python etl_pipeline.py

# Если CSV уже скачаны вручную в data/raw/
python etl_pipeline.py --skip-download

# С загрузкой в PostgreSQL
python etl_pipeline.py --postgres "postgresql://user:pass@localhost/spotify_db"
```

### 5. Открыть дашборд
```bash
# Просто открыть в браузере:
open dashboard/index.html

# Или запустить локальный сервер:
python -m http.server 8080
# → http://localhost:8080/dashboard/
```

---

##  Архитектура данных

```
raw.streams          raw.artists          raw.tracks
     │                    │                    │
     └────────────────────┴────────────────────┘
                          │
                    ETL Pipeline
                          │
          ┌───────────────┼───────────────┐
     dw.dim_date    dw.dim_artist    dw.dim_track
          │               │               │
          └───────────────┴───────────────┘
                          │
                   dw.fact_streams
                          │
          ┌───────────────┼───────────────┐
   mart.top_tracks  mart.artists   mart.genre
```

**Слои:**
- **raw** — сырые данные из Kaggle без изменений
- **dw** — нормализованная dimensional модель (SCD Type 2 для артистов)
- **mart** — агрегированные витрины для BI-дашборда

---

## 📈 Метрики дашборда

| Метрика | Источник | Описание |
|---------|----------|----------|
| **Popularity** | Kaggle DS1 | Индекс популярности Spotify (0–100) |
| **Spotify Streams** | Kaggle DS2 | Кол-во стримов (Most Streamed 2024) |
| **Danceability** | Kaggle DS1 | Насколько трек подходит для танца (0–1) |
| **Energy** | Kaggle DS1 | Интенсивность и активность трека (0–1) |
| **Valence** | Kaggle DS1 | Позитивность/негативность настроения (0–1) |
| **Tempo** | Kaggle DS1 | BPM трека |
| **Completion Rate** | Расчётная | Симулированная метрика |
| **DAU/MAU** | Расчётная | Симулированная метрика |

---

## 🛠️ Технологии

- **Python 3.9+** — ETL пайплайн
- **pandas** — трансформация данных
- **kaggle** — API для скачивания датасетов
- **SQLite / PostgreSQL** — хранение витрин
- **Chart.js** — визуализации в дашборде
- **HTML/CSS/JS** — фронтенд дашборда

---

## 🇬🇧 Отслеживаемые артисты

Stormzy · Dave · Central Cee · Little Simz · Aitch · slowthai · AJ Tracey · Skepta · Giggs · J Hus · Headie One · Loyle Carner · Nines · Knucks · Pa Salieu · Tion Wayne · Digga D · Fredo · Kojey Radical · и др.

---

## 📝 Лицензия

MIT License · данные принадлежат Spotify / Kaggle авторам датасетов.
