"""
═══════════════════════════════════════════════════════════════
  UK RAP · SPOTIFY BI — ETL PIPELINE
  Источники:
    1. maharshipandya/-spotify-tracks-dataset  (114K треков)
    2. nelgiriyewithana/most-streamed-spotify-songs-2024
  Шаги: Extract → Transform → Merge → Load (CSV + SQLite/Postgres)
═══════════════════════════════════════════════════════════════
"""

import os
import json
import zipfile
import logging
import argparse
import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime

# ── опциональные зависимости ────────────────────────────────
try:
    import kaggle  # pip install kaggle
    KAGGLE_AVAILABLE = True
except ImportError:
    KAGGLE_AVAILABLE = False

try:
    import psycopg2  # pip install psycopg2-binary
    from psycopg2.extras import execute_values
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False

# ── настройка логирования ───────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("etl")

# ════════════════════════════════════════════════════════════
#  КОНФИГ
# ════════════════════════════════════════════════════════════
RAW_DIR    = Path("data/raw")
STAGE_DIR  = Path("data/stage")
MART_DIR   = Path("data/mart")
DB_PATH    = Path("data/spotify_uk_rap.db")

# Kaggle dataset slugs
DS_TRACKS  = "maharshipandya/-spotify-tracks-dataset"
DS_STREAMS = "nelgiriyewithana/most-streamed-spotify-songs-2024"

# Британские рэперы / гримейщики для фильтрации
UK_RAPPERS = {
    "Stormzy", "Dave", "Central Cee", "Little Simz", "Aitch",
    "slowthai", "AJ Tracey", "Skepta", "Giggs", "J Hus",
    "Headie One", "Unknown T", "Ghetts", "Wretch 32", "Loyle Carner",
    "Nines", "Clavish", "Knucks", "Pa Salieu", "Tion Wayne",
    "Digga D", "Potter Payper", "Fredo", "Santan Dave", "Kojey Radical",
    "Mist", "Jahlani", "Ivorian Doll", "Shaybo", "Zeze Millz",
}

UK_GENRES = {"hip-hop", "uk-rap", "grime", "afroswing", "trap"}

# ════════════════════════════════════════════════════════════
#  ШАग 1 · EXTRACT
# ════════════════════════════════════════════════════════════

def setup_kaggle_auth():
    """
    Проверяет наличие kaggle.json.
    Файл должен лежать в ~/.kaggle/kaggle.json
    Содержимое: {"username": "...", "key": "..."}
    Получить: kaggle.com → Account → API → Create New Token
    """
    kaggle_path = Path.home() / ".kaggle" / "kaggle.json"
    if not kaggle_path.exists():
        log.error("Файл ~/.kaggle/kaggle.json не найден!")
        log.error("1. Зайди на kaggle.com → Account → API → Create New Token")
        log.error("2. Сохрани скачанный kaggle.json в ~/.kaggle/")
        log.error("3. chmod 600 ~/.kaggle/kaggle.json  (на Mac/Linux)")
        raise FileNotFoundError("kaggle.json не найден")
    log.info(f"Kaggle auth: {kaggle_path} ✓")


def download_dataset(slug: str, dest: Path):
    """Скачивает датасет с Kaggle и распаковывает."""
    dest.mkdir(parents=True, exist_ok=True)
    dataset_name = slug.split("/")[-1]
    zip_path = dest / f"{dataset_name}.zip"

    if any(dest.glob("*.csv")):
        log.info(f"Датасет '{dataset_name}' уже скачан, пропускаем.")
        return

    log.info(f"Скачиваем: {slug} → {dest}")
    os.system(f'kaggle datasets download -d "{slug}" -p "{dest}"')

    # распаковываем
    zips = list(dest.glob("*.zip"))
    for z in zips:
        log.info(f"Распаковываем {z.name}")
        with zipfile.ZipFile(z, "r") as zf:
            zf.extractall(dest)
        z.unlink()
    log.info(f"  → файлы: {[f.name for f in dest.glob('*.csv')]}")


def extract():
    """EXTRACT: скачиваем оба датасета."""
    log.info("═══ EXTRACT ═══════════════════════════════")
    if not KAGGLE_AVAILABLE:
        log.error("kaggle не установлен. Запусти: pip install kaggle")
        raise ImportError("pip install kaggle")

    setup_kaggle_auth()
    download_dataset(DS_TRACKS,  RAW_DIR / "tracks")
    download_dataset(DS_STREAMS, RAW_DIR / "streams")


# ════════════════════════════════════════════════════════════
#  ШАГ 2 · TRANSFORM
# ════════════════════════════════════════════════════════════

def load_tracks_dataset() -> pd.DataFrame:
    """Загружает датасет 1: 114K треков с audio features."""
    csv_files = list((RAW_DIR / "tracks").glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"CSV не найден в {RAW_DIR / 'tracks'}")

    df = pd.read_csv(csv_files[0])
    log.info(f"Dataset 1 загружен: {len(df):,} строк, колонки: {list(df.columns)}")
    return df


def load_streams_dataset() -> pd.DataFrame:
    """Загружает датасет 2: Most Streamed 2024."""
    csv_files = list((RAW_DIR / "streams").glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"CSV не найден в {RAW_DIR / 'streams'}")

    df = pd.read_csv(csv_files[0], encoding="latin-1")
    log.info(f"Dataset 2 загружен: {len(df):,} строк, колонки: {list(df.columns)}")
    return df


def clean_tracks(df: pd.DataFrame) -> pd.DataFrame:
    """Очищает и стандартизирует датасет треков."""
    log.info("Cleaning: tracks dataset…")

    # стандартизация названий колонок
    df.columns = df.columns.str.lower().str.replace(" ", "_")

    # убираем дубликаты по track_id
    before = len(df)
    df = df.drop_duplicates(subset=["track_id"])
    log.info(f"  дубликаты удалены: {before - len(df)} строк")

    # убираем строки без артиста или трека
    df = df.dropna(subset=["artists", "track_name"])

    # числовые колонки → float
    audio_features = [
        "danceability", "energy", "loudness", "speechiness",
        "acousticness", "instrumentalness", "liveness", "valence", "tempo"
    ]
    for col in audio_features:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["duration_min"] = df.get("duration_ms", 0) / 60000
    df["explicit"]     = df.get("explicit", False).astype(bool)
    df["popularity"]   = pd.to_numeric(df.get("popularity", 0), errors="coerce").fillna(0).astype(int)

    log.info(f"  → {len(df):,} треков после очистки")
    return df


def clean_streams(df: pd.DataFrame) -> pd.DataFrame:
    """Очищает датасет Most Streamed 2024."""
    log.info("Cleaning: streams dataset…")
    df.columns = df.columns.str.lower().str.replace(" ", "_")

    # стримы — убираем запятые и конвертируем
    for col in df.columns:
        if "stream" in col or "view" in col or "like" in col:
            df[col] = (
                df[col].astype(str)
                .str.replace(",", "")
                .str.replace("−", "")
                .pipe(pd.to_numeric, errors="coerce")
            )

    df = df.dropna(subset=["track"])
    log.info(f"  → {len(df):,} треков после очистки")
    return df


def filter_uk_rappers(df: pd.DataFrame, artist_col: str) -> pd.DataFrame:
    """Фильтрует только UK рэперов."""
    mask = df[artist_col].astype(str).apply(
        lambda x: any(artist.lower() in x.lower() for artist in UK_RAPPERS)
    )
    filtered = df[mask].copy()
    log.info(f"  UK рэперы: {len(filtered):,} треков (из {len(df):,})")
    return filtered


def extract_primary_artist(artist_str: str) -> str:
    """Берёт первого артиста из строки вида 'Artist1;Artist2'."""
    if pd.isna(artist_str):
        return "Unknown"
    return str(artist_str).split(";")[0].split(",")[0].strip().strip("'\"[]")


def transform_tracks(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Трансформирует tracks датасет в dim_track формат."""
    df = clean_tracks(df_raw)

    # Попытка 1: фильтр по жанру
    if "track_genre" in df.columns:
        genre_mask = df["track_genre"].str.lower().isin(UK_GENRES)
        df_genre = df[genre_mask]
        log.info(f"  По жанру: {len(df_genre):,} треков")

    # Попытка 2: фильтр по артисту
    artist_col = "artists" if "artists" in df.columns else df.columns[1]
    df_artists = filter_uk_rappers(df, artist_col)

    # Объединяем оба фильтра
    df_combined = pd.concat([df_genre, df_artists]).drop_duplicates(subset=["track_id"])
    log.info(f"  Итого уникальных треков (жанр + артист): {len(df_combined):,}")

    # Финальные колонки для dim_track
    df_combined["primary_artist"] = df_combined[artist_col].apply(extract_primary_artist)

    keep = ["track_id", "primary_artist", "track_name", "album_name",
            "popularity", "duration_min", "explicit",
            "danceability", "energy", "loudness", "speechiness",
            "acousticness", "valence", "tempo", "track_genre"]
    keep = [c for c in keep if c in df_combined.columns]

    return df_combined[keep].reset_index(drop=True)


def transform_streams(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Трансформирует streams датасет."""
    df = clean_streams(df_raw)

    artist_col = next((c for c in df.columns if "artist" in c), None)
    track_col  = next((c for c in df.columns if "track" in c or "song" in c), "track")

    if artist_col:
        df = filter_uk_rappers(df, artist_col)

    # Стримы → числовые
    stream_col = next((c for c in df.columns if "spotify_stream" in c or c == "streams"), None)
    if stream_col:
        df = df.rename(columns={stream_col: "spotify_streams"})

    df["primary_artist"] = df[artist_col].apply(extract_primary_artist) if artist_col else "Unknown"
    df["track_name"]     = df[track_col]

    keep = ["track_name", "primary_artist", "spotify_streams",
            "release_date", "released_year"]
    keep = [c for c in keep if c in df.columns]

    return df[keep].reset_index(drop=True)


def merge_datasets(df_tracks: pd.DataFrame, df_streams: pd.DataFrame) -> pd.DataFrame:
    """
    Объединяет оба датасета по track_name + artist.
    Результат: обогащённая таблица с audio features + стримами.
    """
    log.info("Merging datasets…")

    # нормализуем ключи для join
    df_tracks["_key"] = (
        df_tracks["track_name"].str.lower().str.strip() + "|" +
        df_tracks["primary_artist"].str.lower().str.strip()
    )
    df_streams["_key"] = (
        df_streams["track_name"].str.lower().str.strip() + "|" +
        df_streams["primary_artist"].str.lower().str.strip()
    )

    df_merged = df_tracks.merge(
        df_streams[["_key", "spotify_streams", "release_date"]],
        on="_key", how="left"
    ).drop(columns=["_key"])

    matched = df_merged["spotify_streams"].notna().sum()
    log.info(f"  Совпало с датасетом стримов: {matched:,} / {len(df_merged):,} треков")
    return df_merged


def build_marts(df: pd.DataFrame) -> dict:
    """Строит mart-таблицы для BI."""
    log.info("Building mart tables…")

    # mart 1: топ треков
    mart_top_tracks = (
        df.sort_values("popularity", ascending=False)
        .head(50)
        [["track_name", "primary_artist", "popularity", "spotify_streams",
          "danceability", "energy", "valence", "tempo", "track_genre"]]
    )

    # mart 2: профиль артиста
    mart_artists = (
        df.groupby("primary_artist")
        .agg(
            track_count      = ("track_name", "count"),
            avg_popularity   = ("popularity", "mean"),
            avg_danceability = ("danceability", "mean"),
            avg_energy       = ("energy", "mean"),
            avg_valence      = ("valence", "mean"),
            avg_tempo        = ("tempo", "mean"),
            total_streams    = ("spotify_streams", "sum"),
        )
        .round(3)
        .reset_index()
        .sort_values("total_streams", ascending=False)
    )

    # mart 3: аудио-профиль по жанру
    if "track_genre" in df.columns:
        mart_genre = (
            df.groupby("track_genre")
            .agg(
                track_count      = ("track_name", "count"),
                avg_popularity   = ("popularity", "mean"),
                avg_danceability = ("danceability", "mean"),
                avg_energy       = ("energy", "mean"),
            )
            .round(3)
            .reset_index()
        )
    else:
        mart_genre = pd.DataFrame()

    # mart 4: explicit vs clean
    if "explicit" in df.columns:
        mart_explicit = (
            df.groupby(["primary_artist", "explicit"])
            .agg(track_count=("track_name","count"), avg_popularity=("popularity","mean"))
            .round(2).reset_index()
        )
    else:
        mart_explicit = pd.DataFrame()

    return {
        "top_tracks": mart_top_tracks,
        "artist_profile": mart_artists,
        "genre_profile": mart_genre,
        "explicit_analysis": mart_explicit,
    }


# ════════════════════════════════════════════════════════════
#  ШАГ 3 · LOAD
# ════════════════════════════════════════════════════════════

def load_to_csv(marts: dict):
    """Сохраняет mart-таблицы в CSV."""
    MART_DIR.mkdir(parents=True, exist_ok=True)
    for name, df in marts.items():
        if df.empty:
            continue
        path = MART_DIR / f"mart_{name}.csv"
        df.to_csv(path, index=False)
        log.info(f"  CSV сохранён: {path}  ({len(df):,} строк)")


def load_to_sqlite(df_main: pd.DataFrame, marts: dict):
    """Загружает данные в SQLite (для локального BI / Metabase)."""
    log.info(f"Загрузка в SQLite: {DB_PATH}")
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    df_main.to_sql("dim_tracks", conn, if_exists="replace", index=False)
    log.info(f"  dim_tracks: {len(df_main):,} строк")

    for name, df in marts.items():
        if df.empty:
            continue
        df.to_sql(f"mart_{name}", conn, if_exists="replace", index=False)
        log.info(f"  mart_{name}: {len(df):,} строк")

    conn.close()
    log.info(f"SQLite готов: {DB_PATH}")


def load_to_postgres(df_main: pd.DataFrame, marts: dict, conn_str: str):
    """Загружает данные в PostgreSQL."""
    if not POSTGRES_AVAILABLE:
        log.error("psycopg2 не установлен: pip install psycopg2-binary")
        return

    log.info("Загрузка в PostgreSQL…")
    conn = psycopg2.connect(conn_str)
    cur  = conn.cursor()

    # dim_tracks
    cols = list(df_main.columns)
    vals = [tuple(r) for r in df_main.itertuples(index=False)]
    cur.execute(f"DROP TABLE IF EXISTS dim_tracks")
    col_defs = ", ".join([f"{c} TEXT" for c in cols])
    cur.execute(f"CREATE TABLE dim_tracks ({col_defs})")
    execute_values(cur, f"INSERT INTO dim_tracks VALUES %s", vals)

    conn.commit()
    cur.close()
    conn.close()
    log.info("PostgreSQL загрузка завершена")


def generate_summary(df_main: pd.DataFrame, marts: dict):
    """Выводит краткую статистику в консоль."""
    print("\n" + "═"*55)
    print("  UK RAP SPOTIFY · ИТОГИ ETL")
    print("═"*55)
    print(f"  Треков в dim_tracks:     {len(df_main):>8,}")
    print(f"  Уникальных артистов:     {df_main['primary_artist'].nunique():>8,}")
    if "spotify_streams" in df_main.columns:
        total = df_main["spotify_streams"].sum()
        print(f"  Суммарные стримы:        {total:>8,.0f}")
    if not marts["top_tracks"].empty:
        top = marts["top_tracks"].iloc[0]
        print(f"\n  🎤 Топ трек: {top['track_name']} — {top['primary_artist']}")
        print(f"     Popularity: {top['popularity']}")
    if not marts["artist_profile"].empty:
        top_a = marts["artist_profile"].iloc[0]
        print(f"\n  🏆 Топ артист: {top_a['primary_artist']}")
        print(f"     Треков: {top_a['track_count']}  |  Avg popularity: {top_a['avg_popularity']:.1f}")
    print("═"*55)
    print(f"  SQLite: {DB_PATH}")
    print(f"  CSV:    {MART_DIR}/")
    print("═"*55 + "\n")


# ════════════════════════════════════════════════════════════
#  MAIN
# ════════════════════════════════════════════════════════════

def run_etl(skip_download=False, postgres_conn=None):
    log.info("Запуск ETL: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # 1. EXTRACT
    if not skip_download:
        extract()
    else:
        log.info("Пропускаем скачивание (--skip-download)")

    # 2. TRANSFORM
    log.info("═══ TRANSFORM ══════════════════════════════")
    df_tracks_raw  = load_tracks_dataset()
    df_streams_raw = load_streams_dataset()

    df_tracks  = transform_tracks(df_tracks_raw)
    df_streams = transform_streams(df_streams_raw)
    df_main    = merge_datasets(df_tracks, df_streams)
    marts      = build_marts(df_main)

    # 3. LOAD
    log.info("═══ LOAD ═══════════════════════════════════")
    STAGE_DIR.mkdir(parents=True, exist_ok=True)
    df_main.to_csv(STAGE_DIR / "uk_rappers_merged.csv", index=False)
    log.info(f"Stage CSV: {STAGE_DIR / 'uk_rappers_merged.csv'}")

    load_to_csv(marts)
    load_to_sqlite(df_main, marts)

    if postgres_conn:
        load_to_postgres(df_main, marts, postgres_conn)

    generate_summary(df_main, marts)
    log.info("ETL завершён успешно ✓")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UK Rap Spotify ETL Pipeline")
    parser.add_argument("--skip-download", action="store_true",
                        help="Пропустить скачивание (если CSV уже есть)")
    parser.add_argument("--postgres",      type=str, default=None,
                        help="PostgreSQL conn string: postgresql://user:pass@host/db")
    args = parser.parse_args()

    run_etl(skip_download=args.skip_download, postgres_conn=args.postgres)
