-- ============================================================
-- SPOTIFY UK RAPPERS — SQL DATA WAREHOUSE
-- Витрина данных: аудитория британских рэперов
-- ============================================================

-- ── СЛОЙ RAW (staging) ──────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS dw;
CREATE SCHEMA IF NOT EXISTS mart;

-- Сырые данные о прослушиваниях
CREATE TABLE raw.streams (
    stream_id        BIGSERIAL PRIMARY KEY,
    user_id          VARCHAR(64)   NOT NULL,
    track_id         VARCHAR(64)   NOT NULL,
    artist_id        VARCHAR(64)   NOT NULL,
    listened_at      TIMESTAMPTZ   NOT NULL,
    duration_ms      INT,
    completed        BOOLEAN,        -- трек дослушан до конца
    source           VARCHAR(32),    -- radio | playlist | search | album
    country_code     CHAR(2),
    device_type      VARCHAR(16),    -- mobile | desktop | tablet | smart_speaker
    loaded_at        TIMESTAMPTZ DEFAULT NOW()
);

-- Справочник артистов
CREATE TABLE raw.artists (
    artist_id        VARCHAR(64) PRIMARY KEY,
    artist_name      VARCHAR(256) NOT NULL,
    followers        BIGINT,
    popularity       SMALLINT,       -- 0–100
    monthly_listeners BIGINT,
    verified         BOOLEAN DEFAULT FALSE,
    loaded_at        TIMESTAMPTZ DEFAULT NOW()
);

-- Справочник треков
CREATE TABLE raw.tracks (
    track_id         VARCHAR(64) PRIMARY KEY,
    artist_id        VARCHAR(64) NOT NULL,
    track_name       VARCHAR(512) NOT NULL,
    album_name       VARCHAR(512),
    release_date     DATE,
    duration_ms      INT,
    explicit         BOOLEAN,
    danceability     NUMERIC(4,3),   -- 0.0 – 1.0
    energy           NUMERIC(4,3),
    valence          NUMERIC(4,3),
    tempo            NUMERIC(6,2),
    loaded_at        TIMESTAMPTZ DEFAULT NOW()
);

-- Данные о подписках / конверсии
CREATE TABLE raw.user_subscriptions (
    user_id          VARCHAR(64) NOT NULL,
    event_type       VARCHAR(32) NOT NULL, -- trial_start | premium_start | premium_cancel | downgrade
    event_at         TIMESTAMPTZ NOT NULL,
    plan_type        VARCHAR(16),           -- free | premium | family | student
    loaded_at        TIMESTAMPTZ DEFAULT NOW()
);


-- ── СЛОЙ DW (dimensional) ───────────────────────────────────

-- Dim: дата
CREATE TABLE dw.dim_date (
    date_id          INT PRIMARY KEY,       -- YYYYMMDD
    full_date        DATE NOT NULL,
    year             SMALLINT,
    quarter          SMALLINT,
    month            SMALLINT,
    month_name       VARCHAR(12),
    week             SMALLINT,
    day_of_week      SMALLINT,
    day_name         VARCHAR(12),
    is_weekend       BOOLEAN
);

-- Populate dim_date for 2022–2025
INSERT INTO dw.dim_date
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT,
    d::DATE,
    EXTRACT(YEAR    FROM d)::SMALLINT,
    EXTRACT(QUARTER FROM d)::SMALLINT,
    EXTRACT(MONTH   FROM d)::SMALLINT,
    TO_CHAR(d, 'Month'),
    EXTRACT(WEEK    FROM d)::SMALLINT,
    EXTRACT(DOW     FROM d)::SMALLINT,
    TO_CHAR(d, 'Day'),
    EXTRACT(DOW FROM d) IN (0,6)
FROM GENERATE_SERIES('2022-01-01'::DATE, '2025-12-31'::DATE, '1 day') d;

-- Dim: артист
CREATE TABLE dw.dim_artist (
    artist_key       SERIAL PRIMARY KEY,
    artist_id        VARCHAR(64)  NOT NULL UNIQUE,
    artist_name      VARCHAR(256) NOT NULL,
    followers        BIGINT,
    popularity       SMALLINT,
    monthly_listeners BIGINT,
    verified         BOOLEAN,
    valid_from       DATE NOT NULL DEFAULT CURRENT_DATE,
    valid_to         DATE,
    is_current       BOOLEAN DEFAULT TRUE
);

-- Dim: трек
CREATE TABLE dw.dim_track (
    track_key        SERIAL PRIMARY KEY,
    track_id         VARCHAR(64)  NOT NULL UNIQUE,
    artist_key       INT REFERENCES dw.dim_artist(artist_key),
    track_name       VARCHAR(512),
    album_name       VARCHAR(512),
    release_date     DATE,
    duration_ms      INT,
    explicit         BOOLEAN,
    danceability     NUMERIC(4,3),
    energy           NUMERIC(4,3),
    valence          NUMERIC(4,3),
    tempo            NUMERIC(6,2)
);

-- Dim: пользователь (анонимизированный)
CREATE TABLE dw.dim_user (
    user_key         SERIAL PRIMARY KEY,
    user_id          VARCHAR(64) NOT NULL UNIQUE,
    country_code     CHAR(2),
    device_type      VARCHAR(16),
    plan_type        VARCHAR(16) DEFAULT 'free',
    first_seen_date  DATE,
    last_seen_date   DATE
);

-- Fact: прослушивания
CREATE TABLE dw.fact_streams (
    stream_key       BIGSERIAL PRIMARY KEY,
    date_id          INT REFERENCES dw.dim_date(date_id),
    user_key         INT REFERENCES dw.dim_user(user_key),
    track_key        INT REFERENCES dw.dim_track(track_key),
    artist_key       INT REFERENCES dw.dim_artist(artist_key),
    listened_at      TIMESTAMPTZ NOT NULL,
    duration_ms      INT,
    completed        BOOLEAN,
    source           VARCHAR(32)
);

-- Fact: подписки
CREATE TABLE dw.fact_subscriptions (
    sub_key          BIGSERIAL PRIMARY KEY,
    date_id          INT REFERENCES dw.dim_date(date_id),
    user_key         INT REFERENCES dw.dim_user(user_key),
    event_type       VARCHAR(32),
    plan_type        VARCHAR(16),
    event_at         TIMESTAMPTZ NOT NULL
);


-- ── СЛОЙ MART (витрины для BI) ──────────────────────────────

-- MAR-01: DAU / MAU по артистам
CREATE TABLE mart.artist_dau_mau AS
WITH daily AS (
    SELECT
        fs.date_id,
        da.artist_id,
        da.artist_name,
        COUNT(DISTINCT fs.user_key)  AS dau,
        SUM(fs.duration_ms) / 3600000.0 AS listening_hours
    FROM dw.fact_streams fs
    JOIN dw.dim_artist da USING (artist_key)
    JOIN dw.dim_date   dd USING (date_id)
    WHERE da.is_current = TRUE
    GROUP BY 1, 2, 3
),
monthly AS (
    SELECT
        dd.year,
        dd.month,
        da.artist_id,
        COUNT(DISTINCT fs.user_key) AS mau
    FROM dw.fact_streams fs
    JOIN dw.dim_artist da USING (artist_key)
    JOIN dw.dim_date   dd USING (date_id)
    WHERE da.is_current = TRUE
    GROUP BY 1, 2, 3
)
SELECT
    d.date_id,
    d.artist_id,
    d.artist_name,
    d.dau,
    d.listening_hours,
    m.mau,
    ROUND(d.dau::NUMERIC / NULLIF(m.mau, 0) * 100, 2) AS dau_mau_ratio
FROM daily d
JOIN dw.dim_date dd ON dd.date_id = d.date_id
JOIN monthly m ON m.year = dd.year AND m.month = dd.month AND m.artist_id = d.artist_id;

-- MAR-02: Retention (7-дневный когортный)
CREATE TABLE mart.user_retention AS
WITH cohorts AS (
    SELECT
        user_key,
        MIN(date_id) AS cohort_date_id
    FROM dw.fact_streams
    GROUP BY 1
),
activity AS (
    SELECT DISTINCT
        fs.user_key,
        fs.date_id
    FROM dw.fact_streams fs
),
cohort_activity AS (
    SELECT
        c.cohort_date_id,
        (a.date_id - c.cohort_date_id)          AS days_since_first,
        COUNT(DISTINCT a.user_key)               AS retained_users
    FROM cohorts c
    JOIN activity a USING (user_key)
    WHERE a.date_id - c.cohort_date_id BETWEEN 0 AND 30
    GROUP BY 1, 2
),
cohort_sizes AS (
    SELECT cohort_date_id, COUNT(*) AS cohort_size
    FROM cohorts
    GROUP BY 1
)
SELECT
    ca.cohort_date_id,
    ca.days_since_first,
    ca.retained_users,
    cs.cohort_size,
    ROUND(ca.retained_users::NUMERIC / cs.cohort_size * 100, 2) AS retention_rate
FROM cohort_activity ca
JOIN cohort_sizes cs USING (cohort_date_id);

-- MAR-03: Conversion funnel (free → premium)
CREATE TABLE mart.conversion_funnel AS
SELECT
    dd.year,
    dd.month,
    fs.event_type,
    COUNT(DISTINCT fs.user_key) AS users,
    LAG(COUNT(DISTINCT fs.user_key)) OVER (
        PARTITION BY dd.year, dd.month ORDER BY
        CASE fs.event_type
            WHEN 'trial_start'    THEN 1
            WHEN 'premium_start'  THEN 2
            WHEN 'premium_cancel' THEN 3
            ELSE 4
        END
    ) AS prev_stage_users
FROM dw.fact_subscriptions fs
JOIN dw.dim_date dd USING (date_id)
GROUP BY 1, 2, 3;

-- MAR-04: Топ треков по завершённым прослушиваниям
CREATE TABLE mart.top_tracks AS
SELECT
    dt.track_id,
    dt.track_name,
    da.artist_name,
    dt.release_date,
    COUNT(*)                                    AS total_streams,
    COUNT(*) FILTER (WHERE fs.completed = TRUE) AS completed_streams,
    ROUND(
        COUNT(*) FILTER (WHERE fs.completed = TRUE)::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2
    )                                           AS completion_rate,
    AVG(dt.danceability)                        AS avg_danceability,
    AVG(dt.energy)                              AS avg_energy
FROM dw.fact_streams fs
JOIN dw.dim_track  dt USING (track_key)
JOIN dw.dim_artist da USING (artist_key)
GROUP BY 1, 2, 3, 4
ORDER BY total_streams DESC;

-- ── ETL ПРОЦЕДУРЫ ───────────────────────────────────────────

-- Процедура: загрузка dim_artist (SCD Type 2)
CREATE OR REPLACE PROCEDURE dw.load_dim_artist()
LANGUAGE plpgsql AS $$
BEGIN
    -- Закрываем устаревшие записи
    UPDATE dw.dim_artist da
    SET valid_to   = CURRENT_DATE - 1,
        is_current = FALSE
    WHERE da.is_current = TRUE
      AND EXISTS (
        SELECT 1 FROM raw.artists ra
        WHERE ra.artist_id = da.artist_id
          AND (ra.followers != da.followers OR ra.popularity != da.popularity)
    );

    -- Вставляем новые/изменённые
    INSERT INTO dw.dim_artist (artist_id, artist_name, followers, popularity, monthly_listeners, verified)
    SELECT ra.artist_id, ra.artist_name, ra.followers, ra.popularity, ra.monthly_listeners, ra.verified
    FROM raw.artists ra
    WHERE NOT EXISTS (
        SELECT 1 FROM dw.dim_artist da
        WHERE da.artist_id = ra.artist_id AND da.is_current = TRUE
    );

    RAISE NOTICE 'dim_artist loaded at %', NOW();
END;
$$;

-- Процедура: загрузка fact_streams (инкрементальная)
CREATE OR REPLACE PROCEDURE dw.load_fact_streams(p_load_date DATE DEFAULT CURRENT_DATE - 1)
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO dw.fact_streams (date_id, user_key, track_key, artist_key, listened_at, duration_ms, completed, source)
    SELECT
        TO_CHAR(rs.listened_at AT TIME ZONE 'UTC', 'YYYYMMDD')::INT,
        du.user_key,
        dt.track_key,
        da.artist_key,
        rs.listened_at,
        rs.duration_ms,
        rs.completed,
        rs.source
    FROM raw.streams rs
    JOIN dw.dim_user   du ON du.user_id   = rs.user_id
    JOIN dw.dim_track  dt ON dt.track_id  = rs.track_id
    JOIN dw.dim_artist da ON da.artist_id = rs.artist_id AND da.is_current = TRUE
    WHERE rs.listened_at::DATE = p_load_date
      AND NOT EXISTS (
        SELECT 1 FROM dw.fact_streams fs
        WHERE fs.listened_at = rs.listened_at AND fs.user_key = du.user_key
    );

    RAISE NOTICE 'fact_streams loaded for %, rows: %', p_load_date, ROW_COUNT();
END;
$$;

-- ── ИНДЕКСЫ ─────────────────────────────────────────────────
CREATE INDEX idx_fact_streams_date      ON dw.fact_streams (date_id);
CREATE INDEX idx_fact_streams_artist    ON dw.fact_streams (artist_key);
CREATE INDEX idx_fact_streams_user      ON dw.fact_streams (user_key);
CREATE INDEX idx_fact_streams_listened  ON dw.fact_streams (listened_at);
CREATE INDEX idx_raw_streams_artist     ON raw.streams (artist_id, listened_at);
