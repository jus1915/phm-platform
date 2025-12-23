-- init.sql

-- 1) 애플리케이션용 유저 & DB 생성
CREATE USER phm_user WITH PASSWORD 'phm-password';
CREATE DATABASE phm OWNER phm_user;
GRANT ALL PRIVILEGES ON DATABASE phm TO phm_user;

-- 2) phm DB로 접속
\connect phm;

-- 3) session 테이블 생성 (C# PgFrameSink 스키마 + 개선된 처리상태 컬럼)
CREATE TABLE IF NOT EXISTS session (
    id              BIGSERIAL PRIMARY KEY,

    -- 기본 메타데이터 (C#에서 입력)
    device_id       TEXT NOT NULL,
    name            TEXT,

    started_at_utc  TIMESTAMPTZ NOT NULL,
    ended_at_utc    TIMESTAMPTZ,

    sample_rate_hz  DOUBLE PRECISION NOT NULL,
    channel_count   INTEGER NOT NULL,

    device_name     TEXT,
    location        TEXT,

    raw_file_path   TEXT,
    comment         TEXT,

    -- 머신러닝 메타데이터
    task_type       TEXT,
    label_type      TEXT,
    data_split      TEXT,
    operator        TEXT,
    note            TEXT,

    -- 파이프라인 제어 컬럼
    bronze_done_at   TIMESTAMPTZ,
    feature_done_at  TIMESTAMPTZ,
    train_done_at    TIMESTAMPTZ,

    force_reprocess  BOOLEAN DEFAULT false,

    -- 세션 생성 시각
    created_at       TIMESTAMPTZ DEFAULT now()
);

-- 4) 소유자 phm_user 로 변경
ALTER TABLE session OWNER TO phm_user;
