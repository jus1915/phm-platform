# frames_to_features.py
import os
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

PG_URL = os.getenv(
    "PG_URL",
    "postgresql+psycopg2://phm_user:phm-password@localhost:5432/phm",
)
engine = create_engine(PG_URL)


# ----------------- DDL: 피처 테이블 보장 -----------------


def ensure_feature_table():
    ddl = """
    CREATE SCHEMA IF NOT EXISTS mart;

    CREATE TABLE IF NOT EXISTS mart.vibration_frame_features (
        session_id    BIGINT NOT NULL,
        device_id     TEXT   NOT NULL,
        frame_seq     BIGINT NOT NULL,
        t0_utc        TIMESTAMPTZ NOT NULL,
        axis          TEXT   NOT NULL,
        rms           DOUBLE PRECISION,
        peak          DOUBLE PRECISION,
        mean_abs      DOUBLE PRECISION,
        std           DOUBLE PRECISION,
        crest_factor  DOUBLE PRECISION,
        task_type     TEXT,
        label_type    TEXT,
        data_split    TEXT,
        operator      TEXT,
        PRIMARY KEY (session_id, frame_seq, axis)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


# ----------------- 상태 동기화 (마이그레이션용) -----------------


def sync_feature_status_from_mart():
    """
    mart.vibration_frame_features 에는 이미 데이터가 있는데,
    session.feature_done_at 이 NULL인 세션들에 대해 feature_done_at 을 현재 시각으로 세팅.
    기존 데이터에 대한 이중 INSERT / PK 충돌 방지용.
    """
    sql = text(
        """
        UPDATE session s
        SET feature_done_at = NOW()
        FROM (
            SELECT DISTINCT session_id
            FROM mart.vibration_frame_features
        ) ff
        WHERE s.id = ff.session_id
          AND s.feature_done_at IS NULL;
        """
    )
    with engine.begin() as conn:
        conn.execute(sql)


# ----------------- 타겟 세션 조회 -----------------


def fetch_sessions_without_features() -> pd.DataFrame:
    """
    - session.bronze_done_at 이 있어야 함 (raw.vibration_frame 이 준비되었다는 의미)
    - feature_done_at 이 NULL 이거나 force_reprocess = true 인 세션만 대상
    - raw.vibration_frame 에 실제 frame 이 존재하는 세션만 선택
    """
    query = text(
        """
        SELECT
            s.id        AS session_id,
            s.device_id AS device_id,
            COALESCE(s.force_reprocess, FALSE) AS force_reprocess
        FROM session s
        WHERE s.bronze_done_at IS NOT NULL
          AND (s.feature_done_at IS NULL OR s.force_reprocess = TRUE)
          AND EXISTS (
                SELECT 1
                FROM raw.vibration_frame vf
                WHERE vf.session_id = s.id
          )
        ORDER BY s.id;
        """
    )

    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)

    return df


# ----------------- 프레임 데이터 로드 -----------------


def iter_frames_for_session(session_id: int, chunksize: int = 500):
    """
    한 세션에 대한 raw.vibration_frame을 chunk 단위로 스트리밍.
    chunksize 프레임씩 DataFrame으로 넘겨줌.
    메타데이터(task_type, label_type, data_split, operator)도 함께 읽어온다.
    """
    query = text(
        """
        SELECT
            session_id,
            device_id,
            frame_seq,
            t0_utc,
            samples_per_frame,
            ax,
            ay,
            az,
            task_type,
            label_type,
            data_split,
            operator
        FROM raw.vibration_frame
        WHERE session_id = :sid
        ORDER BY frame_seq;
        """
    )

    conn = engine.connect()
    try:
        for chunk in pd.read_sql_query(
            query,
            conn,
            params={"sid": session_id},
            chunksize=chunksize,
        ):
            yield chunk
    finally:
        conn.close()


# ----------------- 피처 계산 -----------------


def compute_features(arr):
    """
    하나의 축 데이터(arr: list or np.array)에 대해
    RMS, peak, mean_abs, std, crest_factor 계산
    """
    if arr is None:
        return dict(rms=None, peak=None, mean_abs=None, std=None, crest_factor=None)

    a = np.asarray(arr, dtype=float)
    if a.size == 0:
        return dict(rms=None, peak=None, mean_abs=None, std=None, crest_factor=None)

    rms = float(np.sqrt(np.mean(a * a)))
    peak = float(np.max(np.abs(a)))
    mean_abs = float(np.mean(np.abs(a)))
    std = float(np.std(a))
    crest = float(peak / rms) if rms > 0 else None

    return dict(
        rms=rms,
        peak=peak,
        mean_abs=mean_abs,
        std=std,
        crest_factor=crest,
    )


def build_feature_rows(frames_df: pd.DataFrame) -> pd.DataFrame:
    """
    raw.vibration_frame 일부(chunk)의 df → long-format 피처 DF
    axis: x / y / z
    frame row 에 들어있는 메타데이터(task_type, label_type, data_split, operator)를 그대로 전달.
    """
    rows = []

    for _, row in frames_df.iterrows():
        session_id = int(row["session_id"])
        device_id = row["device_id"]
        frame_seq = int(row["frame_seq"])
        t0_utc = row["t0_utc"]

        # frame 레벨 메타데이터 (세션 내에서는 대부분 동일할 것)
        task_type = row.get("task_type")
        label_type = row.get("label_type")
        data_split = row.get("data_split")
        operator = row.get("operator")

        for axis, col_name in (("x", "ax"), ("y", "ay"), ("z", "az")):
            arr = row[col_name]
            feats = compute_features(arr)

            rows.append(
                {
                    "session_id": session_id,
                    "device_id": device_id,
                    "frame_seq": frame_seq,
                    "t0_utc": t0_utc,
                    "axis": axis,
                    "rms": feats["rms"],
                    "peak": feats["peak"],
                    "mean_abs": feats["mean_abs"],
                    "std": feats["std"],
                    "crest_factor": feats["crest_factor"],
                    "task_type": task_type,
                    "label_type": label_type,
                    "data_split": data_split,
                    "operator": operator,
                }
            )

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows)


# ----------------- DB INSERT / 삭제 / 상태 업데이트 -----------------


def insert_feature_df(df: pd.DataFrame):
    if df.empty:
        return

    with engine.begin() as conn:
        df.to_sql(
            name="vibration_frame_features",
            schema="mart",
            con=conn,
            if_exists="append",
            index=False,
            method="multi",
        )

    print(f"[FEATURE] inserted {len(df)} rows into mart.vibration_frame_features")


def delete_existing_features_if_any(session_id: int):
    """
    force_reprocess = true 인 세션에 대해,
    기존 mart.vibration_frame_features 데이터를 삭제 (PK 충돌 방지 + 재계산).
    """
    sql = text(
        """
        DELETE FROM mart.vibration_frame_features
        WHERE session_id = :sid;
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, {"sid": session_id})


def mark_session_feature_done(session_id: int):
    """
    세션 피처 계산 완료 마킹.
    """
    sql = text(
        """
        UPDATE session
        SET feature_done_at = NOW(),
            force_reprocess = FALSE
        WHERE id = :sid;
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, {"sid": session_id})


# ----------------- 메인 파이프라인 -----------------


def process_one_session(session_row: pd.Series):
    sid = int(session_row["session_id"])
    device_id = session_row["device_id"]
    force_reprocess = bool(session_row.get("force_reprocess", False))

    print(
        f"[FEATURE] session_id={sid}, device_id={device_id}, "
        f"force_reprocess={force_reprocess}"
    )

    if force_reprocess:
        print(f"[FEATURE] force_reprocess=TRUE, deleting existing features for session_id={sid}")
        delete_existing_features_if_any(sid)

    total_rows = 0
    has_any_frame = False

    # chunk 단위로 프레임 읽어서 바로 피처 계산 & INSERT
    for frames_df in iter_frames_for_session(sid, chunksize=500):
        has_any_frame = True

        if frames_df.empty:
            continue

        feat_df = build_feature_rows(frames_df)
        if feat_df.empty:
            continue

        insert_feature_df(feat_df)
        total_rows += len(feat_df)
        print(
            f"[FEATURE] session_id={sid}, chunk_rows={len(feat_df)}, total_rows={total_rows}"
        )

    if not has_any_frame:
        print(f"[FEATURE] no frames for session_id={sid}, skip")
        return

    print(f"[FEATURE] session_id={sid} done, total_rows={total_rows}")

    if total_rows > 0:
        mark_session_feature_done(sid)
    else:
        print(f"[FEATURE] WARN: session_id={sid} had no feature rows, feature_done_at not updated")


def main():
    ensure_feature_table()
    # 기존 데이터에 대해 feature_done_at 자동 동기화 (마이그레이션용)
    sync_feature_status_from_mart()

    sessions_df = fetch_sessions_without_features()
    if sessions_df.empty:
        print("[FEATURE] no sessions to process")
        return

    for _, row in sessions_df.iterrows():
        try:
            process_one_session(row)
        except Exception as ex:
            print(f"[FEATURE] ERROR on session_id={row['session_id']}: {ex}")


if __name__ == "__main__":
    main()
