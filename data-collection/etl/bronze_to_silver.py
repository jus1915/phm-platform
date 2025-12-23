# bronze_to_silver.py
import os
import io
import re
from urllib.parse import urlparse

from minio import Minio
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()


# ----------------- 설정 -----------------

PG_URL = os.getenv(
    "PG_URL",
    "postgresql+psycopg2://phm_user:phm-password@localhost:5432/phm",
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

engine = create_engine(PG_URL)


def make_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


# ----------------- 유틸 함수 -----------------


def parse_s3_url(s3_url: str):
    """
    s3://bucket/path/to/object.jsonl  →  (bucket, key)
    """
    u = urlparse(s3_url)
    if u.scheme != "s3":
        raise ValueError(f"not s3 url: {s3_url}")
    bucket = u.netloc
    key = u.path.lstrip("/")
    return bucket, key


def derive_session_prefix_from_key(key: str) -> str:
    """
    M001/2025/11/26/20251126_165513_M001_anomaly_normal_000001.jsonl
    → M001/2025/11/26/20251126_165513_M001_anomaly_normal_
    (뒤의 _000001.jsonl 부분 제거)
    """
    m = re.match(r"^(.*)_\d{6}\.[^.]+$", key)
    if not m:
        # 패턴이 다르면 그냥 디렉터리 기준 prefix로 사용
        # M001/2025/11/26/20251126_165513_M001_anomaly_normal_000001.jsonl
        # → M001/2025/11/26/
        return key.rsplit("/", 1)[0] + "/"
    return m.group(1) + "_"  # 마지막 '_' 포함


def list_session_objects(client: Minio, bucket: str, prefix: str):
    """
    세션 prefix로 시작하는 .jsonl 파일들 리스트
    """
    objects = client.list_objects(bucket, prefix=prefix, recursive=False)
    return [obj.object_name for obj in objects if obj.object_name.endswith(".jsonl")]


def read_jsonl_from_minio(client: Minio, bucket: str, object_name: str) -> pd.DataFrame:
    resp = client.get_object(bucket, object_name)
    try:
        data = resp.read()
    finally:
        resp.close()
        resp.release_conn()
    buf = io.BytesIO(data)
    df = pd.read_json(buf, lines=True)
    return df


def ensure_vibration_frame_table():
    """
    raw.vibration_frame 테이블이 없으면 생성.
    (이미 있다면 스키마 변경은 하지 않음 → 기존 환경에서는 ALTER TABLE 별도 실행 필요)
    """
    ddl = """
    CREATE SCHEMA IF NOT EXISTS raw;

    CREATE TABLE IF NOT EXISTS raw.vibration_frame (
        session_id          BIGINT NOT NULL,
        device_id           TEXT NOT NULL,
        frame_seq           BIGINT NOT NULL,
        t0_utc              TIMESTAMPTZ NOT NULL,
        samples_per_frame   INTEGER NOT NULL,
        ax                  DOUBLE PRECISION[] NOT NULL,
        ay                  DOUBLE PRECISION[] NOT NULL,
        az                  DOUBLE PRECISION[] NOT NULL,
        -- 메타데이터 (NULL 허용)
        task_type           TEXT,
        label_type          TEXT,
        data_split          TEXT,
        operator            TEXT,
        PRIMARY KEY (session_id, frame_seq)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def sync_bronze_status_from_raw():
    """
    마이그레이션용:
    raw.vibration_frame 에는 데이터가 있는데, session.bronze_done_at 이 NULL인 세션들에 대해
    bronze_done_at 을 현재 시각으로 세팅해준다.
    이렇게 하면 예전 데이터에 대해서도 중복 insert / PK 충돌을 피할 수 있다.
    """
    sql = text(
        """
        UPDATE session s
        SET bronze_done_at = NOW()
        FROM (
            SELECT DISTINCT session_id
            FROM raw.vibration_frame
        ) vf
        WHERE s.id = vf.session_id
          AND s.bronze_done_at IS NULL;
        """
    )
    with engine.begin() as conn:
        conn.execute(sql)


# ----------------- 핵심 ETL 로직 -----------------


def fetch_uningested_sessions() -> pd.DataFrame:
    """
    아직 bronze 처리가 안 되었거나(force_reprocess = true 포함),
    raw_file_path가 있는 세션만 선택.
    session 테이블의 메타데이터(task_type, label_type, data_split, operator)를 함께 가져온다.
    """
    query = text(
        """
        SELECT
            s.id,
            s.device_id,
            s.raw_file_path,
            s.sample_rate_hz,
            s.channel_count,
            s.task_type,
            s.label_type,
            s.data_split,
            s.operator,
            COALESCE(s.force_reprocess, false) AS force_reprocess
        FROM session s
        WHERE s.raw_file_path IS NOT NULL
          AND (s.bronze_done_at IS NULL OR s.force_reprocess = TRUE)
        ORDER BY s.id;
        """
    )

    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)
    return df


def mark_session_bronze_done(session_id: int):
    """
    세션 bronze 처리 완료 마킹
    """
    sql = text(
        """
        UPDATE session
        SET bronze_done_at = NOW(),
            force_reprocess = FALSE
        WHERE id = :sid;
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, {"sid": session_id})


def delete_existing_frames_if_any(session_id: int):
    """
    force_reprocess = true 인 세션에 대해, 기존 frame 데이터 삭제.
    (PRIMARY KEY (session_id, frame_seq) 충돌 방지)
    """
    sql = text(
        """
        DELETE FROM raw.vibration_frame
        WHERE session_id = :sid;
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, {"sid": session_id})


def process_one_session(row, client: Minio):
    session_id = int(row["id"])
    device_id = row["device_id"]
    raw_file_path = row["raw_file_path"]
    force_reprocess = bool(row.get("force_reprocess", False))

    # 세션 메타데이터 (한 세션 전체에 동일하게 적용)
    task_type = row.get("task_type", None)
    label_type = row.get("label_type", None)
    data_split = row.get("data_split", None)
    operator = row.get("operator", None)

    print(
        f"[ETL] session_id={session_id}, device_id={device_id}, raw={raw_file_path}, "
        f"task_type={task_type}, label_type={label_type}, "
        f"data_split={data_split}, operator={operator}, "
        f"force_reprocess={force_reprocess}"
    )

    if force_reprocess:
        print(f"[ETL] force_reprocess=TRUE, deleting existing frames for session_id={session_id}")
        delete_existing_frames_if_any(session_id)

    bucket, key = parse_s3_url(raw_file_path)
    prefix = derive_session_prefix_from_key(key)

    print(f"[ETL] bucket={bucket}, prefix={prefix}")

    object_names = list_session_objects(client, bucket, prefix)
    if not object_names:
        print(f"[ETL] no objects found for session_id={session_id}")
        return

    print(f"[ETL] found {len(object_names)} objects")

    total_rows = 0

    # object 하나씩 읽어서 바로 DB로 flush
    for obj in sorted(object_names):
        df = read_jsonl_from_minio(client, bucket, obj)

        if df.empty:
            continue

        # 컬럼 체크
        if "seq" not in df.columns or "t0" not in df.columns:
            raise RuntimeError(
                f"unexpected columns in jsonl for session {session_id}, "
                f"object={obj}: {df.columns}"
            )

        # t0 → timestamp
        df["t0"] = pd.to_datetime(df["t0"], utc=True)

        # samples_per_frame = len(ax)
        df["samples_per_frame"] = df["ax"].apply(
            lambda a: len(a) if isinstance(a, (list, tuple)) else 0
        )

        # session_id / device_id 붙이기
        df["session_id"] = session_id
        df["device_id"] = device_id

        # 세션 메타데이터 붙이기 (frame별로 동일)
        df["task_type"] = task_type
        df["label_type"] = label_type
        df["data_split"] = data_split
        df["operator"] = operator

        out_df = df[
            [
                "session_id",
                "device_id",
                "seq",
                "t0",
                "samples_per_frame",
                "ax",
                "ay",
                "az",
                "task_type",
                "label_type",
                "data_split",
                "operator",
            ]
        ].rename(
            columns={
                "seq": "frame_seq",
                "t0": "t0_utc",
            }
        )

        write_frames_to_pg(out_df)
        total_rows += len(out_df)
        print(f"[ETL] session_id={session_id}, object={obj}, rows={len(out_df)}")

    print(f"[ETL] session_id={session_id} done, total_rows={total_rows}")

    if total_rows > 0:
        mark_session_bronze_done(session_id)
    else:
        print(f"[ETL] WARN: session_id={session_id} had no rows, bronze_done_at not updated")


def write_frames_to_pg(df: pd.DataFrame):
    """
    pandas → raw.vibration_frame
    Postgres array 타입에 Python list가 잘 매핑되도록 to_sql 사용
    """
    if df.empty:
        return

    from sqlalchemy import (
        Table,
        Column,
        MetaData,
        BigInteger,
        Integer,
        Text,
    )
    from sqlalchemy.dialects.postgresql import ARRAY, DOUBLE_PRECISION, TIMESTAMP

    metadata = MetaData()
    vibration_frame = Table(
        "vibration_frame",
        metadata,
        Column("session_id", BigInteger, nullable=False),
        Column("device_id", Text, nullable=False),
        Column("frame_seq", BigInteger, nullable=False),
        Column("t0_utc", TIMESTAMP(timezone=True), nullable=False),
        Column("samples_per_frame", Integer, nullable=False),
        Column("ax", ARRAY(DOUBLE_PRECISION), nullable=False),
        Column("ay", ARRAY(DOUBLE_PRECISION), nullable=False),
        Column("az", ARRAY(DOUBLE_PRECISION), nullable=False),
        # 메타데이터 컬럼 (NULL 허용)
        Column("task_type", Text, nullable=True),
        Column("label_type", Text, nullable=True),
        Column("data_split", Text, nullable=True),
        Column("operator", Text, nullable=True),
        schema="raw",
    )

    # to_sql을 쓰되, index=False + if_exists='append'
    with engine.begin() as conn:
        df.to_sql(
            name=vibration_frame.name,
            schema=vibration_frame.schema,
            con=conn,
            if_exists="append",
            index=False,
            method="multi",
        )

    print(f"[ETL] inserted {len(df)} rows into raw.vibration_frame")


def main():
    ensure_vibration_frame_table()
    # 기존 데이터에 대해 bronze_done_at 자동 동기화 (마이그레이션용)
    sync_bronze_status_from_raw()

    client = make_minio_client()

    sessions_df = fetch_uningested_sessions()
    if sessions_df.empty:
        print("[ETL] no sessions to process")
        return

    for _, row in sessions_df.iterrows():
        try:
            process_one_session(row, client)
        except Exception as ex:
            print(f"[ETL] ERROR on session_id={row['id']}: {ex}")


if __name__ == "__main__":
    main()
