using System;
using Npgsql;
using phm_data_pipeline.Models;

namespace phm_data_pipeline.Services
{
    public class PgFrameSink : IDisposable
    {
        private readonly string _connectionString;
        private long _currentSessionId = -1;

        public PgFrameSink(string connectionString)
        {
            _connectionString = connectionString;
            EnsureTables();
        }

        private void EnsureTables()
        {
            using var conn = new NpgsqlConnection(_connectionString);
            conn.Open();

            // -------------------------
            // 1) session 테이블
            // -------------------------
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = @"
                CREATE TABLE IF NOT EXISTS session (
                    id              BIGSERIAL PRIMARY KEY,

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

                    task_type       TEXT,
                    label_type      TEXT,
                    data_split      TEXT,
                    operator        TEXT,
                    note            TEXT,

                    -- 프레임 / 이벤트 단위 수집 구분
                    acquisition_unit TEXT,     -- 'frame' 또는 'event' 등

                    bronze_done_at   TIMESTAMPTZ,
                    feature_done_at  TIMESTAMPTZ,
                    train_done_at    TIMESTAMPTZ,
                    force_reprocess  BOOLEAN DEFAULT false,

                    created_at       TIMESTAMPTZ DEFAULT now()
                );";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = @"
                    ALTER TABLE session
                    ADD COLUMN IF NOT EXISTS acquisition_unit TEXT;";
                cmd.ExecuteNonQuery();
            }

            // -------------------------
            // 2) acquisition_event 테이블
            //    (이벤트 단위 수집 메타데이터 저장용)
            // -------------------------
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = @"
                CREATE TABLE IF NOT EXISTS acquisition_event (
                    id                  BIGSERIAL PRIMARY KEY,
                    session_id          BIGINT NOT NULL REFERENCES session(id) ON DELETE CASCADE,

                    event_index         INTEGER NOT NULL,        -- 세션 내 이벤트 번호 (0,1,2,...)
                    start_sample        BIGINT NOT NULL,         -- 세션 시작 기준 샘플 인덱스
                    end_sample          BIGINT NOT NULL,

                    start_time_utc      TIMESTAMPTZ,             -- 선택: 시간 계산 가능하면 저장
                    end_time_utc        TIMESTAMPTZ,

                    max_abs_value       DOUBLE PRECISION,        -- 이벤트 구간 내 최대 절대값
                    max_abs_channel     INTEGER,                 -- 어느 채널에서 나왔는지 (0/1/2 ...)

                    rms_x               DOUBLE PRECISION,
                    rms_y               DOUBLE PRECISION,
                    rms_z               DOUBLE PRECISION,

                    task_type           TEXT,
                    label_type          TEXT,
                    data_split          TEXT,

                    created_at          TIMESTAMPTZ DEFAULT now()
                );";
                cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// 수집 시작 시 세션 메타정보를 DB에 기록하고, 생성된 session.id를 내부에 보관
        /// </summary>
        public long OnSessionStarted(MeasurementSessionInfo session)
        {
            using var conn = new NpgsqlConnection(_connectionString);
            conn.Open();

            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"
                INSERT INTO session
                (device_id, name, started_at_utc, sample_rate_hz, channel_count,
                 device_name, location, raw_file_path, comment,
                 task_type, label_type, data_split, operator, note,
                 acquisition_unit)
                VALUES
                (@device_id, @name, @started_at_utc, @sample_rate_hz, @channel_count,
                 @device_name, @location, @raw_file_path, @comment,
                 @task_type, @label_type, @data_split, @operator, @note,
                 @acquisition_unit)
                RETURNING id;";

            cmd.Parameters.AddWithValue("@device_id", (object?)session.DeviceId ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@name", (object?)session.Name ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@started_at_utc", session.StartedAtUtc);
            cmd.Parameters.AddWithValue("@sample_rate_hz", session.SampleRateHz);
            cmd.Parameters.AddWithValue("@channel_count", session.ChannelCount);
            cmd.Parameters.AddWithValue("@device_name", (object?)session.DeviceName ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@location", (object?)session.Location ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@raw_file_path", (object?)session.RawFilePath ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@comment", (object?)session.Comment ?? DBNull.Value);

            cmd.Parameters.AddWithValue("@task_type", (object?)session.TaskType ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@label_type", (object?)session.LabelType ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@data_split", (object?)session.DataSplit ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@operator", (object?)session.Operator ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@note", (object?)session.Note ?? DBNull.Value);

            // 프레임 / 이벤트 단위 수집 구분
            cmd.Parameters.AddWithValue("@acquisition_unit",
                (object?)session.AcquisitionUnit ?? DBNull.Value);

            _currentSessionId = (long)cmd.ExecuteScalar();
            return _currentSessionId;
        }

        /// <summary>
        /// 수집 종료 시 ended_at_utc만 업데이트
        /// </summary>
        public void OnSessionStopped()
        {
            if (_currentSessionId <= 0)
                return;

            using var conn = new NpgsqlConnection(_connectionString);
            conn.Open();

            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"
                UPDATE session
                SET ended_at_utc = @ended_at_utc
                WHERE id = @id;";

            cmd.Parameters.AddWithValue("@ended_at_utc", DateTime.UtcNow);
            cmd.Parameters.AddWithValue("@id", _currentSessionId);

            cmd.ExecuteNonQuery();
            _currentSessionId = -1;
        }

        /// <summary>
        /// 이벤트 단위 수집 시, 각 이벤트의 메타데이터를 저장
        /// AccelEvent 모델은 다음 필드를 가진다고 가정:
        ///   long SessionId
        ///   int EventIndex
        ///   long StartSampleIndex, EndSampleIndex
        ///   DateTime? StartTimeUtc, EndTimeUtc
        ///   double MaxAbsValue
        ///   int MaxAbsChannel
        ///   double RmsX, RmsY, RmsZ
        ///   string TaskType, LabelType, DataSplit
        /// </summary>
        public void InsertEvent(AccelEvent ev)
        {
            if (ev == null)
                throw new ArgumentNullException(nameof(ev));

            using var conn = new NpgsqlConnection(_connectionString);
            conn.Open();

            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"
                INSERT INTO acquisition_event
                (session_id,
                 event_index,
                 start_sample,
                 end_sample,
                 start_time_utc,
                 end_time_utc,
                 max_abs_value,
                 max_abs_channel,
                 rms_x,
                 rms_y,
                 rms_z,
                 task_type,
                 label_type,
                 data_split)
                VALUES
                (@session_id,
                 @event_index,
                 @start_sample,
                 @end_sample,
                 @start_time_utc,
                 @end_time_utc,
                 @max_abs_value,
                 @max_abs_channel,
                 @rms_x,
                 @rms_y,
                 @rms_z,
                 @task_type,
                 @label_type,
                 @data_split);";

            cmd.Parameters.AddWithValue("@session_id", ev.SessionId);
            cmd.Parameters.AddWithValue("@event_index", ev.EventIndex);
            cmd.Parameters.AddWithValue("@start_sample", ev.StartSampleIndex);
            cmd.Parameters.AddWithValue("@end_sample", ev.EndSampleIndex);

            cmd.Parameters.AddWithValue("@start_time_utc",
                (object?)ev.StartTimeUtc ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@end_time_utc",
                (object?)ev.EndTimeUtc ?? DBNull.Value);

            cmd.Parameters.AddWithValue("@max_abs_value", ev.MaxAbsValue);
            cmd.Parameters.AddWithValue("@max_abs_channel",
                (object?)ev.MaxAbsChannel ?? DBNull.Value);

            cmd.Parameters.AddWithValue("@rms_x", ev.RmsX);
            cmd.Parameters.AddWithValue("@rms_y", ev.RmsY);
            cmd.Parameters.AddWithValue("@rms_z", ev.RmsZ);

            cmd.Parameters.AddWithValue("@task_type",
                (object?)ev.TaskType ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@label_type",
                (object?)ev.LabelType ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@data_split",
                (object?)ev.DataSplit ?? DBNull.Value);

            cmd.ExecuteNonQuery();
        }

        public void UpdateRawFilePath(string rawFilePath)
        {
            if (_currentSessionId <= 0)
                return;

            using var conn = new NpgsqlConnection(_connectionString);
            conn.Open();

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = @"
                    UPDATE session
                    SET raw_file_path = @path
                    WHERE id = @id;";

                cmd.Parameters.AddWithValue("@path", (object)rawFilePath ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@id", _currentSessionId);

                cmd.ExecuteNonQuery();
            }
        }

        public void Dispose()
        {
            // 나중에 필요하면 정리 로직 추가
        }
    }
}
