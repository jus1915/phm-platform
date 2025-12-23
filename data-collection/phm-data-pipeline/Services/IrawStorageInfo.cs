namespace phm_data_pipeline.Services
{
    /// <summary>
    /// 한 측정 세션 동안 생성된 raw 데이터의 대표 경로 정보를 제공하는 인터페이스
    /// (예: MinIO object prefix, s3://bucket/path, 등)
    /// </summary>
    public interface IRawStorageInfo
    {
        /// <summary>
        /// 이 세션의 raw 데이터가 저장된 대표 경로
        /// ex) "s3://phm-raw/M001/2025/11/26/20251126_145252_M001.bin"
        /// 또는 "phm-raw/M001/2025/11/26/..." 등
        /// </summary>
        string SessionRawPath { get; }
    }
}