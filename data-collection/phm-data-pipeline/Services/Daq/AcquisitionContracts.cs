using phm_data_pipeline.Models;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace phm_data_pipeline.Services.Daq
{
    /// <summary>
    /// 수집 세션의 상태.
    /// </summary>
    public enum AcquisitionState
    {
        Created,
        Running,
        Completed,
        Canceled,
        Faulted
    }

    /// <summary>
    /// 수집 진행 상황 리포트용 DTO.
    /// </summary>
    public sealed class AcquisitionProgress
    {
        /// <summary>
        /// 지금까지 전송된 전체 샘플 수.
        /// </summary>
        public long TotalSamplesSent { get; set; }

        /// <summary>
        /// 마지막 배치가 전송된 시각.
        /// </summary>
        public DateTimeOffset LastBatchAt { get; set; }

        /// <summary>
        /// 디버깅/로그용 메시지.
        /// </summary>
        public string Message { get; set; }
    }

    /// <summary>
    /// 수집 세션 핸들.
    /// - StopAsync로 중단
    /// - State/LastError로 상태 확인
    /// </summary>
    public sealed class AcquisitionHandle : IDisposable
    {
        private readonly Func<CancellationToken, Task> _stopFunc;

        /// <summary>
        /// 세션 식별용 Id.
        /// </summary>
        public Guid Id { get; private set; }

        /// <summary>
        /// 세션 시작 시 사용된 설정.
        /// </summary>
        public DaqAccelConfig Config { get; private set; }

        /// <summary>
        /// 세션 시작 시간.
        /// </summary>
        public DateTimeOffset StartedAt { get; private set; }

        /// <summary>
        /// 현재 상태.
        /// 구현체에서 적절히 업데이트.
        /// </summary>
        public AcquisitionState State { get; internal set; }

        /// <summary>
        /// 실패 시 예외 정보.
        /// </summary>
        public Exception LastError { get; internal set; }

        /// <summary>
        /// Completed / Canceled / Faulted 중 하나이면 true.
        /// </summary>
        public bool IsFinished
        {
            get
            {
                return State == AcquisitionState.Completed
                    || State == AcquisitionState.Canceled
                    || State == AcquisitionState.Faulted;
            }
        }

        public AcquisitionHandle(
            DaqAccelConfig config,
            Func<CancellationToken, Task> stopFunc)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            if (stopFunc == null) throw new ArgumentNullException(nameof(stopFunc));

            Id = Guid.NewGuid();
            Config = config;
            StartedAt = DateTimeOffset.Now;
            State = AcquisitionState.Created;
            _stopFunc = stopFunc;
        }

        /// <summary>
        /// 수집을 중단한다.
        /// 이미 종료된 경우 구현체에서 적절히 무시하도록 하면 된다.
        /// </summary>
        public Task StopAsync(CancellationToken cancellationToken)
        {
            return _stopFunc(cancellationToken);
        }

        public Task StopAsync()
        {
            return StopAsync(CancellationToken.None);
        }

        /// <summary>
        /// Dispose에서 StopAsync를 동기적으로 호출한다.
        /// UI 스레드에서 Deadlock 위험이 있으니, 가능하면
        /// using 패턴 대신 직접 StopAsync를 await 하는 것을 권장.
        /// </summary>
        public void Dispose()
        {
            if (State == AcquisitionState.Running)
            {
                try
                {
                    // 주의: 동기 Wait()이므로 호출 위치에 따라 Deadlock 가능성 있음.
                    _stopFunc(CancellationToken.None).GetAwaiter().GetResult();
                }
                catch
                {
                    // Dispose에서는 예외를 다시 던지지 않음.
                    // 실제 로깅은 구현체 측에서 수행.
                }
            }
        }
    }

    /// <summary>
    /// 수집 구성요소 공통 인터페이스.
    /// - NI-DAQ Stub
    /// - 실제 NI-DAQ
    /// - Kafka Sender
    /// - MinIO Sender
    /// 등에서 구현.
    /// </summary>
    public interface IAcquisitionComponent
    {
        /// <summary>
        /// 가속도 데이터 수집(or 전송)을 시작한다.
        /// </summary>
        /// <param name="cfg">수집 설정 (채널, 샘플링, Kafka/MinIO 설정 포함)</param>
        /// <param name="progress">옵션. 진행 상황 리포트. (null 가능)</param>
        /// <param name="cancellationToken">수집 시작 자체를 취소하고 싶을 때 사용.</param>
        /// <returns>중단/상태 조회용 세션 핸들.</returns>
        Task<AcquisitionHandle> StartAcquisitionAsync(
            DaqAccelConfig cfg,
            IProgress<AcquisitionProgress> progress,
            CancellationToken cancellationToken);

        Task<AcquisitionHandle> StartAcquisitionAsync(
            DaqAccelConfig cfg,
            IProgress<AcquisitionProgress> progress);

        Task<AcquisitionHandle> StartAcquisitionAsync(
            DaqAccelConfig cfg);
    }
}
