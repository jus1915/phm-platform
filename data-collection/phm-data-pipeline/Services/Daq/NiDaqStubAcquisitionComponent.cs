using System;
using System.Threading;
using System.Threading.Tasks;
using phm_data_pipeline;           // AppEvents.RaiseLog
using phm_data_pipeline.Models;

namespace phm_data_pipeline.Services.Daq
{
    /// <summary>
    /// 실제 NI-DAQ 장비 대신, 가짜 데이터 스트림(3축 가속도 난수 프레임)을 생성하는 Stub 구현.
    /// </summary>
    public sealed class NiDaqStubAcquisitionComponent : IAcquisitionComponent, IAccelFrameSource, IDisposable
    {
        private readonly TimeSpan _batchInterval;
        private readonly int _samplesPerBatch;
        private readonly string _name;

        // 런타임 상태
        private readonly object _syncRoot = new object();
        private CancellationTokenSource _sessionCts;
        private Task _workerTask;
        private bool _running;

        // 프레임 시퀀스
        private long _frameSeq = 0;

        // 난수 생성기
        private readonly Random _rng = new Random();

        public double SampleRate { get; private set; }
        public event Action<AccelFrame> FrameGenerated;

        /// <summary>
        /// Stub 설정.
        /// </summary>
        /// <param name="batchInterval">프레임 생성 주기.</param>
        /// <param name="samplesPerBatch">한 프레임당 샘플 수.</param>
        /// <param name="name">로그/디버깅용 이름.</param>
        public NiDaqStubAcquisitionComponent(
            TimeSpan batchInterval,
            int samplesPerBatch,
            string name)
        {
            if (batchInterval <= TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(batchInterval));
            if (samplesPerBatch <= 0)
                throw new ArgumentOutOfRangeException(nameof(samplesPerBatch));
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            _batchInterval = batchInterval;
            _samplesPerBatch = samplesPerBatch;
            _name = name;

            SampleRate = _samplesPerBatch / _batchInterval.TotalSeconds;
        }

        /// <summary>
        /// 기본값(100ms, 1000 samples/batch, 이름 "NiDaqStub")으로 Stub 생성.
        /// </summary>
        public NiDaqStubAcquisitionComponent()
            : this(TimeSpan.FromMilliseconds(100), 1000, "NiDaqStub")
        {
        }
        public void SetSampleRate(double newRate)
        {
            SampleRate = newRate;
        }

        public Task<AcquisitionHandle> StartAcquisitionAsync(
            DaqAccelConfig cfg)
        {
            return StartAcquisitionAsync(cfg, null, CancellationToken.None);
        }

        public Task<AcquisitionHandle> StartAcquisitionAsync(
            DaqAccelConfig cfg,
            IProgress<AcquisitionProgress> progress)
        {
            return StartAcquisitionAsync(cfg, progress, CancellationToken.None);
        }

        public Task<AcquisitionHandle> StartAcquisitionAsync(
            DaqAccelConfig cfg,
            IProgress<AcquisitionProgress> progress,
            CancellationToken cancellationToken)
        {
            if (cfg == null)
                throw new ArgumentNullException(nameof(cfg));

            // 시작 전에 취소되었으면 바로 취소 Task 리턴
            if (cancellationToken.IsCancellationRequested)
            {
                var tcs = new TaskCompletionSource<AcquisitionHandle>();
                tcs.SetCanceled();
                return tcs.Task;
            }

            lock (_syncRoot)
            {
                if (_running)
                    throw new InvalidOperationException("Stub 수집이 이미 실행 중입니다.");

                // 외부 토큰과 연결된 CTS
                _sessionCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                _running = true;
                _frameSeq = 0;
            }

            // stopFunc 정의 (필드 기반으로 안전하게 참조)
            Func<CancellationToken, Task> stopFunc = async ct =>
            {
                CancellationTokenSource cts;
                Task worker;

                lock (_syncRoot)
                {
                    cts = _sessionCts;
                    worker = _workerTask;
                }

                try
                {
                    if (cts != null && !cts.IsCancellationRequested)
                        cts.Cancel();

                    if (worker != null)
                        await worker.ConfigureAwait(false);
                }
                catch
                {
                    // Stop 시점의 예외는 상위로 전파하지 않음
                }
            };

            // 세션 핸들 생성
            var handle = new AcquisitionHandle(cfg, stopFunc);
            handle.State = AcquisitionState.Running;

            // 백그라운드로 가짜 데이터 수집 루프 시작
            _workerTask = Task.Run(async () =>
            {
                long totalSamples = 0; // "채널당" 샘플 기준 (프로그레스용)
                int batchCount = 0;
                CancellationToken token;

                lock (_syncRoot)
                {
                    token = _sessionCts.Token;
                }

                AppEvents.RaiseLog(
                    string.Format("[Stub] {0} START (interval={1} ms, samples/batch={2})",
                        _name,
                        (int)_batchInterval.TotalMilliseconds,
                        _samplesPerBatch));

                try
                {
                    while (!token.IsCancellationRequested)
                    {
                        await Task.Delay(_batchInterval, token).ConfigureAwait(false);

                        // ===== 1) 프레임 생성 (3축 난수) =====
                        var now = DateTimeOffset.Now;
                        long seq = Interlocked.Increment(ref _frameSeq);

                        double[] ax = new double[_samplesPerBatch];
                        double[] ay = new double[_samplesPerBatch];
                        double[] az = new double[_samplesPerBatch];

                        for (int i = 0; i < _samplesPerBatch; i++)
                        {
                            ax[i] = NextGaussian() * 0.3;
                            ay[i] = NextGaussian() * 0.3;
                            az[i] = NextGaussian() * 0.3;
                        }

                        var frame = new AccelFrame
                        {
                            Seq = seq,
                            Timestamp = now,
                            Ax = ax,
                            Ay = ay,
                            Az = az
                        };

                        var handler = FrameGenerated;
                        if (handler != null)
                        {
                            try
                            {
                                handler(frame);
                            }
                            catch
                            {
                                // 리스너 예외로 Stub 죽이지 않음
                            }
                        }

                        // ===== 2) 진행상태 업데이트 =====
                        batchCount++;
                        totalSamples += _samplesPerBatch; // 채널당 샘플 기준

                        if (progress != null)
                        {
                            var report = new AcquisitionProgress
                            {
                                TotalSamplesSent = totalSamples,
                                LastBatchAt = now,
                                Message = string.Format(
                                    "{0} running (Stub). Seq={1}, TotalSamples={2}",
                                    _name,
                                    seq,
                                    totalSamples)
                            };

                            progress.Report(report);
                        }

                        // 너무 시끄럽지 않게, 10번째 배치마다 한 번 로그
                        if (batchCount % 10 == 0)
                        {
                            AppEvents.RaiseLog(
                                string.Format("[Stub] {0} batch x{1} (seq={2}, totalSamples={3})",
                                    _name,
                                    batchCount,
                                    seq,
                                    totalSamples));
                        }
                    }

                    handle.State = AcquisitionState.Canceled;
                }
                catch (TaskCanceledException)
                {
                    handle.State = AcquisitionState.Canceled;
                }
                catch (Exception ex)
                {
                    handle.State = AcquisitionState.Faulted;
                    handle.LastError = ex;

                    AppEvents.RaiseLog(
                        string.Format("[Stub] {0} ERROR: {1}", _name, ex.Message));
                }
                finally
                {
                    lock (_syncRoot)
                    {
                        _running = false;

                        if (_sessionCts != null)
                        {
                            _sessionCts.Dispose();
                            _sessionCts = null;
                        }

                        _workerTask = null;
                    }

                    if (handle.State == AcquisitionState.Running)
                    {
                        // 여기까지 왔는데 아직 Running이면 Completed로 간주
                        handle.State = AcquisitionState.Completed;
                    }

                    AppEvents.RaiseLog(
                        string.Format("[Stub] {0} STOP (State={1}, TotalSamples={2})",
                            _name,
                            handle.State,
                            totalSamples));
                }
            });

            // 지금 단계에서는 바로 핸들을 반환
            return Task.FromResult(handle);
        }

        // ===== 간단한 정규분포 난수 (Box-Muller) =====

        private double NextGaussian()
        {
            // 평균 0, 표준편차 1 약식 구현
            // (Thread-safe를 위해 _rng는 한 스레드에서만 사용)
            double u1 = 1.0 - _rng.NextDouble(); // (0,1]
            double u2 = 1.0 - _rng.NextDouble();
            double r = Math.Sqrt(-2.0 * Math.Log(u1));
            double theta = 2.0 * Math.PI * u2;
            return r * Math.Cos(theta);
        }

        // ===== 내부 정리 & IDisposable =====

        private void InternalStop()
        {
            CancellationTokenSource cts;
            Task worker;

            lock (_syncRoot)
            {
                cts = _sessionCts;
                worker = _workerTask;
            }

            try
            {
                if (cts != null && !cts.IsCancellationRequested)
                    cts.Cancel();

                if (worker != null)
                    worker.GetAwaiter().GetResult();
            }
            catch
            {
                // 무시
            }
            finally
            {
                lock (_syncRoot)
                {
                    _running = false;

                    if (_sessionCts != null)
                    {
                        _sessionCts.Dispose();
                        _sessionCts = null;
                    }

                    _workerTask = null;
                }
            }
        }

        public void Dispose()
        {
            InternalStop();
        }
    }
}
