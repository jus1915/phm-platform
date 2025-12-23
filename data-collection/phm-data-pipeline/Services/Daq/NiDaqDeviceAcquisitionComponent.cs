using System;
using System.Threading;
using NationalInstruments.DAQmx;
using phm_data_pipeline.Models;

// Alias
using DaqTask = NationalInstruments.DAQmx.Task;
using SysTask = System.Threading.Tasks.Task;

namespace phm_data_pipeline.Services.Daq
{
    /// <summary>
    /// 실제 NI-DAQ(IEPE 가속도 센서)을 사용해 3축 가속도 프레임을 읽어오는 컴포넌트.
    /// DaqAccelHttpSender와 동일한 센서(±25 g, IEPE 4mA, mV/g) 기준 설정.
    /// </summary>
    public sealed class NiDaqDeviceAcquisitionComponent : IAcquisitionComponent, IAccelFrameSource, IDisposable
    {
        private readonly object _syncRoot = new object();

        private DaqTask _task;
        private AnalogMultiChannelReader _reader;

        private CancellationTokenSource _sessionCts;
        private SysTask _workerTask;
        private bool _running;

        private long _seq;
        private long _totalSamples;

        // IAccelFrameSource
        public double SampleRate { get; private set; }
        public event Action<AccelFrame> FrameGenerated;
        private const double DefaultSampleRateHz = 12800.0;

        public string Name { get; }

        // ===== 센서/입력 설정 (DaqAccelHttpSender와 동일한 기본값) =====
        /// <summary>가속도 최소 g 값 (예: -25 g)</summary>
        public double MinG { get; set; } = -25.0;

        /// <summary>가속도 최대 g 값 (예: +25 g)</summary>
        public double MaxG { get; set; } = 25.0;

        /// <summary>IEPE 전류 (mA)</summary>
        public double IepeMilliAmps { get; set; } = 4.0;

        /// <summary>X축 민감도 (mV/g)</summary>
        public double SensitivityX_mVpg { get; set; } = 100.0;

        /// <summary>Y축 민감도 (mV/g)</summary>
        public double SensitivityY_mVpg { get; set; } = 100.0;

        /// <summary>Z축 민감도 (mV/g)</summary>
        public double SensitivityZ_mVpg { get; set; } = 100.0;

        /// <summary>X축 오프셋 (g 단위)</summary>
        public double OffsetX_g { get; set; } = 0.0;

        /// <summary>Y축 오프셋 (g 단위)</summary>
        public double OffsetY_g { get; set; } = 0.0;

        /// <summary>Z축 오프셋 (g 단위)</summary>
        public double OffsetZ_g { get; set; } = 0.0;

        public NiDaqDeviceAcquisitionComponent(string name)
        {
            if (name == null) throw new ArgumentNullException("name");
            Name = name;
            SampleRate = DefaultSampleRateHz;
        }

        public NiDaqDeviceAcquisitionComponent()
            : this("DeviceDAQ")
        {
        }

        public void SetSampleRate(double newRateHz)
        {
            if (newRateHz <= 0)
                throw new ArgumentOutOfRangeException(nameof(newRateHz));

            SampleRate = newRateHz;
        }

        // ===== IAcquisitionComponent 오버로드 3개 =====

        public System.Threading.Tasks.Task<AcquisitionHandle> StartAcquisitionAsync(DaqAccelConfig cfg)
        {
            return StartAcquisitionAsync(cfg, null, CancellationToken.None);
        }

        public System.Threading.Tasks.Task<AcquisitionHandle> StartAcquisitionAsync(
            DaqAccelConfig cfg,
            IProgress<AcquisitionProgress> progress)
        {
            return StartAcquisitionAsync(cfg, progress, CancellationToken.None);
        }

        public System.Threading.Tasks.Task<AcquisitionHandle> StartAcquisitionAsync(
            DaqAccelConfig cfg,
            IProgress<AcquisitionProgress> progress,
            CancellationToken cancellationToken)
        {
            if (cfg == null)
                throw new ArgumentNullException("cfg");

            if (cfg.Daq == null)
                throw new ArgumentException("cfg.Daq 가 null 입니다.", "cfg");

            if (string.IsNullOrWhiteSpace(cfg.Daq.Channels))
                throw new ArgumentException("DAQ Channels(ai0:2 등)이 비어 있습니다.", "cfg");

            if (cfg.Daq.SamplingRate <= 0)
                throw new ArgumentException("SamplingRate 는 0보다 커야 합니다.", "cfg");

            if (cfg.Daq.FrameSize <= 0)
                throw new ArgumentException("FrameSize 는 0보다 커야 합니다.", "cfg");

            if (cancellationToken.IsCancellationRequested)
            {
                var tcs = new System.Threading.Tasks.TaskCompletionSource<AcquisitionHandle>();
                tcs.SetCanceled();
                return tcs.Task;
            }

            lock (_syncRoot)
            {
                if (_running)
                    throw new InvalidOperationException("NiDaqDeviceAcquisitionComponent 가 이미 실행 중입니다.");

                _sessionCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                _running = true;
            }

            string device = (cfg.Daq.Device ?? "").Trim();
            string channels = (cfg.Daq.Channels ?? "").Trim(); // 예: "ai0:2" 또는 "cDAQ1Mod1/ai0:2"
            int fs = cfg.Daq.SamplingRate;
            int frameSize = cfg.Daq.FrameSize;

            // ===== 물리 채널 파싱 =====
            // - Channels 에 전체 물리 채널(cDAQ1Mod1/ai0:2) 이 들어 있으면 거기서 모듈/ai0 시작 인덱스 추출
            // - 아니면 Device 를 모듈로 보고 Channels("ai0:2") 에서 시작 인덱스 추출
            string moduleName;
            string chanSpec;

            if (channels.IndexOf('/') >= 0)
            {
                int slash = channels.IndexOf('/');
                moduleName = channels.Substring(0, slash);         // "cDAQ1Mod1"
                chanSpec = channels.Substring(slash + 1);          // "ai0:2"
                if (string.IsNullOrEmpty(moduleName))
                    moduleName = device;
            }
            else
            {
                moduleName = device;
                chanSpec = channels;                               // "ai0:2"
            }

            if (string.IsNullOrEmpty(moduleName))
                throw new ArgumentException("DAQ 모듈/디바이스 이름을 찾을 수 없습니다. Device 또는 Channels에 모듈명을 입력하세요.", "cfg");

            int baseIndex = ParseAiBaseIndex(chanSpec);            // ai0:2 → 0

            string physX = moduleName + "/ai" + baseIndex;         // cDAQ1Mod1/ai0
            string physY = moduleName + "/ai" + (baseIndex + 1);   // cDAQ1Mod1/ai1
            string physZ = moduleName + "/ai" + (baseIndex + 2);   // cDAQ1Mod1/ai2

            try
            {
                _task = new DaqTask(Name + "_AI");

                // 가속도(IEPE) 채널 3개 생성 (X/Y/Z)
                CreateAccelChannel(physX, SensitivityX_mVpg);
                CreateAccelChannel(physY, SensitivityY_mVpg);
                CreateAccelChannel(physZ, SensitivityZ_mVpg);

                // 버퍼는 프레임 * 10 정도
                int bufferSize = frameSize * 10;
                _task.Timing.ConfigureSampleClock(
                    "",
                    fs,
                    SampleClockActiveEdge.Rising,
                    SampleQuantityMode.ContinuousSamples,
                    bufferSize);

                _task.Stream.Timeout = 5000; // ms

                _reader = new AnalogMultiChannelReader(_task.Stream);

                _task.Start();

                _seq = 0;
                _totalSamples = 0;

                AppEvents.RaiseLog(
                    string.Format("[DAQ] {0} START (phys=[{1},{2},{3}], fs={4}, frameSize={5}, Iepe={6}mA, Range={7}~{8} g)",
                        Name,
                        physX, physY, physZ,
                        fs,
                        frameSize,
                        IepeMilliAmps,
                        MinG, MaxG));
            }
            catch (Exception ex)
            {
                SafeCleanupTask();
                lock (_syncRoot)
                {
                    _running = false;
                    if (_sessionCts != null)
                    {
                        _sessionCts.Dispose();
                        _sessionCts = null;
                    }
                }

                var tcs = new System.Threading.Tasks.TaskCompletionSource<AcquisitionHandle>();
                tcs.SetException(ex);
                return tcs.Task;
            }

            // stopFunc 정의
            Func<CancellationToken, System.Threading.Tasks.Task> stopFunc = async ct =>
            {
                CancellationTokenSource cts;
                SysTask worker;

                lock (_syncRoot)
                {
                    cts = _sessionCts;
                    worker = _workerTask;
                }

                try
                {
                    if (cts != null && !cts.IsCancellationRequested)
                        cts.Cancel();

                    // DAQ Task를 강제 중단하여 Read가 빠져나오도록
                    SafeAbortTask();

                    if (worker != null)
                        await worker.ConfigureAwait(false);
                }
                catch
                {
                    // Stop 중 예외는 상위로 올리지 않음
                }
            };

            var handle = new AcquisitionHandle(cfg, stopFunc);
            handle.State = AcquisitionState.Running;

            // 백그라운드 수집 루프
            _workerTask = SysTask.Run(() =>
            {
                RunAcquisitionLoop(handle, fs, frameSize, progress);
            });

            return System.Threading.Tasks.Task.FromResult(handle);
        }

        // ===== 실제 수집 루프 =====

        private void RunAcquisitionLoop(
            AcquisitionHandle handle,
            int sampleRate,
            int frameSize,
            IProgress<AcquisitionProgress> progress)
        {
            CancellationToken token;
            lock (_syncRoot)
            {
                token = _sessionCts != null ? _sessionCts.Token : CancellationToken.None;
            }

            try
            {
                while (!token.IsCancellationRequested)
                {
                    // frameSize 만큼 블록 읽기 (채널 x 샘플)
                    double[,] block = _reader.ReadMultiSample(frameSize);

                    int chCount = block.GetLength(0);
                    int n = block.GetLength(1); // 보통 frameSize

                    if (n <= 0 || chCount <= 0)
                        continue;

                    // 우리는 X/Y/Z 3채널을 사용 (0,1,2)
                    int chX = 0;
                    int chY = chCount > 1 ? 1 : 0;
                    int chZ = chCount > 2 ? 2 : (chCount > 1 ? 1 : 0);

                    double[] ax = new double[n];
                    double[] ay = new double[n];
                    double[] az = new double[n];

                    for (int i = 0; i < n; i++)
                    {
                        ax[i] = block[chX, i] - OffsetX_g;
                        ay[i] = block[chY, i] - OffsetY_g;
                        az[i] = block[chZ, i] - OffsetZ_g;
                    }

                    long seq = System.Threading.Interlocked.Increment(ref _seq);
                    long incSamples = n;
                    long total = System.Threading.Interlocked.Add(ref _totalSamples, incSamples);

                    var frame = new AccelFrame
                    {
                        Seq = seq,
                        Timestamp = DateTimeOffset.Now,
                        Ax = ax,
                        Ay = ay,
                        Az = az
                    };

                    // RMS 로그 (Stub와 형식 맞춤)
                    double rmsX = Rms(ax);
                    double rmsY = Rms(ay);
                    double rmsZ = Rms(az);

                    //AppEvents.RaiseLog(
                    //    string.Format("[DeviceFrame] seq={0}, RMS=({1:0.000}, {2:0.000}, {3:0.000})",
                    //        seq, rmsX, rmsY, rmsZ));

                    // 이벤트 발행 (Kafka/MinIO 등에서 구독)
                    var handler = FrameGenerated;
                    if (handler != null)
                    {
                        try
                        {
                            handler(frame);
                        }
                        catch
                        {
                            // 리스너 예외는 여기서 삼킴 (수집은 계속)
                        }
                    }

                    // 10프레임 단위 배치 로그 / Progress 보고
                    if (seq % 10 == 0)
                    {
                        //AppEvents.RaiseLog(
                        //    string.Format("[DAQ] {0} batch x{1} (seq={2}, totalSamples={3})",
                        //        Name,
                        //        seq,
                        //        seq,
                        //        total));

                        if (progress != null)
                        {
                            var report = new AcquisitionProgress
                            {
                                TotalSamplesSent = total,
                                LastBatchAt = DateTimeOffset.Now,
                                Message = string.Format(
                                    "{0} running (Device). seq={1}, totalSamples={2}",
                                    Name,
                                    seq,
                                    total)
                            };
                            progress.Report(report);
                        }
                    }
                }

                handle.State = AcquisitionState.Canceled;
            }
            catch (DaqException ex)
            {
                if (_sessionCts != null && _sessionCts.IsCancellationRequested)
                {
                    handle.State = AcquisitionState.Canceled;
                }
                else
                {
                    handle.State = AcquisitionState.Faulted;
                    handle.LastError = ex;

                    AppEvents.RaiseLog(
                        string.Format("[DAQ] {0} ERROR (DaqException): {1}", Name, ex.Message));
                }
            }
            catch (Exception ex)
            {
                handle.State = AcquisitionState.Faulted;
                handle.LastError = ex;

                AppEvents.RaiseLog(
                    string.Format("[DAQ] {0} ERROR: {1}", Name, ex.Message));
            }
            finally
            {
                SafeCleanupTask();

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
                    handle.State = AcquisitionState.Completed;

                long finalSamples = System.Threading.Interlocked.Read(ref _totalSamples);

                AppEvents.RaiseLog(
                    string.Format("[DAQ] {0} STOP (State={1}, TotalSamples={2})",
                        Name,
                        handle.State,
                        finalSamples));
            }
        }

        // ===== DAQ Task 정리 유틸 =====

        private void SafeAbortTask()
        {
            try
            {
                if (_task != null)
                    _task.Control(TaskAction.Abort);
            }
            catch
            {
            }
        }

        private void SafeCleanupTask()
        {
            try
            {
                if (_task != null)
                {
                    try
                    {
                        _task.Stop();
                    }
                    catch
                    {
                    }

                    _task.Dispose();
                    _task = null;
                }
            }
            catch
            {
            }

            _reader = null;
        }

        // ===== 유틸 =====

        private void CreateAccelChannel(string phys, double sens_mVpg)
        {
            _task.AIChannels.CreateAccelerometerChannel(
                phys,
                "",
                AITerminalConfiguration.Pseudodifferential,
                MinG,
                MaxG,
                sens_mVpg,
                AIAccelerometerSensitivityUnits.MillivoltsPerG,
                AIExcitationSource.Internal,
                IepeMilliAmps / 1000.0,
                AIAccelerationUnits.G
            );
        }

        private static int ParseAiBaseIndex(string chanSpec)
        {
            // "ai0:2" → 0, "ai4:6" → 4, "ai1" → 1
            if (string.IsNullOrWhiteSpace(chanSpec))
                return 0;

            chanSpec = chanSpec.Trim();

            if (chanSpec.StartsWith("ai", StringComparison.OrdinalIgnoreCase))
            {
                string rest = chanSpec.Substring(2);
                int colon = rest.IndexOf(':');
                string startPart = colon >= 0 ? rest.Substring(0, colon) : rest;

                int startIndex;
                if (int.TryParse(startPart, out startIndex))
                    return startIndex;
            }

            return 0;
        }

        private static double Rms(double[] a)
        {
            if (a == null || a.Length == 0) return 0.0;

            double s = 0.0;
            for (int i = 0; i < a.Length; i++)
                s += a[i] * a[i];

            return Math.Sqrt(s / a.Length);
        }

        // ===== IDisposable =====

        private void InternalStop()
        {
            CancellationTokenSource cts;
            SysTask worker;

            lock (_syncRoot)
            {
                cts = _sessionCts;
                worker = _workerTask;
            }

            try
            {
                if (cts != null && !cts.IsCancellationRequested)
                    cts.Cancel();

                SafeAbortTask();

                if (worker != null)
                    worker.GetAwaiter().GetResult();
            }
            catch
            {
            }
            finally
            {
                SafeCleanupTask();

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
