using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using phm_data_pipeline;           // AppEvents.RaiseLog
using phm_data_pipeline.Models;    // DaqAccelConfig, AcquisitionHandle, AcquisitionState, AccelFrame, IAccelFrameSource, AcquisitionProgress
using phm_data_pipeline.Services;
using Minio;
using Minio.DataModel.Args;

namespace phm_data_pipeline.Services.Daq
{
    /// <summary>
    /// IAccelFrameSource로부터 프레임을 받아 MinIO로 전송하는 컴포넌트.
    /// - MinIO 쪽은 "프레임 스트림"만 보고 저장한다.
    /// - 이벤트 검출은 UI(DaqAccelSenderForm)의 SampleEventDetector에서만 수행.
    /// - SaveIntervalMinutes(시간) + MaxBytesPerFile(크기) 기준으로 파일을 롤링.
    /// </summary>
    public sealed class MinioSenderAcquisitionComponent : IAcquisitionComponent, IDisposable
    {
        private readonly object _syncRoot = new object();
        private CancellationTokenSource _sessionCts;
        private Task _workerTask;
        private bool _running;

        private readonly string _name;
        private readonly IAccelFrameSource _frameSource;

        private string _sessionRawPath;
        public string SessionRawPath => _sessionRawPath;

        private bool _hasSuccessfulUpload;
        public bool HasSuccessfulUpload => _hasSuccessfulUpload;

        private DateTime _sessionStartLocal;
        private string _sessionObjectPrefix;

        private readonly BlockingCollection<AccelFrame> _queue;
        private readonly Action<AccelFrame> _onFrameGenerated;
        private readonly string _spoolDir;

        private IMinioClient _minioClient;

        private readonly BlockingCollection<UploadJob> _uploadQueue;
        private Task _uploadTask;

        private sealed class UploadJob
        {
            public string Bucket { get; set; }
            public string ObjectName { get; set; }
            public string FilePath { get; set; }
        }
        /// <summary>
        /// 파일 롤링 기준 (대략적인 바이트 수)
        /// </summary>
        private const long MaxBytesPerFile = 1L * 1024 * 1024;   // 1 MB
        private readonly ConcurrentQueue<AccelEvent> _eventQueue = new ConcurrentQueue<AccelEvent>();
        /// <summary>
        /// UI 쪽 SampleEventDetector에서 검출한 이벤트 메타를 넘겨받는다.
        /// (파형은 프레임 파일에 있고, 여기서는 메타만 jsonl로 저장)
        /// </summary>
        public void OnEventDetected(AccelEvent ev)
        {
            if (ev == null) return;
            _eventQueue.Enqueue(ev);
        }
        // -----------------------
        // 생성자
        // -----------------------

        public MinioSenderAcquisitionComponent(string name, IAccelFrameSource source)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));
            if (source == null) throw new ArgumentNullException(nameof(source));

            _name = name;
            _frameSource = source;
            _queue = new BlockingCollection<AccelFrame>(boundedCapacity: 2000);
            _uploadQueue = new BlockingCollection<UploadJob>(boundedCapacity: 100);

            _onFrameGenerated = OnFrameGenerated;
            _frameSource.FrameGenerated += _onFrameGenerated;

            _spoolDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "minio_spool");
            Directory.CreateDirectory(_spoolDir);
        }

        public MinioSenderAcquisitionComponent(IAccelFrameSource source)
            : this("MinioSender", source)
        {
        }

        // -----------------------
        // 프레임 수신
        // -----------------------

        private void OnFrameGenerated(AccelFrame frame)
        {
            if (frame == null || _queue.IsAddingCompleted)
                return;

            try
            {
                _queue.Add(frame);
            }
            catch (InvalidOperationException)
            {
                // CompleteAdding 이후
            }
        }

        private void EnqueueUpload(string bucket, string objectName, string filePath)
        {
            if (_minioClient == null)
                return;
            if (string.IsNullOrEmpty(bucket))
                return;
            if (string.IsNullOrEmpty(objectName) || string.IsNullOrEmpty(filePath))
                return;

            try
            {
                _uploadQueue.Add(new UploadJob
                {
                    Bucket = bucket,
                    ObjectName = objectName,
                    FilePath = filePath
                });
            }
            catch (InvalidOperationException)
            {
                // CompleteAdding() 이후면 그냥 무시
            }
        }

        // -----------------------
        // IAcquisitionComponent 구현
        // -----------------------

        public Task<AcquisitionHandle> StartAcquisitionAsync(DaqAccelConfig cfg)
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

            // 호출 측에서 CancellationToken 이미 취소된 경우
            if (cancellationToken.IsCancellationRequested)
            {
                var tcs = new TaskCompletionSource<AcquisitionHandle>();
                tcs.SetCanceled();
                return tcs.Task;
            }

            // MinIO 설정이 없거나 Raw 저장 비활성화면 바로 Completed 핸들 반환
            if (cfg.Minio == null || !cfg.Minio.EnableRawSave)
            {
                AppEvents.RaiseLog("[MinIO] Raw 저장 비활성화 → Sender 동작하지 않음.");

                Func<CancellationToken, Task> noopStop = t => Task.CompletedTask;
                var handleNoop = new AcquisitionHandle(cfg, noopStop)
                {
                    State = AcquisitionState.Completed
                };
                return Task.FromResult(handleNoop);
            }

            lock (_syncRoot)
            {
                if (_running)
                    throw new InvalidOperationException("MinIO Sender가 이미 실행 중입니다.");

                // UI 쪽 Stop 과 외부 cancellationToken 을 모두 묶는다
                _sessionCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                _running = true;
            }

            // MinIO / 수집 설정
            string endpoint = cfg.Minio.Endpoint ?? "";
            string accessKey = cfg.Minio.AccessKey ?? "";
            string secretKey = cfg.Minio.SecretKey ?? "";
            string bucket = cfg.Minio.Bucket ?? "";
            string saveType = cfg.Minio.SaveType ?? "jsonl";

            string acquisitionUnit = cfg.Daq?.AcquisitionUnit ?? "frame";
            bool isEventMode = string.Equals(acquisitionUnit, "event", StringComparison.OrdinalIgnoreCase);

            double saveIntervalMinutes = cfg.Minio.SaveIntervalMinutes > 0
                ? cfg.Minio.SaveIntervalMinutes
                : 1.0;

            TimeSpan saveInterval = TimeSpan.FromMinutes(saveIntervalMinutes);

            _sessionStartLocal = DateTime.Now;
            string machineId = cfg.Kafka?.MachineId ?? "Unknown";
            string taskType = cfg.Meta?.TaskType ?? "task";
            string labelType = cfg.Meta?.LabelType ?? "label";

            _sessionObjectPrefix = string.Format(
                "{0}/{1:yyyy/MM/dd}/{1:yyyyMMdd_HHmmss}_{0}_{2}_{3}",
                machineId,
                _sessionStartLocal,
                taskType,
                labelType
            );

            _sessionRawPath = null;
            _hasSuccessfulUpload = false;

            _minioClient = BuildMinioClient(endpoint, accessKey, secretKey);

            CancellationToken uploadToken;
            lock (_syncRoot)
            {
                uploadToken = _sessionCts.Token;
            }
            _uploadTask = Task.Run(() => RunUploadLoopAsync(uploadToken));

            // -----------------------
            // Stop 함수 (UI가 호출)
            // -----------------------
            Func<CancellationToken, Task> stopFunc = async ct =>
            {
                CancellationTokenSource cts;
                Task worker;
                Task uploader;

                lock (_syncRoot)
                {
                    cts = _sessionCts;
                    worker = _workerTask;
                    uploader = _uploadTask;
                }

                try
                {
                    // 이 컴포넌트의 Stop은 "남은 데이터 flush"를 위해 Cancel()은 하지 않고
                    // 큐만 닫는 방식 유지.
                    _queue.CompleteAdding();
                    _uploadQueue.CompleteAdding();

                    if (worker != null)
                        await worker.ConfigureAwait(false);

                    if (uploader != null)
                        await uploader.ConfigureAwait(false);
                }
                catch
                {
                    // Stop 예외 무시
                }
            };

            var handle = new AcquisitionHandle(cfg, stopFunc)
            {
                State = AcquisitionState.Running
            };

            // -----------------------
            // 워커 태스크
            // -----------------------
            _workerTask = Task.Run(async () =>
            {
                long totalSamplesSent = 0;

                CancellationToken token;
                lock (_syncRoot)
                {
                    token = _sessionCts.Token;
                }

                AppEvents.RaiseLog(
                    string.Format("[MinIO] {0} START (endpoint={1}, bucket={2}, type={3}, interval={4} min, unit={5} - MinIO는 프레임 기반 저장)",
                        _name,
                        string.IsNullOrEmpty(endpoint) ? "<none>" : endpoint,
                        string.IsNullOrEmpty(bucket) ? "<none>" : bucket,
                        saveType,
                        saveIntervalMinutes,
                        acquisitionUnit));

                try
                {
                    if (!isEventMode)
                    {
                        // -------------------------------
                        // 프레임 모드: 배치 저장 (시간 + 크기 기준 flush)
                        // -------------------------------
                        long fileCount = 0;
                        long totalFrames = 0;
                        long totalSamples = 0;

                        var batch = new List<AccelFrame>();
                        DateTimeOffset nextFlushAt = DateTimeOffset.Now + saveInterval;
                        long approxBytesInBatch = 0;

                        while (true)
                        {
                            bool hasFrame = false;
                            AccelFrame frame = null;

                            // 큐가 아직 열려 있으면 계속 프레임 수신
                            if (!_queue.IsCompleted)
                            {
                                hasFrame = _queue.TryTake(out frame, millisecondsTimeout: 500);
                            }

                            if (hasFrame && frame != null)
                            {
                                batch.Add(frame);

                                if (frame.Ax != null)
                                {
                                    int n = frame.Ax.Length;
                                    // 3축 double 기준 대략적인 크기
                                    approxBytesInBatch += (long)n * 3L * sizeof(double);
                                }
                            }

                            var now = DateTimeOffset.Now;
                            bool timeToFlush = now >= nextFlushAt;
                            bool sizeToFlush = approxBytesInBatch >= MaxBytesPerFile;

                            if (batch.Count > 0 && (timeToFlush || sizeToFlush))
                            {
                                fileCount++;

                                long framesInBatch = batch.Count;
                                long samplesInBatch = 0;
                                int samplesPerFrame = 0;

                                for (int i = 0; i < batch.Count; i++)
                                {
                                    var f = batch[i];
                                    if (f.Ax != null)
                                    {
                                        samplesInBatch += f.Ax.Length;
                                        samplesPerFrame = f.Ax.Length;
                                    }
                                }

                                totalFrames += framesInBatch;
                                totalSamples += samplesInBatch;

                                string filePath = null;
                                string objectName = null;

                                try
                                {
                                    string relativePath = BuildSessionRelativePath(saveType, fileCount);
                                    filePath = BuildSpoolFilePath(relativePath);
                                    objectName = relativePath;

                                    SaveBatchToFile(saveType, filePath, batch, framesInBatch, samplesPerFrame);

                                    if (_minioClient != null && !string.IsNullOrEmpty(bucket))
                                    {
                                        EnqueueUpload(bucket, objectName, filePath);
                                    }

                                    AppEvents.RaiseLog(
                                        string.Format("[MinIO] {0} raw saved (file={1}, frames={2}, samples={3}, approx={4:F2} MB)",
                                            _name,
                                            fileCount,
                                            framesInBatch,
                                            samplesInBatch,
                                            approxBytesInBatch / 1024.0 / 1024.0));
                                }
                                catch (Exception ex)
                                {
                                    AppEvents.RaiseLog(
                                        string.Format("[MinIO] {0} 파일 저장/업로드 실패: {1}", _name, ex.Message));
                                }

                                if (progress != null)
                                {
                                    var report = new AcquisitionProgress
                                    {
                                        TotalSamplesSent = totalSamples,
                                        LastBatchAt = now,
                                        Message = string.Format(
                                            "{0} saving raw... Files={1}, TotalFrames={2}, TotalSamples={3}",
                                            _name,
                                            fileCount,
                                            totalFrames,
                                            totalSamples)
                                    };

                                    progress.Report(report);
                                }

                                batch.Clear();
                                approxBytesInBatch = 0;
                                nextFlushAt = now + saveInterval;
                            }

                            // Stop 에서 _queue.CompleteAdding()이 호출되면,
                            // 큐가 비고(IsCompleted) 더 이상 프레임이 없을 때 루프 종료
                            if (_queue.IsCompleted && !hasFrame)
                                break;

                            // 외부 cancellationToken 이 강제 취소를 요청한 경우
                            // (이 때는 남은 데이터 손실을 감수하고 빠르게 탈출)
                            if (token.IsCancellationRequested && _queue.IsCompleted)
                                break;
                        }

                        // 루프 종료 후, 남은 batch가 있다면 마지막으로 한 번 더 flush
                        if (batch.Count > 0)
                        {
                            fileCount++;

                            long framesInBatch = batch.Count;
                            long samplesInBatch = 0;
                            int samplesPerFrame = 0;

                            for (int i = 0; i < batch.Count; i++)
                            {
                                var f = batch[i];
                                if (f.Ax != null)
                                {
                                    samplesInBatch += f.Ax.Length;
                                    samplesPerFrame = f.Ax.Length;
                                }
                            }

                            totalFrames += framesInBatch;
                            totalSamples += samplesInBatch;

                            string filePath = null;
                            string objectName = null;

                            try
                            {
                                string relativePath = BuildSessionRelativePath(saveType, fileCount);
                                filePath = BuildSpoolFilePath(relativePath);
                                objectName = relativePath;

                                SaveBatchToFile(saveType, filePath, batch, framesInBatch, samplesPerFrame);

                                if (_minioClient != null && !string.IsNullOrEmpty(bucket))
                                {
                                    try
                                    {
                                        await UploadFileToMinioAsync(_minioClient, bucket, objectName, filePath)
                                            .ConfigureAwait(false);

                                        if (!_hasSuccessfulUpload)
                                        {
                                            _sessionRawPath = string.Format("s3://{0}/{1}", bucket, objectName);
                                            _hasSuccessfulUpload = true;
                                            AppEvents.RaiseLog("[MinIO] SessionRawPath 설정(마지막 배치): " + _sessionRawPath);
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        AppEvents.RaiseLog(
                                            string.Format("[MinIO] 업로드 실패 (bucket={0}, object={1}): {2}",
                                                bucket,
                                                objectName,
                                                ex.Message));
                                    }
                                }

                                AppEvents.RaiseLog(
                                    string.Format("[MinIO] {0} raw 마지막 배치 저장 (file={1}, frames={2}, samples={3})",
                                        _name,
                                        fileCount,
                                        framesInBatch,
                                        samplesInBatch));
                            }
                            catch (Exception ex)
                            {
                                AppEvents.RaiseLog(
                                    string.Format("[MinIO] {0} 마지막 배치 저장/업로드 실패: {1}", _name, ex.Message));
                            }
                        }

                        totalSamplesSent = totalSamples;
                    }
                    else
                    {
                        // -------------------------------
                        // 이벤트 모드: RunEventModeLoopAsync 사용
                        // (jsonl일 때는 내부에서 여러 이벤트를 모아서 파일 크기 기준 롤링)
                        // -------------------------------
                        totalSamplesSent = await RunEventModeLoopAsync(
                            token,
                            bucket,
                            saveType,
                            progress
                        ).ConfigureAwait(false);
                    }

                    // 기존 로직 유지
                    handle.State = AcquisitionState.Canceled;
                }
                catch (OperationCanceledException)
                {
                    handle.State = AcquisitionState.Canceled;
                }
                catch (Exception ex)
                {
                    handle.State = AcquisitionState.Faulted;
                    handle.LastError = ex;

                    AppEvents.RaiseLog(
                        string.Format("[MinIO] {0} ERROR: {1}", _name, ex.Message));
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
                        handle.State = AcquisitionState.Completed;

                    AppEvents.RaiseLog(
                        string.Format("[MinIO] {0} STOP (State={1}, TotalSamples={2})",
                            _name,
                            handle.State,
                            totalSamplesSent));
                }
            });

            return Task.FromResult(handle);
        }

        // -----------------------
        // MinIO 유틸
        // -----------------------

        // 이벤트 모드에서 사용할 내부 버퍼용 타입
        private sealed class FrameWithRange
        {
            public AccelFrame Frame { get; set; }
            public long StartSample { get; set; }
            public long EndSample { get; set; }
        }

        /// <summary>
        /// 이벤트 모드:
        /// - UI에서 OnEventDetected(AccelEvent)로 넘어온 이벤트 정보(샘플 인덱스)를 기준으로
        ///   AccelFrame 스트림에서 해당 구간 raw를 정확하게 잘라낸 뒤
        ///   프레임 모드와 동일한 확장자(jsonl/bin/npy)로 저장.
        /// - 메타데이터는 저장하지 않는다.
        /// - jsonl일 때는 여러 이벤트를 한 파일에 모아서 MaxBytesPerFile 기준으로 파일 롤링.
        ///   (bin/npy는 포맷 특성상 이벤트 1개당 파일 1개 유지)
        /// </summary>
        private async Task<long> RunEventModeLoopAsync(
            CancellationToken token,
            string bucket,
            string saveType,
            IProgress<AcquisitionProgress> progress)
        {
            long nextSampleIndex = 0;      // 프레임 스트림 기준 글로벌 sample index
            long fileIndex = 0;
            long totalSamplesSaved = 0;

            var frames = new List<FrameWithRange>();

            // saveType은 프레임 모드와 동일하게 사용
            string st = saveType != null ? saveType.ToLowerInvariant() : "jsonl";

            // -------------------------------
            // jsonl일 때: 이벤트 여러 개를 한 파일에 배치 저장
            // -------------------------------
            bool isJsonl = string.Equals(st, "jsonl", StringComparison.OrdinalIgnoreCase);
            var currentEventBatch = new List<AccelFrame>();
            long approxBytesInCurrentEventFile = 0;
            int eventsInCurrentFile = 0;

            // 종료 조건:
            // - 취소 + 프레임 큐 비었고 + 이벤트 큐도 비었을 때
            while (!token.IsCancellationRequested || !_queue.IsCompleted || !_eventQueue.IsEmpty)
            {
                // 1) 프레임 수신
                AccelFrame frame;
                if (!_queue.IsCompleted && _queue.TryTake(out frame, millisecondsTimeout: 200))
                {
                    if (frame != null && frame.Ax != null && frame.Ax.Length > 0)
                    {
                        int n = frame.Ax.Length;
                        long start = nextSampleIndex;
                        long end = start + n - 1;

                        frames.Add(new FrameWithRange
                        {
                            Frame = frame,
                            StartSample = start,
                            EndSample = end
                        });

                        nextSampleIndex = end + 1;
                    }
                }

                // 2) 이벤트 처리
                while (_eventQueue.TryPeek(out var ev))
                {
                    if (frames.Count == 0)
                        break;

                    long lastEnd = frames[frames.Count - 1].EndSample;

                    // 아직 이 이벤트의 끝까지 들어오지 않았으면 다음 프레임을 기다린다.
                    if (lastEnd < ev.EndSampleIndex)
                        break;

                    // 여기까지 왔으면 이 이벤트를 구성하는 raw가 모두 들어온 상태
                    _eventQueue.TryDequeue(out ev);

                    long evStart = ev.StartSampleIndex;
                    long evEnd = ev.EndSampleIndex;

                    // 이벤트에 해당하는 전체 샘플 수 계산
                    long sampleCount = 0;
                    foreach (var fr in frames)
                    {
                        if (fr.EndSample < evStart || fr.StartSample > evEnd)
                            continue;

                        long s = Math.Max(fr.StartSample, evStart);
                        long e = Math.Min(fr.EndSample, evEnd);
                        sampleCount += (e - s + 1);
                    }

                    if (sampleCount <= 0)
                        continue;

                    int len = checked((int)sampleCount);

                    var ax = new double[len];
                    var ay = new double[len];
                    var az = new double[len];

                    int dst = 0;
                    foreach (var fr in frames)
                    {
                        if (fr.EndSample < evStart || fr.StartSample > evEnd)
                            continue;

                        long sGlobal = Math.Max(fr.StartSample, evStart);
                        long eGlobal = Math.Min(fr.EndSample, evEnd);

                        int localStart = (int)(sGlobal - fr.StartSample);
                        int localEnd = (int)(eGlobal - fr.StartSample);
                        int count = localEnd - localStart + 1;

                        Array.Copy(fr.Frame.Ax, localStart, ax, dst, count);
                        if (fr.Frame.Ay != null) Array.Copy(fr.Frame.Ay, localStart, ay, dst, count);
                        if (fr.Frame.Az != null) Array.Copy(fr.Frame.Az, localStart, az, dst, count);

                        dst += count;
                    }

                    // 이벤트 전체를 하나의 프레임으로 묶음
                    var eventFrame = new AccelFrame
                    {
                        Seq = ev.EventIndex,
                        // 시간 정보는 크게 중요하지 않으므로 첫 프레임 기준 혹은 현재 시간 사용
                        Timestamp = DateTimeOffset.UtcNow,
                        Ax = ax,
                        Ay = ay,
                        Az = az
                    };

                    // -------------------------------
                    // jsonl: 여러 이벤트를 한 파일에 모아서 크기 기준 롤링
                    // -------------------------------
                    if (isJsonl)
                    {
                        currentEventBatch.Add(eventFrame);
                        eventsInCurrentFile++;

                        // 대략적인 파일 크기 추정: double 3채널 기준
                        approxBytesInCurrentEventFile += (long)len * 3L * sizeof(double);

                        totalSamplesSaved += len;

                        // 진행 상황은 "이벤트 수준" 기준으로 계속 올려주자 (파일 flush와는 별개)
                        if (progress != null)
                        {
                            var nowEvt = DateTimeOffset.Now;
                            var reportEvt = new AcquisitionProgress
                            {
                                TotalSamplesSent = totalSamplesSaved,
                                LastBatchAt = nowEvt,
                                Message = $"Event {ev.EventIndex} queued (samples={len}, currentFileEvents={eventsInCurrentFile})"
                            };
                            progress.Report(reportEvt);
                        }

                        bool shouldFlush = approxBytesInCurrentEventFile >= MaxBytesPerFile;

                        if (shouldFlush)
                        {
                            fileIndex++;
                            string relativePath = BuildSessionRelativePath(st, fileIndex);
                            string filePath = BuildSpoolFilePath(relativePath);
                            string objectName = relativePath;

                            try
                            {
                                // jsonl은 framesInBatch / samplesPerFrame를 사용하지 않으므로 dummy 값 전달
                                SaveBatchToFile(st, filePath, currentEventBatch,
                                    framesInBatch: currentEventBatch.Count,
                                    samplesPerFrame: len);

                                if (_minioClient != null && !string.IsNullOrEmpty(bucket))
                                {
                                    await UploadFileToMinioAsync(_minioClient, bucket, objectName, filePath)
                                        .ConfigureAwait(false);

                                    if (!_hasSuccessfulUpload)
                                    {
                                        _sessionRawPath = string.Format("s3://{0}/{1}", bucket, objectName);
                                        _hasSuccessfulUpload = true;
                                        AppEvents.RaiseLog("[MinIO] SessionRawPath 설정(이벤트 jsonl): " + _sessionRawPath);
                                    }
                                }

                                AppEvents.RaiseLog(
                                    $"[MinIO] 이벤트 raw 배치 저장 (fileIndex={fileIndex}, events={eventsInCurrentFile}, approx={approxBytesInCurrentEventFile / 1024.0 / 1024.0:F2} MB)");

                                if (progress != null)
                                {
                                    var now = DateTimeOffset.Now;
                                    var report = new AcquisitionProgress
                                    {
                                        TotalSamplesSent = totalSamplesSaved,
                                        LastBatchAt = now,
                                        Message = $"Event batch flushed (fileIndex={fileIndex}, events={eventsInCurrentFile})"
                                    };
                                    progress.Report(report);
                                }
                            }
                            catch (Exception ex)
                            {
                                AppEvents.RaiseLog(
                                    $"[MinIO] 이벤트 raw 배치 저장/업로드 실패 (fileIndex={fileIndex}, events={eventsInCurrentFile}): {ex.Message}");
                            }
                            finally
                            {
                                // 다음 파일 준비
                                currentEventBatch.Clear();
                                approxBytesInCurrentEventFile = 0;
                                eventsInCurrentFile = 0;
                            }
                        }
                    }
                    else
                    {
                        // -------------------------------
                        // bin / npy 등: 기존처럼 이벤트 1개 → 파일 1개
                        // (SaveAsBinary/SaveAsNpy가 고정 samplesPerFrame 가정이어서 다수 이벤트 묶기 어려움)
                        // -------------------------------
                        fileIndex++;
                        string relativePath = BuildSessionRelativePath(st, fileIndex);
                        string filePath = BuildSpoolFilePath(relativePath);
                        string objectName = relativePath;

                        try
                        {
                            SaveBatchToFile(st, filePath,
                                new List<AccelFrame> { eventFrame },
                                framesInBatch: 1,
                                samplesPerFrame: len);

                            if (_minioClient != null && !string.IsNullOrEmpty(bucket))
                            {
                                await UploadFileToMinioAsync(_minioClient, bucket, objectName, filePath)
                                    .ConfigureAwait(false);

                                if (!_hasSuccessfulUpload)
                                {
                                    _sessionRawPath = string.Format("s3://{0}/{1}", bucket, objectName);
                                    _hasSuccessfulUpload = true;
                                    AppEvents.RaiseLog("[MinIO] SessionRawPath 설정(이벤트): " + _sessionRawPath);
                                }
                            }

                            totalSamplesSaved += len;

                            AppEvents.RaiseLog(
                                $"[MinIO] 이벤트 raw 저장 (eventIndex={ev.EventIndex}, samples={len}, file={fileIndex})");

                            if (progress != null)
                            {
                                var now = DateTimeOffset.Now;
                                var report = new AcquisitionProgress
                                {
                                    TotalSamplesSent = totalSamplesSaved,
                                    LastBatchAt = now,
                                    Message = $"Event {ev.EventIndex} saved (samples={len}, fileIndex={fileIndex})"
                                };
                                progress.Report(report);
                            }
                        }
                        catch (Exception ex)
                        {
                            AppEvents.RaiseLog(
                                $"[MinIO] 이벤트 raw 저장/업로드 실패 (eventIndex={ev.EventIndex}, fileIndex={fileIndex}): {ex.Message}");
                        }
                    }

                    // 이 이벤트 이전의 프레임들은 버퍼에서 제거 (메모리 정리)
                    long minNeeded = evEnd + 1;
                    int removeCount = 0;
                    foreach (var fr in frames)
                    {
                        if (fr.EndSample < minNeeded)
                            removeCount++;
                        else
                            break;
                    }
                    if (removeCount > 0)
                        frames.RemoveRange(0, removeCount);
                }

                // 프레임도 다 끝나고 이벤트도 더 이상 없으면 종료
                if (_queue.IsCompleted && _eventQueue.IsEmpty)
                    break;
            }

            // 루프 종료 후, jsonl 모드에서 아직 flush 안 된 배치가 있으면 마지막 파일로 저장
            if (isJsonl && currentEventBatch.Count > 0)
            {
                fileIndex++;
                string relativePath = BuildSessionRelativePath(st, fileIndex);
                string filePath = BuildSpoolFilePath(relativePath);
                string objectName = relativePath;

                try
                {
                    int lastSamplesPerFrame = currentEventBatch[0].Ax != null
                        ? currentEventBatch[0].Ax.Length
                        : 0;

                    SaveBatchToFile(st, filePath,
                        currentEventBatch,
                        framesInBatch: currentEventBatch.Count,
                        samplesPerFrame: lastSamplesPerFrame);

                    if (_minioClient != null && !string.IsNullOrEmpty(bucket))
                    {
                        await UploadFileToMinioAsync(_minioClient, bucket, objectName, filePath)
                            .ConfigureAwait(false);

                        if (!_hasSuccessfulUpload)
                        {
                            _sessionRawPath = string.Format("s3://{0}/{1}", bucket, objectName);
                            _hasSuccessfulUpload = true;
                            AppEvents.RaiseLog("[MinIO] SessionRawPath 설정(이벤트 jsonl 마지막): " + _sessionRawPath);
                        }
                    }

                    AppEvents.RaiseLog(
                        $"[MinIO] 이벤트 raw 마지막 배치 저장 (fileIndex={fileIndex}, events={currentEventBatch.Count}, approx={approxBytesInCurrentEventFile / 1024.0 / 1024.0:F2} MB)");

                    if (progress != null)
                    {
                        var now = DateTimeOffset.Now;
                        var report = new AcquisitionProgress
                        {
                            TotalSamplesSent = totalSamplesSaved,
                            LastBatchAt = now,
                            Message = $"Final event batch flushed (fileIndex={fileIndex}, events={currentEventBatch.Count})"
                        };
                        progress.Report(report);
                    }
                }
                catch (Exception ex)
                {
                    AppEvents.RaiseLog(
                        $"[MinIO] 이벤트 raw 마지막 배치 저장/업로드 실패 (fileIndex={fileIndex}, events={currentEventBatch.Count}): {ex.Message}");
                }
            }

            return totalSamplesSaved;
        }

        /// <summary>
        /// MinIO 업로드 전용 루프:
        /// - 수집 스레드는 파일만 디스크에 쓰고, 여기서는 UploadJob 큐만 소비해서 MinIO로 전송.
        /// - 첫 성공 시 _sessionRawPath / _hasSuccessfulUpload 설정.
        /// </summary>
        private async Task RunUploadLoopAsync(CancellationToken token)
        {
            while (!_uploadQueue.IsCompleted && !token.IsCancellationRequested)
            {
                UploadJob job = null;

                try
                {
                    if (!_uploadQueue.TryTake(out job, millisecondsTimeout: 500, cancellationToken: token))
                        continue;
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                if (job == null)
                    continue;

                try
                {
                    await UploadFileToMinioAsync(
                            _minioClient,
                            job.Bucket,
                            job.ObjectName,
                            job.FilePath)
                        .ConfigureAwait(false);

                    if (!_hasSuccessfulUpload)
                    {
                        _sessionRawPath = $"s3://{job.Bucket}/{job.ObjectName}";
                        _hasSuccessfulUpload = true;
                        AppEvents.RaiseLog("[MinIO] SessionRawPath 설정(업로드 큐): " + _sessionRawPath);
                    }
                }
                catch (Exception ex)
                {
                    AppEvents.RaiseLog(
                        $"[MinIO] 업로드 실패 (bucket={job.Bucket}, object={job.ObjectName}): {ex.Message}");
                    // 여기서는 throw 안 하고 그냥 다음 job 처리
                }
            }

            AppEvents.RaiseLog("[MinIO] 업로드 루프 종료");
        }

        private IMinioClient BuildMinioClient(string endpoint, string accessKey, string secretKey)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(endpoint) ||
                    string.IsNullOrWhiteSpace(accessKey) ||
                    string.IsNullOrWhiteSpace(secretKey))
                {
                    AppEvents.RaiseLog("[MinIO] Endpoint/AccessKey/SecretKey 중 일부가 비어 있어 업로드를 비활성화합니다.");
                    return null;
                }

                string ep = endpoint.Trim();
                bool useSsl = false;

                if (ep.StartsWith("http://", StringComparison.OrdinalIgnoreCase))
                {
                    ep = ep.Substring("http://".Length);
                }
                else if (ep.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
                {
                    ep = ep.Substring("https://".Length);
                    useSsl = true;
                }

                var client = new MinioClient()
                    .WithEndpoint(ep)
                    .WithCredentials(accessKey, secretKey);

                if (useSsl)
                    client = client.WithSSL();

                var built = client.Build();

                AppEvents.RaiseLog("[MinIO] 클라이언트 초기화 완료 (endpoint=" + ep + ", ssl=" + useSsl + ")");
                return built;
            }
            catch (Exception ex)
            {
                AppEvents.RaiseLog("[MinIO] 클라이언트 생성 실패: " + ex.Message);
                return null;
            }
        }

        private async Task UploadFileToMinioAsync(
            IMinioClient client,
            string bucket,
            string objectName,
            string filePath)
        {
            if (client == null)
            {
                AppEvents.RaiseLog("[MinIO] Upload 요청 받았지만 client = null");
                return;
            }

            AppEvents.RaiseLog(
                string.Format("[MinIO] 업로드 시도 (bucket={0}, object={1}, file={2})",
                    bucket, objectName, filePath));

            try
            {
                var putArgs = new PutObjectArgs()
                    .WithBucket(bucket)
                    .WithObject(objectName)
                    .WithFileName(filePath)
                    .WithContentType("application/octet-stream");

                await client.PutObjectAsync(putArgs)
                    .ConfigureAwait(false);

                AppEvents.RaiseLog(
                    string.Format("[MinIO] 업로드 완료 (bucket={0}, object={1})", bucket, objectName));
            }
            catch (ObjectDisposedException ode)
            {
                // ReadOnlyMemoryContent 정리 타이밍 예외는 성공으로 간주
                if (string.Equals(
                        ode.ObjectName,
                        "System.Net.Http.ReadOnlyMemoryContent",
                        StringComparison.Ordinal))
                {
                    return;
                }

                AppEvents.RaiseLog(
                    string.Format("[MinIO] 업로드 실패 (bucket={0}, object={1}): {2}",
                        bucket,
                        objectName,
                        ode.Message));
            }
            catch (Exception ex)
            {
                AppEvents.RaiseLog(
                    string.Format("[MinIO] 업로드 실패 (bucket={0}, object={1}): {2}\n{3}",
                        bucket,
                        objectName,
                        ex.Message,
                        ex.StackTrace));
                throw;
            }
        }

        // -----------------------
        // 파일 저장 유틸
        // -----------------------

        private string BuildSessionRelativePath(string saveType, long fileIndex)
        {
            string ext = string.IsNullOrEmpty(saveType) ? "bin" : saveType.ToLowerInvariant();
            string prefix = string.IsNullOrEmpty(_sessionObjectPrefix) ? "session" : _sessionObjectPrefix;

            string relative = string.Format("{0}_{1:D6}.{2}", prefix, fileIndex, ext);
            return relative.Replace('\\', '/');
        }

        private string BuildSpoolFilePath(string relativePath)
        {
            var localRelative = relativePath.Replace('/', Path.DirectorySeparatorChar);
            string fullPath = Path.Combine(_spoolDir, localRelative);

            string dir = Path.GetDirectoryName(fullPath);
            if (!string.IsNullOrEmpty(dir))
                Directory.CreateDirectory(dir);

            return fullPath;
        }

        private void SaveBatchToFile(
            string saveType,
            string path,
            IList<AccelFrame> batch,
            long framesInBatch,
            int samplesPerFrame)
        {
            if (batch == null || batch.Count == 0)
                return;

            string st = saveType != null ? saveType.ToLowerInvariant() : "jsonl";

            if (st == "jsonl")
            {
                SaveAsJsonl(path, batch);
            }
            else if (st == "bin")
            {
                SaveAsBinary(path, batch, framesInBatch, samplesPerFrame);
            }
            else if (st == "npy")
            {
                SaveAsNpy(path, batch, framesInBatch, samplesPerFrame);
            }
            else
            {
                SaveAsJsonl(path, batch);
            }
        }

        private void SaveAsJsonl(string path, IList<AccelFrame> batch)
        {
            using (var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read))
            using (var sw = new StreamWriter(fs, new UTF8Encoding(false)))
            {
                for (int i = 0; i < batch.Count; i++)
                {
                    var f = batch[i];
                    var obj = new
                    {
                        seq = f.Seq,
                        t0 = f.Timestamp.UtcDateTime,
                        ax = f.Ax,
                        ay = f.Ay,
                        az = f.Az
                    };
                    string line = JsonConvert.SerializeObject(obj);
                    sw.WriteLine(line);
                }
            }
        }

        /// <summary>
        /// 간단한 바이너리 포맷:
        ///   magic(4) = 'A','C','E','L'
        ///   int32 frames
        ///   int32 samplesPerFrame
        /// 각 프레임마다:
        ///   Int64 seq
        ///   Int64 ticks (Timestamp.UtcDateTime.Ticks)
        ///   double[samplesPerFrame] ax, ay, az
        /// </summary>
        private void SaveAsBinary(
            string path,
            IList<AccelFrame> batch,
            long framesInBatch,
            int samplesPerFrame)
        {
            using (var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read))
            using (var bw = new BinaryWriter(fs, new UTF8Encoding(false)))
            {
                bw.Write((byte)'A');
                bw.Write((byte)'C');
                bw.Write((byte)'E');
                bw.Write((byte)'L');
                bw.Write((int)framesInBatch);
                bw.Write((int)samplesPerFrame);

                for (int i = 0; i < batch.Count; i++)
                {
                    var f = batch[i];
                    bw.Write((long)f.Seq);
                    bw.Write(f.Timestamp.UtcDateTime.Ticks);

                    double[] ax = f.Ax ?? new double[samplesPerFrame];
                    double[] ay = f.Ay ?? new double[samplesPerFrame];
                    double[] az = f.Az ?? new double[samplesPerFrame];

                    for (int s = 0; s < samplesPerFrame; s++)
                        bw.Write(ax[s]);
                    for (int s = 0; s < samplesPerFrame; s++)
                        bw.Write(ay[s]);
                    for (int s = 0; s < samplesPerFrame; s++)
                        bw.Write(az[s]);
                }
            }
        }

        /// <summary>
        /// NumPy .npy (v1.0) 형식으로 저장.
        /// dtype=float64, fortran_order=False, shape=(frames, samples, 3)
        /// </summary>
        private void SaveAsNpy(
            string path,
            IList<AccelFrame> batch,
            long framesInBatch,
            int samplesPerFrame)
        {
            int frames = (int)framesInBatch;
            int channels = 3;
            int totalCount = frames * samplesPerFrame * channels;

            double[] data = new double[totalCount];
            int idx = 0;

            for (int fi = 0; fi < frames; fi++)
            {
                var f = batch[fi];
                double[] ax = f.Ax ?? new double[samplesPerFrame];
                double[] ay = f.Ay ?? new double[samplesPerFrame];
                double[] az = f.Az ?? new double[samplesPerFrame];

                for (int s = 0; s < samplesPerFrame; s++)
                {
                    data[idx++] = ax[s];
                    data[idx++] = ay[s];
                    data[idx++] = az[s];
                }
            }

            string headerDict = string.Format(
                "{{'descr': '<f8', 'fortran_order': False, 'shape': ({0}, {1}, {2}), }}",
                frames,
                samplesPerFrame,
                channels);

            byte[] magic = new byte[] { 0x93, (byte)'N', (byte)'U', (byte)'M', (byte)'P', (byte)'Y' };
            byte major = 1;
            byte minor = 0;

            string headerPadded = headerDict;
            int headerLen;
            {
                string tmp = headerPadded + new string(' ', 1) + "\n";
                headerLen = Encoding.ASCII.GetByteCount(tmp);
                int pad = 16 - (10 + headerLen) % 16;
                if (pad == 16) pad = 0;

                headerPadded = headerDict + new string(' ', pad) + "\n";
                headerLen = Encoding.ASCII.GetByteCount(headerPadded);
            }

            byte[] headerBytes = Encoding.ASCII.GetBytes(headerPadded);

            using (var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read))
            using (var bw = new BinaryWriter(fs))
            {
                bw.Write(magic);
                bw.Write(major);
                bw.Write(minor);
                bw.Write((ushort)headerLen);
                bw.Write(headerBytes);

                for (int i = 0; i < data.Length; i++)
                    bw.Write(data[i]);
            }
        }

        private static double ComputeRms(double[] a)
        {
            if (a == null || a.Length == 0) return 0.0;
            double s = 0.0;
            for (int i = 0; i < a.Length; i++)
                s += a[i] * a[i];
            return Math.Sqrt(s / a.Length);
        }

        // -----------------------
        // IDisposable / 정리
        // -----------------------

        private void InternalStop()
        {
            CancellationTokenSource cts;
            Task worker;
            Task uploader;

            lock (_syncRoot)
            {
                cts = _sessionCts;
                worker = _workerTask;
                uploader = _uploadTask;
            }

            try
            {
                if (cts != null && !cts.IsCancellationRequested)
                    cts.Cancel();

                _queue.CompleteAdding();
                _uploadQueue.CompleteAdding();

                if (worker != null)
                    worker.GetAwaiter().GetResult();

                if (uploader != null)
                    uploader.GetAwaiter().GetResult();
            }
            catch
            {
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
                    _uploadTask = null;
                }

                if (_minioClient != null)
                {
                    try { _minioClient.Dispose(); } catch { }
                    _minioClient = null;
                }
            }
        }

        public void Dispose()
        {
            InternalStop();

            if (_frameSource != null && _onFrameGenerated != null)
            {
                try
                {
                    _frameSource.FrameGenerated -= _onFrameGenerated;
                }
                catch
                {
                    // 무시
                }
            }
        }
    }
}
