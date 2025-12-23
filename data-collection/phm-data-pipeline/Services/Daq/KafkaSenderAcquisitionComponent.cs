using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Newtonsoft.Json;
using phm_data_pipeline.Models;

namespace phm_data_pipeline.Services.Daq
{
    /// <summary>
    /// IAccelFrameSource로부터 프레임을 받아 Kafka로 전송하는 컴포넌트.
    /// </summary>
    public sealed class KafkaSenderAcquisitionComponent : IAcquisitionComponent, IDisposable
    {
        private readonly string _name;
        private readonly IAccelFrameSource _source;

        private readonly object _syncRoot = new object();

        private BlockingCollection<AccelFrame> _queue;
        private CancellationTokenSource _sessionCts;
        private Task _workerTask;
        private bool _running;

        private Action<AccelFrame> _onFrameGenerated;

        private long _frameCount;
        private long _totalSamples;

        public KafkaSenderAcquisitionComponent(string name, IAccelFrameSource source)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));
            if (source == null) throw new ArgumentNullException(nameof(source));

            _name = name;
            _source = source;
        }

        // ============================================================
        //   IAcquisitionComponent 구현 (오버로드 3개)
        // ============================================================
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

            if (cfg.Kafka == null)
                throw new ArgumentException("cfg.Kafka 가 null 입니다.", nameof(cfg));

            string bootstrap = cfg.Kafka.Bootstrap ?? "";
            string topic = cfg.Kafka.Topic ?? "";
            string machineId = cfg.Kafka.MachineId ?? "";

            if (string.IsNullOrWhiteSpace(bootstrap) ||
                string.IsNullOrWhiteSpace(topic))
            {
                AppEvents.RaiseLog("[Kafka] Kafka 설정이 비어 있어 Sender를 시작하지 않습니다.");
                // 작동 안 하는 No-op 핸들 반환
                var noOpHandle = new AcquisitionHandle(cfg, async ct => { await Task.CompletedTask; })
                {
                    State = AcquisitionState.Completed
                };
                return Task.FromResult(noOpHandle);
            }

            if (cancellationToken.IsCancellationRequested)
            {
                var tcs = new TaskCompletionSource<AcquisitionHandle>();
                tcs.SetCanceled();
                return tcs.Task;
            }

            lock (_syncRoot)
            {
                if (_running)
                    throw new InvalidOperationException("KafkaSenderAcquisitionComponent 가 이미 실행 중입니다.");

                _sessionCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                _queue = new BlockingCollection<AccelFrame>(boundedCapacity: 2000);
                _running = true;
            }

            _frameCount = 0;
            _totalSamples = 0;

            // FrameSource 구독
            _onFrameGenerated = frame =>
            {
                if (frame == null) return;

                var q = _queue;
                if (q == null || q.IsAddingCompleted) return;

                try
                {
                    // 큐가 가득 차면 오래 블로킹하지 않도록 TryAdd 사용
                    if (!q.TryAdd(frame))
                    {
                        // 가장 오래된 것 하나 버리고 새 것 추가 (드롭-올드 전략)
                        AccelFrame dummy;
                        q.TryTake(out dummy);
                        q.TryAdd(frame);
                    }
                }
                catch (InvalidOperationException)
                {
                    // CompleteAdding 이후: 무시
                }
            };
            _source.FrameGenerated += _onFrameGenerated;

            AppEvents.RaiseLog(string.Format(
                "[Kafka] {0} START (bootstrap={1}, topic={2}, machine={3})",
                _name, bootstrap, topic, machineId));

            // stopFunc 정의 (여기서도 타임아웃 걸어서 절대 안 막히게 함)
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
                    // 이벤트 구독 해제
                    if (_onFrameGenerated != null)
                    {
                        _source.FrameGenerated -= _onFrameGenerated;
                        _onFrameGenerated = null;
                    }

                    if (cts != null && !cts.IsCancellationRequested)
                        cts.Cancel();

                    // 큐 종료 → Worker의 GetConsumingEnumerable 종료
                    var q = _queue;
                    if (q != null && !q.IsAddingCompleted)
                    {
                        try { q.CompleteAdding(); } catch { }
                    }

                    if (worker != null)
                    {
                        // 1초만 기다리고, 안 끝나면 그냥 반환 (UI 안 막히게)
                        var completed = await Task.WhenAny(worker, Task.Delay(1000, ct))
                                                 .ConfigureAwait(false);
                        if (completed != worker)
                        {
                            AppEvents.RaiseLog(
                                string.Format("[Kafka] {0} StopAsync TIMEOUT(1s) - 백그라운드에서 정리 계속", _name));
                        }
                    }
                }
                catch (Exception ex)
                {
                    AppEvents.RaiseLog("[Kafka] StopAsync 중 예외 무시: " + ex.Message);
                }
            };

            var handle = new AcquisitionHandle(cfg, stopFunc);
            handle.State = AcquisitionState.Running;

            // 백그라운드 Kafka 전송 루프
            _workerTask = Task.Run(() =>
            {
                RunWorkerLoop(handle, bootstrap, topic, machineId, cfg, progress);
            });

            return Task.FromResult(handle);
        }

        // ============================================================
        //   Kafka 전송 루프
        // ============================================================
        private void RunWorkerLoop(
            AcquisitionHandle handle,
            string bootstrap,
            string topic,
            string machineId,
            DaqAccelConfig cfg,
            IProgress<AcquisitionProgress> progress)
        {
            CancellationToken token;
            lock (_syncRoot)
            {
                token = _sessionCts != null ? _sessionCts.Token : CancellationToken.None;
            }

            var config = new ProducerConfig
            {
                BootstrapServers = bootstrap,
                // 필요 시 Acks, MessageTimeoutMs 등 추가 가능
            };

            try
            {
                using (var producer = new ProducerBuilder<string, string>(config).Build())
                {
                    foreach (var frame in _queue.GetConsumingEnumerable(token))
                    {
                        if (frame == null) continue;

                        int n = (frame.Ax != null) ? frame.Ax.Length : 0;
                        if (n <= 0) continue;

                        long frameIndex = Interlocked.Increment(ref _frameCount);
                        long total = Interlocked.Add(ref _totalSamples, n);

                        // 간단한 JSON 페이로드 예시
                        var payload = new
                        {
                            machine_id = machineId,
                            seq = frame.Seq,
                            ts_utc = frame.Timestamp.UtcDateTime.ToString("o"),
                            ax = frame.Ax,
                            ay = frame.Ay,
                            az = frame.Az,
                            fs = (cfg.Daq != null ? cfg.Daq.SamplingRate : 0),
                            frame_size = n
                        };

                        string json = JsonConvert.SerializeObject(payload);

                        try
                        {
                            // key: machineId, value: json
                            var msg = new Message<string, string>
                            {
                                Key = machineId,
                                Value = json
                            };

                            // 비동기 전송 (await 안 걸고 fire-and-forget로도 가능하지만,
                            // 여기서는 한 프레임씩 await)
                            producer.Produce(topic, msg);
                        }
                        catch (ProduceException<string, string> pex)
                        {
                            AppEvents.RaiseLog("[Kafka] Produce 예외: " + pex.Error.Reason);
                        }
                        catch (Exception ex)
                        {
                            AppEvents.RaiseLog("[Kafka] Produce 예외(일반): " + ex.Message);
                        }

                        // 10프레임마다 로그/Progress
                        if (frameIndex % 10 == 0)
                        {
                            //AppEvents.RaiseLog(string.Format(
                            //    "[Kafka] {0} frame x{1} produced (TotalSamples={2})",
                            //    _name, frameIndex, total));

                            if (progress != null)
                            {
                                var report = new AcquisitionProgress
                                {
                                    TotalSamplesSent = total,
                                    LastBatchAt = DateTimeOffset.Now,
                                    Message = string.Format(
                                        "{0} running (Kafka). frames={1}, totalSamples={2}",
                                        _name, frameIndex, total)
                                };
                                progress.Report(report);
                            }
                        }
                    }

                    // 남은 메시지 flush
                    try { producer.Flush(TimeSpan.FromSeconds(2)); } catch { }
                }

                handle.State = AcquisitionState.Canceled; // 보통 Stop으로 빠짐
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
                    string.Format("[Kafka] {0} ERROR: {1}", _name, ex.Message));
            }
            finally
            {
                lock (_syncRoot)
                {
                    _running = false;

                    if (_queue != null)
                    {
                        try { _queue.Dispose(); } catch { }
                        _queue = null;
                    }

                    if (_sessionCts != null)
                    {
                        _sessionCts.Dispose();
                        _sessionCts = null;
                    }

                    _workerTask = null;
                }

                if (handle.State == AcquisitionState.Running)
                    handle.State = AcquisitionState.Completed;

                long frames = Interlocked.Read(ref _frameCount);
                long totalSamples = Interlocked.Read(ref _totalSamples);

                AppEvents.RaiseLog(string.Format(
                    "[Kafka] {0} STOP (State={1}, Frames={2}, TotalSamples={3})",
                    _name,
                    handle.State,
                    frames,
                    totalSamples));
            }
        }

        // ============================================================
        //   IDisposable
        // ============================================================
        public void Dispose()
        {
            try
            {
                // 이벤트 구독 해제
                if (_onFrameGenerated != null)
                {
                    _source.FrameGenerated -= _onFrameGenerated;
                    _onFrameGenerated = null;
                }

                var cts = _sessionCts;
                var worker = _workerTask;

                if (cts != null && !cts.IsCancellationRequested)
                    cts.Cancel();

                if (_queue != null && !_queue.IsAddingCompleted)
                {
                    try { _queue.CompleteAdding(); } catch { }
                }

                if (worker != null)
                {
                    try { worker.Wait(500); } catch { }
                }
            }
            catch
            {
            }
            finally
            {
                if (_queue != null)
                {
                    try { _queue.Dispose(); } catch { }
                    _queue = null;
                }

                if (_sessionCts != null)
                {
                    _sessionCts.Dispose();
                    _sessionCts = null;
                }
            }
        }
    }
}
