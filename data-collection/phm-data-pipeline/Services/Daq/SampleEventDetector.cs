using System;
using System.Collections.Generic;
using phm_data_pipeline.Models;

namespace phm_data_pipeline.Services.Daq
{
    /// <summary>
    /// 프레임 스트림에서 이벤트 단위(타격 등)를 검출하는 클래스.
    /// - RMS 기반 threshold로 이벤트 시작/종료 판정
    /// - EventMinGapMs 안쪽의 "잠깐 잠잠한 구간"은 하나의 이벤트로 묶음
    /// - 여러 프레임에 걸친 신호를 하나의 AccelEvent(메타데이터 + raw 파형)로 내보냄
    /// </summary>
    public class SampleEventDetector
    {
        // ---- 구성값 ----
        private readonly long _sessionId;
        private readonly double _sampleRateHz;
        private readonly double _rmsThreshold;
        private readonly double _minGapMs;

        private readonly string _taskType;
        private readonly string _labelType;
        private readonly string _dataSplit;

        // ---- 내부 상태 ----
        // 세션 기준 샘플 인덱스 (0-based)
        private long _nextSampleIndex = 0;

        private bool _inEvent = false;
        private long _eventIndex = 0;

        private long _eventStartSampleIndex;
        private long _eventEndSampleIndex;

        // RMS 계산용 누적값 (∑x^2, ∑y^2, ∑z^2)
        private double _sumSqX;
        private double _sumSqY;
        private double _sumSqZ;
        private long _sampleCount;

        // 이벤트 동안 최대 절대값
        private double _maxAbsValue;
        private int _maxAbsChannel;    // 0=X, 1=Y, 2=Z

        // threshold 아래로 내려간 후 누적된 gap 시간 (ms)
        private double _currentGapMs;

        // ★ 이벤트 구간 raw 샘플 버퍼
        private readonly List<double> _bufAx = new List<double>();
        private readonly List<double> _bufAy = new List<double>();
        private readonly List<double> _bufAz = new List<double>();

        /// <summary>
        /// 이벤트가 검출되었을 때 발생하는 콜백.
        /// AccelEvent는 메타데이터 + 이벤트 구간 raw 파형(Ax, Ay, Az)을 포함한다.
        /// </summary>
        public event Action<AccelEvent> EventDetected;

        // -------------------------------------------------
        // 생성자
        // -------------------------------------------------

        public SampleEventDetector(
            long sessionId,
            double sampleRateHz,
            string taskType,
            string labelType,
            string dataSplit,
            double rmsThreshold,
            double minGapMs)
        {
            _sessionId = sessionId;
            _sampleRateHz = sampleRateHz;
            _taskType = taskType;
            _labelType = labelType;
            _dataSplit = dataSplit;
            _rmsThreshold = rmsThreshold;
            _minGapMs = minGapMs;
        }

        /// <summary>
        /// DaqAccelConfig에서 직접 읽어오는 헬퍼 생성자
        /// </summary>
        public SampleEventDetector(long sessionId, double sampleRateHz, DaqAccelConfig cfg)
            : this(
                  sessionId,
                  sampleRateHz,
                  cfg?.Meta?.TaskType,
                  cfg?.Meta?.LabelType,
                  cfg?.Meta?.DataSplit,
                  cfg?.Daq?.EventRmsThreshold ?? 0.5,  // 기본 threshold
                  cfg?.Daq?.EventMinGapMs ?? 200       // 기본 200ms
              )
        {
        }

        // -------------------------------------------------
        // 외부 인터페이스
        // -------------------------------------------------

        /// <summary>
        /// 프레임 하나가 들어올 때마다 호출.
        /// 내부적으로 이벤트 시작/종료를 판정하고, 종료 시 AccelEvent를 발생시킨다.
        /// </summary>
        public void OnFrame(AccelFrame frame)
        {
            if (frame == null || frame.Ax == null)
                return;

            int n = frame.Ax.Length;
            if (n <= 0)
                return;

            // 프레임 단위 특성 계산 (RMS, max abs 등)
            double rmsX = ComputeRms(frame.Ax);
            double rmsY = ComputeRms(frame.Ay);
            double rmsZ = ComputeRms(frame.Az);

            double maxRmsFrame = Math.Max(rmsX, Math.Max(rmsY, rmsZ));

            double maxAbsFrame;
            int maxAbsChFrame;
            ComputeMaxAbs(frame.Ax, frame.Ay, frame.Az, out maxAbsFrame, out maxAbsChFrame);

            // 프레임 시간/샘플 정보
            long frameStartSampleIndex = _nextSampleIndex;
            long frameEndSampleIndex = _nextSampleIndex + n - 1;

            double frameDurationMs = n * 1000.0 / _sampleRateHz;

            if (!_inEvent)
            {
                // ---- 아직 이벤트가 아닌 상태 ----
                if (maxRmsFrame >= _rmsThreshold)
                {
                    // 이벤트 시작
                    StartEvent(
                        frameStartSampleIndex,
                        frameEndSampleIndex,
                        rmsX, rmsY, rmsZ,
                        maxAbsFrame, maxAbsChFrame,
                        n);

                    // ★ 첫 프레임 샘플 버퍼에 추가
                    AppendFrameSamples(frame);
                }
                // else: 조용한 구간, 아무것도 안 함
            }
            else
            {
                // ---- 이미 이벤트 진행 중 ----
                // 이 프레임은 일단 이벤트의 tail 포함 구간으로 본다.
                ExtendEvent(
                    frameEndSampleIndex,
                    rmsX, rmsY, rmsZ,
                    maxAbsFrame, maxAbsChFrame,
                    n);

                // ★ 이벤트 버퍼에 샘플 추가
                AppendFrameSamples(frame);

                if (maxRmsFrame >= _rmsThreshold)
                {
                    // threshold 위로 다시 올라오면 gap 초기화
                    _currentGapMs = 0;
                }
                else
                {
                    // threshold 아래 → gap 누적
                    _currentGapMs += frameDurationMs;

                    if (_currentGapMs >= _minGapMs)
                    {
                        // gap이 너무 길어지면 이벤트 종료
                        EndEvent();
                    }
                }
            }

            // 다음 프레임의 시작 sample index
            _nextSampleIndex += n;
        }

        /// <summary>
        /// 세션 종료 시 호출해서, 아직 열린 이벤트가 있으면 강제로 종료.
        /// </summary>
        public void Flush()
        {
            if (_inEvent)
            {
                EndEvent();
            }
        }

        // -------------------------------------------------
        // 내부 이벤트 상태 관리
        // -------------------------------------------------

        private void StartEvent(
            long startSampleIndex,
            long endSampleIndex,
            double rmsX,
            double rmsY,
            double rmsZ,
            double maxAbsFrame,
            int maxAbsChFrame,
            int sampleCount)
        {
            _inEvent = true;
            _eventIndex++;

            _eventStartSampleIndex = startSampleIndex;
            _eventEndSampleIndex = endSampleIndex;

            // 첫 프레임 RMS 누적
            _sumSqX = rmsX * rmsX * sampleCount;
            _sumSqY = rmsY * rmsY * sampleCount;
            _sumSqZ = rmsZ * rmsZ * sampleCount;
            _sampleCount = sampleCount;

            _maxAbsValue = maxAbsFrame;
            _maxAbsChannel = maxAbsChFrame;

            _currentGapMs = 0;

            // 버퍼 초기화
            _bufAx.Clear();
            _bufAy.Clear();
            _bufAz.Clear();
        }

        private void ExtendEvent(
            long newEndSampleIndex,
            double rmsX,
            double rmsY,
            double rmsZ,
            double maxAbsFrame,
            int maxAbsChFrame,
            int sampleCount)
        {
            _eventEndSampleIndex = newEndSampleIndex;

            // RMS 누적 (∑x^2, ∑y^2, ∑z^2)
            _sumSqX += rmsX * rmsX * sampleCount;
            _sumSqY += rmsY * rmsY * sampleCount;
            _sumSqZ += rmsZ * rmsZ * sampleCount;
            _sampleCount += sampleCount;

            // 최대 절대값 갱신
            if (maxAbsFrame > _maxAbsValue)
            {
                _maxAbsValue = maxAbsFrame;
                _maxAbsChannel = maxAbsChFrame;
            }
        }

        private void AppendFrameSamples(AccelFrame frame)
        {
            if (frame.Ax != null) _bufAx.AddRange(frame.Ax);
            if (frame.Ay != null) _bufAy.AddRange(frame.Ay);
            if (frame.Az != null) _bufAz.AddRange(frame.Az);
        }

        private void EndEvent()
        {
            if (!_inEvent || _sampleCount <= 0)
                return;

            // 이벤트 전체 RMS 계산
            double rmsX = Math.Sqrt(_sumSqX / _sampleCount);
            double rmsY = Math.Sqrt(_sumSqY / _sampleCount);
            double rmsZ = Math.Sqrt(_sumSqZ / _sampleCount);

            double maxRms = Math.Max(rmsX, Math.Max(rmsY, rmsZ));

            var ev = new AccelEvent
            {
                SessionId = _sessionId,
                EventIndex = _eventIndex,
                StartSampleIndex = _eventStartSampleIndex,
                EndSampleIndex = _eventEndSampleIndex,

                // 시간 정보는 여기서는 알 수 없으므로 null.
                // 필요 시 프레임 Timestamp와 sampleRate로 외부에서 계산.
                StartTimeUtc = null,
                EndTimeUtc = null,

                MaxAbsValue = _maxAbsValue,
                MaxAbsChannel = _maxAbsChannel,
                RmsX = rmsX,
                RmsY = rmsY,
                RmsZ = rmsZ,
                MaxRms = maxRms,

                TaskType = _taskType,
                LabelType = _labelType,
                DataSplit = _dataSplit,

                // ★ 이벤트 구간 raw 파형
                Ax = _bufAx.ToArray(),
                Ay = _bufAy.ToArray(),
                Az = _bufAz.ToArray()
            };

            EventDetected?.Invoke(ev);

            // 상태 초기화
            _inEvent = false;
            _sumSqX = _sumSqY = _sumSqZ = 0;
            _sampleCount = 0;
            _maxAbsValue = 0;
            _maxAbsChannel = 0;
            _currentGapMs = 0;

            _bufAx.Clear();
            _bufAy.Clear();
            _bufAz.Clear();
        }

        // -------------------------------------------------
        // 유틸 함수 (RMS, MaxAbs)
        // -------------------------------------------------

        private static double ComputeRms(double[] a)
        {
            if (a == null || a.Length == 0) return 0;
            double s = 0;
            for (int i = 0; i < a.Length; i++)
            {
                double v = a[i];
                s += v * v;
            }
            return Math.Sqrt(s / a.Length);
        }

        private static void ComputeMaxAbs(
            double[] ax,
            double[] ay,
            double[] az,
            out double maxAbs,
            out int maxCh)
        {
            maxAbs = 0;
            maxCh = 0; // 0=X, 1=Y, 2=Z

            int nx = ax?.Length ?? 0;
            int ny = ay?.Length ?? 0;
            int nz = az?.Length ?? 0;

            int len = Math.Max(nx, Math.Max(ny, nz));

            for (int i = 0; i < len; i++)
            {
                if (ax != null && i < ax.Length)
                {
                    double v = Math.Abs(ax[i]);
                    if (v > maxAbs)
                    {
                        maxAbs = v;
                        maxCh = 0;
                    }
                }

                if (ay != null && i < ay.Length)
                {
                    double v = Math.Abs(ay[i]);
                    if (v > maxAbs)
                    {
                        maxAbs = v;
                        maxCh = 1;
                    }
                }

                if (az != null && i < az.Length)
                {
                    double v = Math.Abs(az[i]);
                    if (v > maxAbs)
                    {
                        maxAbs = v;
                        maxCh = 2;
                    }
                }
            }
        }
    }
}
