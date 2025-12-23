using phm_data_pipeline.Models;
using phm_data_pipeline.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using WeifenLuo.WinFormsUI.Docking;
using System.Windows.Forms.DataVisualization.Charting;
using System.Drawing;

namespace phm_data_pipeline.UI
{
    public class LogGraphForm : DockContent
    {
        private Chart _chart;
        private IAccelFrameSource _currentSource;

        private double _currentSampleRateHz = 0.0;

        // 30초 / 60초에서 사용할 프레임 개수 기준 (대략)
        private const int WindowFrames30s = 300;   // 100 ms/frame 가정
        private const int WindowFrames60s = 600;
        private int _maxFramesInWindow = WindowFrames30s;

        // 프레임 당 그래프에 찍을 최대 포인트 수 (강한 다운샘플링)
        private const int PointsPerFrame = 32;

        // FFT: NFFT 최대 길이 & 출력 포인트 수도 제한
        private const int MaxFftLength = 1024;
        private const int MaxFftPoints = 256;

        // FFT 평균 (지수 평균)
        private double[] _fftAvgX;
        private double[] _fftAvgY;
        private double[] _fftAvgZ;
        private bool _fftAvgInitialized = false;
        private const double FftAvgAlpha = 0.2;

        // FFT 업데이트 최소 간격 (너무 빨리 바뀌지 않게)
        private DateTimeOffset _lastFftUpdate = DateTimeOffset.MinValue;
        private static readonly TimeSpan FftUpdateMinInterval = TimeSpan.FromMilliseconds(500);

        // 피크 표시 개수
        private const int MaxFftPeaksPerAxis = 3;

        private ComboBox _cboWindow;   // 윗쪽에 30초/60초 선택용

        public LogGraphForm()
        {
            InitializeComponent();

            Text = "Log Graph Viewer";

            AppEvents.FrameSourceChanged += OnFrameSourceChanged;

            // 처음엔 빈 차트 상태 유지
            // if (AppEvents.CurrentFrameSource != null)
            // {
            //     OnFrameSourceChanged(AppEvents.CurrentFrameSource);
            // }
        }

        private void InitializeComponent()
        {
            this.Text = "실시간 가속도 그래프";
            this.ClientSize = new System.Drawing.Size(900, 600);

            var main = new TableLayoutPanel();
            main.Dock = DockStyle.Fill;
            main.RowCount = 2;
            main.ColumnCount = 1;
            main.RowStyles.Add(new RowStyle(SizeType.AutoSize));          // 상단 컨트롤
            main.RowStyles.Add(new RowStyle(SizeType.Percent, 100F));     // 차트
            this.Controls.Add(main);

            // ===== 상단: 윈도우 길이 선택 =====
            var top = new FlowLayoutPanel();
            top.Dock = DockStyle.Fill;
            top.AutoSize = true;
            top.AutoSizeMode = AutoSizeMode.GrowAndShrink;

            var lblWindow = new Label();
            lblWindow.Text = "표시 구간:";
            lblWindow.AutoSize = true;
            lblWindow.Padding = new Padding(0, 6, 4, 0);

            _cboWindow = new ComboBox();
            _cboWindow.DropDownStyle = ComboBoxStyle.DropDownList;
            _cboWindow.Items.AddRange(new object[] { "최근 30초", "최근 60초" });
            _cboWindow.SelectedIndex = 0;
            _cboWindow.SelectedIndexChanged += CboWindow_SelectedIndexChanged;

            top.Controls.Add(lblWindow);
            top.Controls.Add(_cboWindow);

            main.Controls.Add(top, 0, 0);

            // ===== 차트 =====
            _chart = new Chart();
            _chart.Dock = DockStyle.Fill;

            // ★ Time 도메인 영역
            var areaTime = new ChartArea("Time");
            areaTime.AxisX.Title = "Sample Index (최근 구간)";
            areaTime.AxisY.Title = "Acceleration (g)";
            areaTime.AxisX.MajorGrid.LineDashStyle = ChartDashStyle.Dot;
            areaTime.AxisY.MajorGrid.LineDashStyle = ChartDashStyle.Dot;

            // 확대/축소 + 드래그
            areaTime.AxisX.ScaleView.Zoomable = true;
            areaTime.AxisY.ScaleView.Zoomable = true;
            areaTime.CursorX.IsUserEnabled = true;
            areaTime.CursorX.IsUserSelectionEnabled = true;
            areaTime.CursorY.IsUserEnabled = true;
            areaTime.CursorY.IsUserSelectionEnabled = true;

            // ★ FFT 영역 (아래쪽)
            var areaFreq = new ChartArea("Freq");
            areaFreq.AlignWithChartArea = "Time";
            areaFreq.AlignmentOrientation = AreaAlignmentOrientations.Vertical;
            areaFreq.AlignmentStyle = AreaAlignmentStyles.Position;

            areaTime.Position.FromRectangleF(new System.Drawing.RectangleF(0, 10, 100, 40)); // 위쪽 40%, 위 10%는 legend 영역 느낌
            areaFreq.Position.FromRectangleF(new System.Drawing.RectangleF(0, 50, 100, 50)); // 아래 50%

            areaFreq.AxisX.Title = "Frequency (Hz)";
            areaFreq.AxisY.Title = "Magnitude";
            areaFreq.AxisX.MajorGrid.LineDashStyle = ChartDashStyle.Dot;
            areaFreq.AxisY.MajorGrid.LineDashStyle = ChartDashStyle.Dot;
            areaFreq.AxisX.IsMarginVisible = false;

            areaFreq.AxisX.ScaleView.Zoomable = true;
            areaFreq.AxisY.ScaleView.Zoomable = true;
            areaFreq.CursorX.IsUserEnabled = true;
            areaFreq.CursorX.IsUserSelectionEnabled = true;
            areaFreq.CursorY.IsUserEnabled = true;
            areaFreq.CursorY.IsUserSelectionEnabled = true;

            _chart.ChartAreas.Add(areaTime);
            _chart.ChartAreas.Add(areaFreq);

            // ★ 범례
            var legend = new Legend();
            legend.Docking = Docking.Top;
            legend.IsDockedInsideChartArea = false;
            _chart.Legends.Add(legend);

            // ===== Time 도메인 시리즈 =====
            var sx = new Series("Ax");
            sx.ChartType = SeriesChartType.FastLine;
            sx.ChartArea = "Time";
            sx.IsXValueIndexed = true;
            sx.Color = System.Drawing.Color.Red;

            var sy = new Series("Ay");
            sy.ChartType = SeriesChartType.FastLine;
            sy.ChartArea = "Time";
            sy.IsXValueIndexed = true;
            sy.Color = System.Drawing.Color.Green;

            var sz = new Series("Az");
            sz.ChartType = SeriesChartType.FastLine;
            sz.ChartArea = "Time";
            sz.IsXValueIndexed = true;
            sz.Color = System.Drawing.Color.Blue;

            // ===== FFT 시리즈 =====
            var sfx = new Series("Ax_FFT");
            sfx.ChartType = SeriesChartType.FastLine;
            sfx.ChartArea = "Freq";
            sfx.Color = System.Drawing.Color.Red;     // ★ Ax와 동일 색

            var sfy = new Series("Ay_FFT");
            sfy.ChartType = SeriesChartType.FastLine;
            sfy.ChartArea = "Freq";
            sfy.Color = System.Drawing.Color.Green;   // ★ Ay와 동일 색

            var sfz = new Series("Az_FFT");
            sfz.ChartType = SeriesChartType.FastLine;
            sfz.ChartArea = "Freq";
            sfz.Color = System.Drawing.Color.Blue;    // ★ Az와 동일 색

            // ===== FFT 피크 시리즈 (동그라미 + 라벨) =====
            var sfxPeak = new Series("Ax_Peaks");
            sfxPeak.ChartType = SeriesChartType.Point;
            sfxPeak.ChartArea = "Freq";
            sfxPeak.MarkerStyle = MarkerStyle.Circle;
            sfxPeak.MarkerSize = 7;
            sfxPeak.IsXValueIndexed = false;
            sfxPeak.IsVisibleInLegend = false;
            sfxPeak.Color = System.Drawing.Color.Red;     // ★ Ax와 동일

            var sfyPeak = new Series("Ay_Peaks");
            sfyPeak.ChartType = SeriesChartType.Point;
            sfyPeak.ChartArea = "Freq";
            sfyPeak.MarkerStyle = MarkerStyle.Circle;
            sfyPeak.MarkerSize = 7;
            sfyPeak.IsXValueIndexed = false;
            sfyPeak.IsVisibleInLegend = false;
            sfyPeak.Color = System.Drawing.Color.Green;   // ★ Ay와 동일

            var sfzPeak = new Series("Az_Peaks");
            sfzPeak.ChartType = SeriesChartType.Point;
            sfzPeak.ChartArea = "Freq";
            sfzPeak.MarkerStyle = MarkerStyle.Circle;
            sfzPeak.MarkerSize = 7;
            sfzPeak.IsXValueIndexed = false;
            sfzPeak.IsVisibleInLegend = false;
            sfzPeak.Color = System.Drawing.Color.Blue;    // ★ Az와 동일

            _chart.Series.Add(sx);
            _chart.Series.Add(sy);
            _chart.Series.Add(sz);

            _chart.Series.Add(sfx);
            _chart.Series.Add(sfy);
            _chart.Series.Add(sfz);

            _chart.Series.Add(sfxPeak);
            _chart.Series.Add(sfyPeak);
            _chart.Series.Add(sfzPeak);

            _chart.Series["Ax_FFT"].BorderWidth = 2;
            _chart.Series["Ay_FFT"].BorderWidth = 2;
            _chart.Series["Az_FFT"].BorderWidth = 2;

            // ★ 더블클릭으로 줌 리셋
            _chart.MouseDoubleClick += Chart_MouseDoubleClick;

            main.Controls.Add(_chart, 0, 1);
        }

        public void UpdateSamplingRate(double fsHz)
        {
            if (fsHz <= 0) return;
            _currentSampleRateHz = fsHz;
        }

        private void CboWindow_SelectedIndexChanged(object sender, EventArgs e)
        {
            _maxFramesInWindow = (_cboWindow.SelectedIndex == 0)
                ? WindowFrames30s
                : WindowFrames60s;
        }

        private void Chart_MouseDoubleClick(object sender, MouseEventArgs e)
        {
            foreach (var area in _chart.ChartAreas)
            {
                area.AxisX.ScaleView.ZoomReset(0);
                area.AxisY.ScaleView.ZoomReset(0);
            }
        }

        private void OnFrameSourceChanged(IAccelFrameSource newSource)
        {
            if (IsHandleCreated && InvokeRequired)
            {
                try { BeginInvoke((Action)(() => OnFrameSourceChanged(newSource))); }
                catch { }
                return;
            }

            // "새로운 소스가 붙는다"는 상황 체크
            bool isNewStart = (_currentSource == null && newSource != null);

            DetachCurrentSource();

            _currentSource = newSource;
            if (_currentSource != null)
            {
                _currentSource.FrameGenerated += OnFrameGenerated;
                _currentSampleRateHz = _currentSource.SampleRate;
            }
            else
            {
                _currentSampleRateHz = 0.0;
            }

            // ★ 새로 Start 되는 경우에만 차트 초기화
            if (isNewStart && _chart != null && _chart.Series.Count > 0)
            {
                foreach (var s in _chart.Series)
                    s.Points.Clear();
            }

            // FFT 평균 상태 리셋
            _fftAvgX = _fftAvgY = _fftAvgZ = null;
            _fftAvgInitialized = false;
            _lastFftUpdate = DateTimeOffset.MinValue;
        }

        private void DetachCurrentSource()
        {
            if (_currentSource != null)
            {
                _currentSource.FrameGenerated -= OnFrameGenerated;
                _currentSource = null;
            }
        }

        private void OnFrameGenerated(AccelFrame frame)
        {
            if (frame == null) return;

            if (IsHandleCreated && InvokeRequired)
            {
                try { BeginInvoke((Action)(() => OnFrameGenerated(frame))); }
                catch { }
                return;
            }

            if (_chart == null || _chart.Series.Count < 9)
                return;

            var sx = _chart.Series["Ax"];
            var sy = _chart.Series["Ay"];
            var sz = _chart.Series["Az"];
            var sfx = _chart.Series["Ax_FFT"];
            var sfy = _chart.Series["Ay_FFT"];
            var sfz = _chart.Series["Az_FFT"];

            var sfxPeak = _chart.Series["Ax_Peaks"];
            var sfyPeak = _chart.Series["Ay_Peaks"];
            var sfzPeak = _chart.Series["Az_Peaks"];

            var ax = frame.Ax ?? Array.Empty<double>();
            var ay = frame.Ay ?? Array.Empty<double>();
            var az = frame.Az ?? Array.Empty<double>();

            int n = ax.Length;
            if (ay.Length < n) n = ay.Length;
            if (az.Length < n) n = az.Length;
            if (n <= 0) return;

            // ===== 1) 타임 도메인: 다운샘플 + 슬라이딩 윈도우 =====

            int step = n / PointsPerFrame;
            if (step < 1) step = 1;

            for (int i = 0; i < n; i += step)
            {
                sx.Points.AddY(ax[i]);
                sy.Points.AddY(ay[i]);
                sz.Points.AddY(az[i]);
            }

            int maxPointsTime = _maxFramesInWindow * PointsPerFrame;

            // 항상 세 축 포인트 개수는 같다고 가정
            while (sx.Points.Count > maxPointsTime)
            {
                sx.Points.RemoveAt(0);
                sy.Points.RemoveAt(0);
                sz.Points.RemoveAt(0);
            }

            _chart.ChartAreas["Time"].RecalculateAxesScale();

            // ===== 2) FFT 도메인: 업데이트 속도 제한 =====
            if (frame.Timestamp - _lastFftUpdate < FftUpdateMinInterval)
            {
                return; // 타임 도메인은 갱신했지만 FFT는 이번 프레임에는 스킵
            }
            _lastFftUpdate = frame.Timestamp;

            int nfft = n;
            if (nfft > MaxFftLength) nfft = MaxFftLength;

            double fs = 0.0;

            if (_currentSource != null && _currentSource.SampleRate > 0)
                fs = _currentSource.SampleRate;
            else
                fs = _currentSampleRateHz;

            if (nfft > 0 && fs > 0)
            {
                double nyquist = fs / 2.0;

                var magX = ComputeMagnitudeSpectrum(ax, nfft);
                var magY = ComputeMagnitudeSpectrum(ay, nfft);
                var magZ = ComputeMagnitudeSpectrum(az, nfft);

                int half = magX.Length;

                // === 3) 지수 평균(Exponential Averaging) 적용 ===
                if (!_fftAvgInitialized || _fftAvgX == null || _fftAvgX.Length != half)
                {
                    _fftAvgX = (double[])magX.Clone();
                    _fftAvgY = (double[])magY.Clone();
                    _fftAvgZ = (double[])magZ.Clone();
                    _fftAvgInitialized = true;
                }
                else
                {
                    double beta = 1.0 - FftAvgAlpha;

                    for (int i = 0; i < half; i++)
                    {
                        _fftAvgX[i] = FftAvgAlpha * magX[i] + beta * _fftAvgX[i];
                        _fftAvgY[i] = FftAvgAlpha * magY[i] + beta * _fftAvgY[i];
                        _fftAvgZ[i] = FftAvgAlpha * magZ[i] + beta * _fftAvgZ[i];
                    }
                }

                int stepF = half / MaxFftPoints;
                if (stepF < 1) stepF = 1;

                sfx.Points.Clear();
                sfy.Points.Clear();
                sfz.Points.Clear();
                sfxPeak.Points.Clear();
                sfyPeak.Points.Clear();
                sfzPeak.Points.Clear();

                for (int k = 0; k < half; k += stepF)
                {
                    double freq = (double)k * fs / nfft;

                    sfx.Points.AddXY(freq, _fftAvgX[k]);
                    sfy.Points.AddXY(freq, _fftAvgY[k]);
                    sfz.Points.AddXY(freq, _fftAvgZ[k]);
                }

                // === 4) 피크 검출 + 표시 (평균 스펙트럼 기준) ===
                var peaksX = FindTopPeaks(_fftAvgX, fs, nfft, MaxFftPeaksPerAxis);
                var peaksY = FindTopPeaks(_fftAvgY, fs, nfft, MaxFftPeaksPerAxis);
                var peaksZ = FindTopPeaks(_fftAvgZ, fs, nfft, MaxFftPeaksPerAxis);

                foreach (var p in peaksX)
                {
                    int idx = sfxPeak.Points.AddXY(p.freq, p.mag);
                    sfxPeak.Points[idx].Label = $"{p.freq:F1} Hz";
                }

                foreach (var p in peaksY)
                {
                    int idx = sfyPeak.Points.AddXY(p.freq, p.mag);
                    sfyPeak.Points[idx].Label = $"{p.freq:F1} Hz";
                }

                foreach (var p in peaksZ)
                {
                    int idx = sfzPeak.Points.AddXY(p.freq, p.mag);
                    sfzPeak.Points[idx].Label = $"{p.freq:F1} Hz";
                }

                var areaFreq = _chart.ChartAreas["Freq"];
                areaFreq.AxisX.Minimum = 0;
                areaFreq.AxisX.Maximum = nyquist;
                areaFreq.AxisX.Title = "Frequency (Hz)";
                areaFreq.RecalculateAxesScale();
            }
        }

        private static double[] ComputeMagnitudeSpectrum(double[] x, int nfft)
        {
            if (x == null) return Array.Empty<double>();
            if (nfft <= 0) return Array.Empty<double>();

            int n = x.Length;
            if (n < nfft) nfft = n;
            if (nfft <= 0) return Array.Empty<double>();

            int half = nfft / 2;
            if (half <= 0) return Array.Empty<double>();

            var mag = new double[half];

            // Hann window
            var window = new double[nfft];
            for (int i = 0; i < nfft; i++)
            {
                window[i] = 0.5 * (1.0 - Math.Cos(2.0 * Math.PI * i / (nfft - 1)));
            }

            for (int k = 0; k < half; k++)
            {
                double re = 0.0;
                double im = 0.0;
                double omega = -2.0 * Math.PI * k / nfft;

                for (int nIdx = 0; nIdx < nfft; nIdx++)
                {
                    double angle = omega * nIdx;
                    double v = x[nIdx] * window[nIdx];

                    re += v * Math.Cos(angle);
                    im += v * Math.Sin(angle);
                }

                mag[k] = Math.Sqrt(re * re + im * im);
            }

            return mag;
        }

        // FFT 평균 스펙트럼에서 상위 피크 찾기
        private static List<(double freq, double mag)> FindTopPeaks(
            double[] mag,
            double fs,
            int nfft,
            int maxPeaks)
        {
            var peaks = new List<(double freq, double mag)>();
            if (mag == null || mag.Length < 3 || fs <= 0 || nfft <= 0)
                return peaks;

            int half = mag.Length;

            for (int k = 1; k < half - 1; k++)
            {
                double m = mag[k];
                if (m > mag[k - 1] && m > mag[k + 1])
                {
                    double freq = (double)k * fs / nfft;
                    peaks.Add((freq, m));
                }
            }

            peaks.Sort((a, b) => b.mag.CompareTo(a.mag));
            if (peaks.Count > maxPeaks)
                peaks = peaks.GetRange(0, maxPeaks);

            return peaks;
        }

        protected override void OnFormClosed(FormClosedEventArgs e)
        {
            AppEvents.FrameSourceChanged -= OnFrameSourceChanged;
            DetachCurrentSource();
            base.OnFormClosed(e);
        }
    }
}
