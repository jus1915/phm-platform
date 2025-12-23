// 파일: UI/MlflowMonitorForm.cs
using System;
using System.Threading.Tasks;
using System.Windows.Forms;
using phm_data_pipeline.Services;
using WeifenLuo.WinFormsUI.Docking;
using System.Windows.Forms.DataVisualization.Charting;
using System.Collections.Generic;

namespace phm_data_pipeline.UI
{
    public partial class MlflowMonitorForm : DockContent
    {
        private readonly MlflowClient _client;

        private const string MlflowBaseUrl = "http://localhost:5000";

        private const string TrainMetricKey = "train_accuracy"; // 또는 "train_acc"
        private const string ValMetricKey = "val_accuracy";     // 또는 "val_acc"
        private const string TestMetricKey = "test_accuracy";   // 또는 "test_acc"

        private string? _experimentId;
        private string? _currentRunId;

        private readonly Timer _timer;
        private bool _isUpdating = false;
        private bool _suppressComboEvent = false;

        private ComboBox _cmbExperiments;
        private ComboBox _cmbRuns;
        private CheckBox _chkFollowLatest;
        private Button _btnRefreshRuns;

        private Label _lblRunIdCaption;
        private Label _lblStatusCaption;
        private Label _lblTrainCaption;
        private Label _lblValCaption;
        private Label _lblTestCaption;

        private Label _lblRunId;
        private Label _lblStatus;
        private Label _lblTrain;
        private Label _lblVal;
        private Label _lblTest;

        private Chart _chart;

        private class RunListItem
        {
            public string RunId { get; }
            public string Display { get; }

            public RunListItem(string runId, string display)
            {
                RunId = runId;
                Display = display;
            }

            public override string ToString() => Display;
        }

        private class ComboBoxItemExperiment
        {
            public string ExperimentId { get; }
            public string Name { get; }

            public ComboBoxItemExperiment(string experimentId, string name)
            {
                ExperimentId = experimentId;
                Name = name;
            }

            public override string ToString() => $"{Name} ({ExperimentId})";
        }

        public MlflowMonitorForm()
        {
            InitializeComponent();

            _client = new MlflowClient(MlflowBaseUrl);

            _timer = new Timer();
            _timer.Interval = 2000;
            _timer.Tick += async (s, e) => await TimerTickAsync();
        }

        private void InitializeComponent()
        {
            this.Text = "MLflow Monitor";
            this.TabText = "MLflow Monitor";
            this.HideOnClose = true;
            this.ClientSize = new System.Drawing.Size(720, 400);

            InitializeUi();
        }

        private void InitializeUi()
        {
            var split = new SplitContainer
            {
                Dock = DockStyle.Fill,
                Orientation = Orientation.Horizontal,
                SplitterDistance = 130
            };

            var table = new TableLayoutPanel
            {
                Dock = DockStyle.Fill,
                ColumnCount = 2,
                RowCount = 7,
                AutoSize = true,
                Padding = new Padding(8)
            };

            table.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 25));
            table.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 75));

            for (int i = 0; i < 7; i++)
            {
                table.RowStyles.Add(new RowStyle(SizeType.Percent, 100f / 7f));
            }

            // Row 0: Experiment 선택
            var lblExpSelect = new Label
            {
                Text = "Experiment:",
                Dock = DockStyle.Fill,
                TextAlign = System.Drawing.ContentAlignment.MiddleLeft
            };

            _cmbExperiments = new ComboBox
            {
                DropDownStyle = ComboBoxStyle.DropDownList,
                Width = 320
            };
            _cmbExperiments.SelectedIndexChanged += CmbExperiments_SelectedIndexChanged;

            table.Controls.Add(lblExpSelect, 0, 0);
            table.Controls.Add(_cmbExperiments, 1, 0);

            // Row 1: Run 선택
            var lblRunSelect = new Label
            {
                Text = "Run 선택:",
                Dock = DockStyle.Fill,
                TextAlign = System.Drawing.ContentAlignment.MiddleLeft
            };

            var panelRunSelect = new FlowLayoutPanel
            {
                Dock = DockStyle.Fill,
                FlowDirection = FlowDirection.LeftToRight,
                AutoSize = true
            };

            _cmbRuns = new ComboBox
            {
                DropDownStyle = ComboBoxStyle.DropDownList,
                Width = 320
            };
            _cmbRuns.SelectedIndexChanged += CmbRuns_SelectedIndexChanged;

            _chkFollowLatest = new CheckBox
            {
                Text = "최신 Run 자동 추적",
                AutoSize = true,
                Checked = true
            };

            _btnRefreshRuns = new Button
            {
                Text = "새로고침",
                AutoSize = true
            };
            _btnRefreshRuns.Click += async (s, e) => await LoadRunListAsync();

            panelRunSelect.Controls.Add(_cmbRuns);
            panelRunSelect.Controls.Add(_chkFollowLatest);
            panelRunSelect.Controls.Add(_btnRefreshRuns);

            table.Controls.Add(lblRunSelect, 0, 1);
            table.Controls.Add(panelRunSelect, 1, 1);

            // Row 2~6: Run 정보 / metrics
            _lblRunIdCaption = new Label { Text = "RunId:", Dock = DockStyle.Fill, TextAlign = System.Drawing.ContentAlignment.MiddleLeft };
            _lblStatusCaption = new Label { Text = "Status:", Dock = DockStyle.Fill, TextAlign = System.Drawing.ContentAlignment.MiddleLeft };
            _lblTrainCaption = new Label { Text = $"{TrainMetricKey}:", Dock = DockStyle.Fill, TextAlign = System.Drawing.ContentAlignment.MiddleLeft };
            _lblValCaption = new Label { Text = $"{ValMetricKey}:", Dock = DockStyle.Fill, TextAlign = System.Drawing.ContentAlignment.MiddleLeft };
            _lblTestCaption = new Label { Text = $"{TestMetricKey}:", Dock = DockStyle.Fill, TextAlign = System.Drawing.ContentAlignment.MiddleLeft };

            _lblRunId = new Label { Text = "-", Dock = DockStyle.Fill, TextAlign = System.Drawing.ContentAlignment.MiddleLeft };
            _lblStatus = new Label { Text = "-", Dock = DockStyle.Fill, TextAlign = System.Drawing.ContentAlignment.MiddleLeft };
            _lblTrain = new Label { Text = "-", Dock = DockStyle.Fill, TextAlign = System.Drawing.ContentAlignment.MiddleLeft };
            _lblVal = new Label { Text = "-", Dock = DockStyle.Fill, TextAlign = System.Drawing.ContentAlignment.MiddleLeft };
            _lblTest = new Label { Text = "-", Dock = DockStyle.Fill, TextAlign = System.Drawing.ContentAlignment.MiddleLeft };

            table.Controls.Add(_lblRunIdCaption, 0, 2);
            table.Controls.Add(_lblRunId, 1, 2);
            table.Controls.Add(_lblStatusCaption, 0, 3);
            table.Controls.Add(_lblStatus, 1, 3);
            table.Controls.Add(_lblTrainCaption, 0, 4);
            table.Controls.Add(_lblTrain, 1, 4);
            table.Controls.Add(_lblValCaption, 0, 5);
            table.Controls.Add(_lblVal, 1, 5);
            table.Controls.Add(_lblTestCaption, 0, 6);
            table.Controls.Add(_lblTest, 1, 6);

            split.Panel1.Controls.Add(table);

            // 하단: Chart
            _chart = new Chart { Dock = DockStyle.Fill };
            var area = new ChartArea("Default");
            area.AxisX.LabelStyle.Format = "HH:mm:ss";
            area.AxisX.MajorGrid.Enabled = false;
            area.AxisY.MajorGrid.LineDashStyle = ChartDashStyle.Dot;
            area.AxisY.Title = "Metric value";

            area.AxisY.Minimum = 0.0;
            area.AxisY.Maximum = 1.0;

            _chart.ChartAreas.Add(area);

            var trainSeries = new Series("Train")
            {
                ChartType = SeriesChartType.Line,
                XValueType = ChartValueType.DateTime,
                BorderWidth = 2,
                MarkerStyle = MarkerStyle.Circle,
                MarkerSize = 7
            };
            var valSeries = new Series("Val")
            {
                ChartType = SeriesChartType.Line,
                XValueType = ChartValueType.DateTime,
                BorderWidth = 2,
                MarkerStyle = MarkerStyle.Circle,
                MarkerSize = 7
            };
            var testSeries = new Series("Test")
            {
                ChartType = SeriesChartType.Line,
                XValueType = ChartValueType.DateTime,
                BorderWidth = 2,
                MarkerStyle = MarkerStyle.Circle,
                MarkerSize = 7
            };

            _chart.Series.Add(trainSeries);
            _chart.Series.Add(valSeries);
            _chart.Series.Add(testSeries);
            _chart.Legends.Add(new Legend("Legend"));

            split.Panel2.Controls.Add(_chart);

            this.Controls.Add(split);
        }

        protected override async void OnLoad(EventArgs e)
        {
            base.OnLoad(e);

            _lblRunId.Text = "-";
            _lblStatus.Text = "Experiment 목록 조회 중...";
            _lblTrain.Text = "-";
            _lblVal.Text = "-";
            _lblTest.Text = "-";

            try
            {
                await LoadExperimentListAsync();

                if (_experimentId == null)
                {
                    _lblStatus.Text = "Experiment 없음 (MLflow에 Experiment가 있는지 확인)";
                    return;
                }

                _lblStatus.Text = "Run 목록 조회 중...";
                await LoadRunListAsync();

                _timer.Start();
                _lblStatus.Text = "대기 중";
            }
            catch (Exception ex)
            {
                _lblStatus.Text = "Experiment 로드 에러";
                MessageBox.Show(this, ex.Message, "MLflow Experiment 로드 에러", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        }

        private async Task LoadExperimentListAsync()
        {
            try
            {
                _suppressComboEvent = true;

                var exps = await _client.ListExperimentsAsync();

                _cmbExperiments.Items.Clear();

                foreach (var e in exps)
                {
                    _cmbExperiments.Items.Add(new ComboBoxItemExperiment(e.ExperimentId, e.Name));
                }

                if (_cmbExperiments.Items.Count == 0)
                {
                    _experimentId = null;
                    AppEvents.RaiseLog("[MLflow][UI] Experiment 콤보 비어 있음");
                    return;
                }

                _cmbExperiments.SelectedIndex = 0;
                var selected = (ComboBoxItemExperiment)_cmbExperiments.SelectedItem;
                _experimentId = selected.ExperimentId;

                AppEvents.RaiseLog($"[MLflow][UI] Experiment 선택: id={_experimentId}, name={selected.Name}");

                _currentRunId = null;
                ClearChartAndHeader();
            }
            finally
            {
                _suppressComboEvent = false;
            }
        }

        private void ClearChartAndHeader()
        {
            _lblRunId.Text = "-";
            _lblStatus.Text = "대기 중";
            _lblTrain.Text = "-";
            _lblVal.Text = "-";
            _lblTest.Text = "-";

            if (_chart != null)
            {
                foreach (var s in _chart.Series)
                    s.Points.Clear();
            }
        }

        private async void CmbExperiments_SelectedIndexChanged(object sender, EventArgs e)
        {
            if (_suppressComboEvent)
                return;

            var item = _cmbExperiments.SelectedItem as ComboBoxItemExperiment;
            if (item == null)
                return;

            _experimentId = item.ExperimentId;
            _currentRunId = null;
            ClearChartAndHeader();

            AppEvents.RaiseLog($"[MLflow][UI] Experiment 변경: id={_experimentId}, name={item.Name}");

            _lblStatus.Text = "Run 목록 조회 중...";
            await LoadRunListAsync();
            _lblStatus.Text = "대기 중";
        }

        private async Task LoadRunListAsync()
        {
            if (_experimentId == null)
                return;

            try
            {
                _suppressComboEvent = true;

                var runs = await _client.ListRecentRunsAsync(_experimentId, maxResults: 20);

                _cmbRuns.Items.Clear();

                foreach (var r in runs)
                {
                    var startStr = r.StartTime?.ToLocalTime().ToString("MM-dd HH:mm") ?? "??-?? ??:??";
                    var shortId = r.RunId?.Substring(0, Math.Min(8, r.RunId.Length)) ?? "";
                    var namePart = string.IsNullOrEmpty(r.RunName) ? "" : $" | {r.RunName}";
                    var disp = $"{startStr} | {r.Status} | {shortId}{namePart}";
                    _cmbRuns.Items.Add(new RunListItem(r.RunId, disp));
                }

                if (_cmbRuns.Items.Count == 0)
                {
                    _currentRunId = null;
                    _lblStatus.Text = "Run 없음";
                    AppEvents.RaiseLog("[MLflow][UI] 선택된 Experiment에 Run 없음");
                    return;
                }

                if (string.IsNullOrEmpty(_currentRunId))
                {
                    var firstItem = (RunListItem)_cmbRuns.Items[0];
                    _currentRunId = firstItem.RunId;
                    _cmbRuns.SelectedIndex = 0;
                    await RefreshCurrentRunAsync();
                }
                else
                {
                    for (int i = 0; i < _cmbRuns.Items.Count; i++)
                    {
                        var item = (RunListItem)_cmbRuns.Items[i];
                        if (item.RunId == _currentRunId)
                        {
                            _cmbRuns.SelectedIndex = i;
                            break;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _lblStatus.Text = "Run 목록 로드 에러";
                AppEvents.RaiseLog($"[MLflow][UI][RunList][EXCEPTION] ex={ex}");
            }
            finally
            {
                _suppressComboEvent = false;
            }
        }

        private async Task TimerTickAsync()
        {
            if (_isUpdating)
                return;

            if (!_chkFollowLatest.Checked)
                return;

            _isUpdating = true;
            try
            {
                if (_experimentId == null)
                    return;

                var latest = await _client.GetLatestRunInfoAsync(_experimentId);
                if (latest != null && latest.RunId != _currentRunId)
                {
                    _currentRunId = latest.RunId;
                    await LoadRunListAsync();
                    UpdateHeaderFromRun(latest);
                    await UpdateChartAsync(_currentRunId);
                    return;
                }

                if (string.IsNullOrEmpty(_currentRunId))
                    return;

                var runInfo = await _client.GetRunInfoAsync(_currentRunId);
                if (runInfo == null)
                    return;

                UpdateHeaderFromRun(runInfo);
                await UpdateChartAsync(_currentRunId);
            }
            catch (Exception ex)
            {
                _lblStatus.Text = "자동 갱신 에러";
                AppEvents.RaiseLog($"[MLflow][UI][Timer][EXCEPTION] ex={ex}");
            }
            finally
            {
                _isUpdating = false;
            }
        }

        private async void CmbRuns_SelectedIndexChanged(object sender, EventArgs e)
        {
            if (_suppressComboEvent)
                return;

            var item = _cmbRuns.SelectedItem as RunListItem;
            if (item == null)
                return;

            _currentRunId = item.RunId;

            if (_chkFollowLatest.Checked)
                _chkFollowLatest.Checked = false;

            await RefreshCurrentRunAsync();
        }

        private async Task RefreshCurrentRunAsync()
        {
            if (string.IsNullOrEmpty(_currentRunId))
                return;

            try
            {
                var runInfo = await _client.GetRunInfoAsync(_currentRunId);
                if (runInfo == null)
                    return;

                UpdateHeaderFromRun(runInfo);
                await UpdateChartAsync(_currentRunId);
            }
            catch (Exception ex)
            {
                _lblStatus.Text = "Run 정보 로드 에러";
                AppEvents.RaiseLog($"[MLflow][UI][RefreshRun][EXCEPTION] ex={ex}");
            }
        }

        private void UpdateHeaderFromRun(MlflowRunInfo runInfo)
        {
            _lblRunId.Text = runInfo.RunId ?? "-";
            _lblStatus.Text = runInfo.Status ?? "-";

            if (runInfo.Metrics.TryGetValue(TrainMetricKey, out var trainVal))
                _lblTrain.Text = trainVal.ToString("F4");
            else
                _lblTrain.Text = "-";

            if (runInfo.Metrics.TryGetValue(ValMetricKey, out var valVal))
                _lblVal.Text = valVal.ToString("F4");
            else
                _lblVal.Text = "-";

            if (runInfo.Metrics.TryGetValue(TestMetricKey, out var testVal))
                _lblTest.Text = testVal.ToString("F4");
            else
                _lblTest.Text = "-";
        }

        private async Task UpdateChartAsync(string runId)
        {
            try
            {
                var trainHistory = await _client.GetMetricHistoryAsync(runId, TrainMetricKey);
                var valHistory = await _client.GetMetricHistoryAsync(runId, ValMetricKey);
                var testHistory = await _client.GetMetricHistoryAsync(runId, TestMetricKey);

                var trainSeries = _chart.Series["Train"];
                var valSeries = _chart.Series["Val"];
                var testSeries = _chart.Series["Test"];

                trainSeries.Points.Clear();
                valSeries.Points.Clear();
                testSeries.Points.Clear();

                const int MaxPoints = 200;

                void FillSeries(Series series, List<(DateTime time, double value)> history, string name)
                {
                    if (history == null || history.Count == 0)
                    {
                        AppEvents.RaiseLog(
                            $"[MLflow][Chart] run_id={runId}, series={name}, history=0");
                        return;
                    }

                    var start = Math.Max(0, history.Count - MaxPoints);
                    for (int i = start; i < history.Count; i++)
                    {
                        var (time, value) = history[i];
                        series.Points.AddXY(time, value);
                    }

                    //AppEvents.RaiseLog(
                    //    $"[MLflow][Chart] run_id={runId}, series={name}, history={history.Count}, points={series.Points.Count}");
                }

                FillSeries(trainSeries, trainHistory, "Train");
                FillSeries(valSeries, valHistory, "Val");
                FillSeries(testSeries, testHistory, "Test");

                _chart.ChartAreas[0].RecalculateAxesScale();
            }
            catch (Exception ex)
            {
                AppEvents.RaiseLog($"[MLflow][Chart][EXCEPTION] run_id={runId}, ex={ex}");
            }
        }

        protected override void OnFormClosing(FormClosingEventArgs e)
        {
            base.OnFormClosing(e);
            _timer.Stop();
            _timer.Dispose();
        }
    }
}
