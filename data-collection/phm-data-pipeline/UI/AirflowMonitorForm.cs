// 파일: UI/AirflowMonitorForm.cs
using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using System.Windows.Forms;
using WeifenLuo.WinFormsUI.Docking;
using phm_data_pipeline.Services;

namespace phm_data_pipeline.UI
{
    public class AirflowMonitorForm : DockContent
    {
        // === Airflow 설정 (환경에 맞게 바꿔줘) ===========================
        // phm-airflow 컨테이너가 호스트 입장에서 http://localhost:8080 이라고 가정
        private const string AirflowBaseUrl = "http://localhost:8080";
        private const string AirflowDagId = "bronze_to_silver_and_features";

        // 기본 admin 계정 (airflow 공식 이미지 기본값)
        private const string AirflowUser = "admin";
        private const string AirflowPassword = "admin";

        // ================================================================
        // UI 컨트롤
        private GroupBox grpConfig;
        private ComboBox cboTaskType;      // anomaly / fault_diag
        private ComboBox cboModelType;     // rf (추가 예정)
        private NumericUpDown nudInterval; // 분 단위 주기
        private Button btnRunOnce;
        private Button btnStartAuto;
        private Button btnStopAuto;
        private Label lblNextRun;
        private Label lblStatus;           // 최근 상태 한 줄만 표시

        private readonly Timer _autoTimer;
        private readonly HttpClient _httpClient;
        private bool _autoRunning = false;

        public AirflowMonitorForm()
        {
            _httpClient = new HttpClient();
            _autoTimer = new Timer();
            _autoTimer.Tick += async (s, e) => await OnAutoTimerTickAsync();

            InitializeComponent();
        }

        private void InitializeComponent()
        {
            Text = "Airflow Monitor / Retrain Control";
            TabText = "Airflow Monitor";
            HideOnClose = true;
            ClientSize = new System.Drawing.Size(800, 300);

            var mainLayout = new TableLayoutPanel
            {
                Dock = DockStyle.Fill,
                ColumnCount = 1,
                RowCount = 2,
                Padding = new Padding(8)
            };
            mainLayout.RowStyles.Add(new RowStyle(SizeType.AutoSize));         // 상단 설정
            mainLayout.RowStyles.Add(new RowStyle(SizeType.AutoSize));         // 상태 표시

            InitializeConfigGroup();
            InitializeStatusArea();

            mainLayout.Controls.Add(grpConfig, 0, 0);
            mainLayout.Controls.Add(lblStatus, 0, 1);

            Controls.Add(mainLayout);

            // 초기 로그
            Log("[Init] Airflow Monitor 초기화 완료.");
            Log(" - UI에서 학습 설정 후 '한 번 학습 실행' 또는 '자동 재학습 시작'을 사용할 수 있습니다.");
        }

        // ------------------------------------------------------------------
        // 1) 상단: 학습 설정 + 재학습 주기
        // ------------------------------------------------------------------
        private void InitializeConfigGroup()
        {
            grpConfig = new GroupBox
            {
                Text = "모델 학습 / 재학습 설정 (Airflow DAG 제어)",
                Dock = DockStyle.Top,
                Padding = new Padding(10),
                AutoSize = true,
                AutoSizeMode = AutoSizeMode.GrowAndShrink
            };

            var tlp = new TableLayoutPanel
            {
                Dock = DockStyle.Fill,
                AutoSize = true,
                AutoSizeMode = AutoSizeMode.GrowAndShrink,
                ColumnCount = 4,
                RowCount = 3
            };

            tlp.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));          // 라벨
            tlp.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 150));     // 콤보/숫자
            tlp.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));          // 라벨
            tlp.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100F));     // 나머지

            // --- 1행: Task Type -----------------------------------------
            var lblTaskType = new Label { Text = "Task Type", AutoSize = true };
            cboTaskType = new ComboBox
            {
                DropDownStyle = ComboBoxStyle.DropDownList,
                Width = 140
            };
            // ETL / 세션에 저장되는 task_type 값과 일치
            // anomaly_detection : 이상 탐지 (normal / anomaly)
            // fault_diagnosis   : 결함 진단 (normal / faultA / faultB ...)
            cboTaskType.Items.AddRange(new object[] { "anomaly_detection", "fault_diagnosis" });
            cboTaskType.SelectedIndex = 0;

            // 오른쪽 칸은 간단한 설명용으로 사용 (선택 사항)
            var lblTaskHint = new Label
            {
                Text = "(anomaly_detection: 이상 탐지, fault_diagnosis: 결함 진단)",
                AutoSize = true,
                Anchor = AnchorStyles.Left
            };

            tlp.Controls.Add(lblTaskType, 0, 0);
            tlp.Controls.Add(cboTaskType, 1, 0);
            tlp.Controls.Add(lblTaskHint, 2, 0);
            tlp.SetColumnSpan(lblTaskHint, 2);

            // --- 2행: Model Type / 재학습 주기 ---------------------------
            var lblModelType = new Label { Text = "Model Type", AutoSize = true };
            cboModelType = new ComboBox
            {
                DropDownStyle = ComboBoxStyle.DropDownList,
                Width = 140
            };
            // 지금은 rf 만, 나중에 svm, xgb etc. 추가
            cboModelType.Items.AddRange(new object[] { "rf" });
            cboModelType.SelectedIndex = 0;

            var lblInterval = new Label { Text = "재학습 주기 (분)", AutoSize = true };
            nudInterval = new NumericUpDown
            {
                Minimum = 1,
                Maximum = 24 * 60,
                Value = 60,
                Width = 80
            };

            tlp.Controls.Add(lblModelType, 0, 1);
            tlp.Controls.Add(cboModelType, 1, 1);
            tlp.Controls.Add(lblInterval, 2, 1);
            tlp.Controls.Add(nudInterval, 3, 1);

            // --- 3행: 버튼들 / 다음 실행 시간 ----------------------------
            btnRunOnce = new Button
            {
                Text = "한 번 학습 실행",
                AutoSize = true
            };
            btnRunOnce.Click += async (s, e) => await OnRunOnceClickedAsync();

            btnStartAuto = new Button
            {
                Text = "자동 재학습 시작",
                AutoSize = true
            };
            btnStartAuto.Click += (s, e) => StartAutoRetrain();

            btnStopAuto = new Button
            {
                Text = "자동 재학습 중지",
                AutoSize = true,
                Enabled = false
            };
            btnStopAuto.Click += (s, e) => StopAutoRetrain();

            lblNextRun = new Label
            {
                Text = "다음 자동 재학습: (중지됨)",
                AutoSize = true,
                Anchor = AnchorStyles.Left
            };

            var buttonPanel = new FlowLayoutPanel
            {
                Dock = DockStyle.Fill,
                AutoSize = true,
                AutoSizeMode = AutoSizeMode.GrowAndShrink
            };
            buttonPanel.Controls.Add(btnRunOnce);
            buttonPanel.Controls.Add(btnStartAuto);
            buttonPanel.Controls.Add(btnStopAuto);

            tlp.Controls.Add(buttonPanel, 0, 2);
            tlp.SetColumnSpan(buttonPanel, 2);

            tlp.Controls.Add(lblNextRun, 2, 2);
            tlp.SetColumnSpan(lblNextRun, 2);

            grpConfig.Controls.Add(tlp);
        }


        // ------------------------------------------------------------------
        // 2) 상태 표시 영역 (최근 메시지 한 줄)
        // ------------------------------------------------------------------
        private void InitializeStatusArea()
        {
            lblStatus = new Label
            {
                Dock = DockStyle.Top,
                AutoSize = true,
                Padding = new Padding(2),
                Text = "상태: 초기화 중..."
            };
        }

        private void SetStatus(string message)
        {
            lblStatus.Text = $"상태: {message}";
        }

        private void Log(string message)
        {
            var line = $"[{DateTime.Now:HH:mm:ss}] {message}";
            // 상태 라벨 업데이트
            SetStatus(message);
            // 공용 LogWriterForm 으로 보내기
            AppEvents.RaiseLog("[AirflowMonitor] " + line);
        }

        // ------------------------------------------------------------------
        // 3) 버튼 동작
        // ------------------------------------------------------------------
        private async Task OnRunOnceClickedAsync()
        {
            await TriggerTrainingAsync(triggerReason: "manual_button");
        }

        private void StartAutoRetrain()
        {
            if (_autoRunning)
                return;

            var intervalMinutes = (double)nudInterval.Value;
            var intervalMs = (int)Math.Max(1000, intervalMinutes * 60_000);

            _autoTimer.Interval = intervalMs;
            _autoTimer.Start();
            _autoRunning = true;

            var nextLocal = DateTime.Now.AddMinutes(intervalMinutes);
            lblNextRun.Text = $"다음 자동 재학습: {nextLocal:yyyy-MM-dd HH:mm:ss} (로컬 시간 기준)";
            btnStartAuto.Enabled = false;
            btnStopAuto.Enabled = true;

            Log($"자동 재학습 시작 (주기: {intervalMinutes}분).");
            // 시작할 때 바로 한 번 돌리고 싶으면 여기에:
            // _ = TriggerTrainingAsync("auto_immediate");
        }

        private void StopAutoRetrain()
        {
            if (!_autoRunning)
                return;

            _autoTimer.Stop();
            _autoRunning = false;

            lblNextRun.Text = "다음 자동 재학습: (중지됨)";
            btnStartAuto.Enabled = true;
            btnStopAuto.Enabled = false;

            Log("자동 재학습 중지.");
        }

        private async Task OnAutoTimerTickAsync()
        {
            // 타이머 중복 실행 방지
            _autoTimer.Stop();
            try
            {
                await TriggerTrainingAsync(triggerReason: "auto_timer");
            }
            finally
            {
                if (_autoRunning)
                {
                    var intervalMinutes = (double)nudInterval.Value;
                    var nextLocal = DateTime.Now.AddMinutes(intervalMinutes);
                    lblNextRun.Text = $"다음 자동 재학습: {nextLocal:yyyy-MM-dd HH:mm:ss} (로컬 시간 기준)";

                    var intervalMs = (int)Math.Max(1000, intervalMinutes * 60_000);
                    _autoTimer.Interval = intervalMs;
                    _autoTimer.Start();
                }
            }
        }

        // ------------------------------------------------------------------
        // 4) Airflow DAG Run 트리거
        // ------------------------------------------------------------------
        private async Task TriggerTrainingAsync(string triggerReason)
        {
            // UI에서 선택한 값 가져오기
            var taskType = cboTaskType.SelectedItem?.ToString() ?? "anomaly_detection";
            var modelType = cboModelType.SelectedItem?.ToString() ?? "rf";

            Log($"DAG 실행 요청: task_type={taskType}, model_type={modelType}, reason={triggerReason}");

            try
            {
                var dagRunId = $"phm_ui_{taskType}_{DateTime.UtcNow:yyyyMMdd_HHmmss}";

                var url = $"{AirflowBaseUrl.TrimEnd('/')}/api/v1/dags/{AirflowDagId}/dagRuns";

                // conf 객체 구성 (dag_run.conf 로 넘어감)
                var bodyObj = new
                {
                    dag_run_id = dagRunId,
                    conf = new
                    {
                        task_type = taskType,
                        model_type = modelType
                    }
                };

                var json = JsonSerializer.Serialize(
                    bodyObj,
                    new JsonSerializerOptions
                    {
                        PropertyNamingPolicy = null, // 우리가 준 이름(dag_run_id 등) 그대로 사용
                        WriteIndented = false
                    });

                var req = new HttpRequestMessage(HttpMethod.Post, url)
                {
                    Content = new StringContent(json, Encoding.UTF8, "application/json")
                };

                var authBytes = Encoding.ASCII.GetBytes($"{AirflowUser}:{AirflowPassword}");
                req.Headers.Authorization = new AuthenticationHeaderValue(
                    "Basic",
                    Convert.ToBase64String(authBytes)
                );

                var resp = await _httpClient.SendAsync(req);
                var respText = await resp.Content.ReadAsStringAsync();

                if (!resp.IsSuccessStatusCode)
                {
                    Log($"[ERROR] DAG 실행 실패 (HTTP {(int)resp.StatusCode}): {respText}");
                    return;
                }

                Log($"DAG 실행 성공: dag_run_id={dagRunId}");
                // 필요하면 respText 파싱해서 state, execution_date 등 뽑을 수 있음
            }
            catch (Exception ex)
            {
                Log("[ERROR] DAG 실행 중 예외: " + ex.Message);
            }
        }
    }
}
