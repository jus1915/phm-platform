// 파일: UI/SessionManagerForm.cs
using System;
using System.ComponentModel;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using Npgsql;
using phm_data_pipeline.Services;
using WeifenLuo.WinFormsUI.Docking;

namespace phm_data_pipeline.UI
{
    public class SessionManagerForm : DockContent
    {
        // 환경에 맞게 수정 (Windows에서 접속 가능한 주소)
        private const string PgConnectionString =
            "Host=localhost;Port=15432;Username=phm_user;Password=phm-password;Database=phm";

        // -------------------------------
        // 상단: 필터 / 메타 컨트롤
        // -------------------------------
        private GroupBox grpMeta;
        private ComboBox cboTaskType;
        private ComboBox cboLabelType;
        private ComboBox cboSplit;
        private TextBox txtOperator;

        // 중앙: 요약 텍스트
        private TextBox txtSummary;

        // 하단: 세션 CRUD
        private SplitContainer splitSession;
        private DataGridView dgvSession;
        private BindingSource _sessionBinding = new BindingSource();

        // 상세 편집
        private TextBox txtSessionId;
        private TextBox txtDeviceId;
        private TextBox txtRawFilePath;
        private TextBox txtSampleRate;
        private TextBox txtChannelCount;
        private ComboBox cboTaskTypeDetail;
        private ComboBox cboLabelTypeDetail;
        private ComboBox cboSplitDetail;
        private TextBox txtOperatorDetail;
        private TextBox txtCreatedAt;

        private Button btnNewSession;
        private Button btnSaveSession;
        private Button btnDeleteSession;
        private Button btnRefresh;         // 🔹 새로고침 버튼
        private Button btnDeleteAll;       // 🔹 전체 초기화 버튼

        private bool _isLoading = false;

        // 세션 DTO
        private class SessionDto
        {
            public long Id { get; set; }
            public string DeviceId { get; set; }
            public string RawFilePath { get; set; }
            public double? SampleRateHz { get; set; }
            public int? ChannelCount { get; set; }
            public string TaskType { get; set; }
            public string LabelType { get; set; }
            public string DataSplit { get; set; }
            public string Operator { get; set; }
            public DateTime CreatedAt { get; set; }
        }

        public SessionManagerForm()
        {
            InitializeComponent();
        }

        private void InitializeComponent()
        {
            Text = "Session / Data Manager";
            TabText = "Session Manager";
            HideOnClose = true;
            ClientSize = new System.Drawing.Size(900, 600);

            var mainLayout = new TableLayoutPanel
            {
                Dock = DockStyle.Fill,
                ColumnCount = 1,
                RowCount = 3,
                Padding = new Padding(8)
            };
            mainLayout.RowStyles.Add(new RowStyle(SizeType.AutoSize));          // 메타 영역
            mainLayout.RowStyles.Add(new RowStyle(SizeType.Percent, 50f));      // Summary 영역
            mainLayout.RowStyles.Add(new RowStyle(SizeType.Percent, 50f));      // 세션 CRUD 영역

            InitializeMetaGroup();
            InitializeSummaryArea();
            InitializeSessionCrudArea();

            mainLayout.Controls.Add(grpMeta, 0, 0);
            mainLayout.Controls.Add(txtSummary, 0, 1);
            mainLayout.Controls.Add(splitSession, 0, 2);

            Controls.Add(mainLayout);

            Load += async (s, e) => await OnFormLoadedAsync();
        }

        // ------------------------------------------------------------------
        // 1) 메타데이터 필터 UI
        // ------------------------------------------------------------------
        private void InitializeMetaGroup()
        {
            grpMeta = new GroupBox
            {
                Text = "학습 데이터 필터 (작업 메타데이터)",
                Dock = DockStyle.Top,
                Padding = new Padding(10),
                AutoSize = true,
                AutoSizeMode = AutoSizeMode.GrowAndShrink
            };

            var tlpMeta = new TableLayoutPanel
            {
                Dock = DockStyle.Fill,
                AutoSize = true,
                AutoSizeMode = AutoSizeMode.GrowAndShrink,
                ColumnCount = 3,  // 🔹 오른쪽에 버튼 영역 추가
                RowCount = 4
            };

            tlpMeta.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));
            tlpMeta.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100F));
            tlpMeta.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));   // 버튼용

            // Task Type
            var lblTaskType = new Label
            {
                Text = "Task Type",
                AutoSize = true
            };

            cboTaskType = new ComboBox
            {
                DropDownStyle = ComboBoxStyle.DropDownList,
                Width = 140
            };
            cboTaskType.Items.AddRange(new object[] { "all", "anomaly_detection", "fault_diagnosis" });
            cboTaskType.SelectedIndex = 0;
            cboTaskType.SelectedIndexChanged += async (s, e) => await OnFilterChangedAsync();

            tlpMeta.Controls.Add(lblTaskType, 0, 0);
            tlpMeta.Controls.Add(cboTaskType, 1, 0);

            // Label Type
            var lblLabelType = new Label
            {
                Text = "Label Type",
                AutoSize = true
            };

            cboLabelType = new ComboBox
            {
                DropDownStyle = ComboBoxStyle.DropDownList,
                Width = 140
            };
            cboLabelType.Items.AddRange(new object[] { "all", "normal", "anomaly", "fault_A", "fault_B" });
            cboLabelType.SelectedIndex = 0;
            cboLabelType.SelectedIndexChanged += async (s, e) => await OnFilterChangedAsync();

            tlpMeta.Controls.Add(lblLabelType, 0, 1);
            tlpMeta.Controls.Add(cboLabelType, 1, 1);

            // Data Split
            var lblSplit = new Label
            {
                Text = "Data Split",
                AutoSize = true
            };

            cboSplit = new ComboBox
            {
                DropDownStyle = ComboBoxStyle.DropDownList,
                Width = 140
            };
            cboSplit.Items.AddRange(new object[] { "all", "train", "val", "test" });
            cboSplit.SelectedIndex = 0;
            cboSplit.SelectedIndexChanged += async (s, e) => await OnFilterChangedAsync();

            tlpMeta.Controls.Add(lblSplit, 0, 2);
            tlpMeta.Controls.Add(cboSplit, 1, 2);

            // Operator
            var lblOperator = new Label
            {
                Text = "Operator",
                AutoSize = true
            };

            txtOperator = new TextBox
            {
                Text = "",
                Width = 140
            };
            txtOperator.TextChanged += async (s, e) => await OnFilterChangedAsync();

            tlpMeta.Controls.Add(lblOperator, 0, 3);
            tlpMeta.Controls.Add(txtOperator, 1, 3);

            // 🔹 오른쪽 상단에 "새로고침" 버튼 추가
            btnRefresh = new Button
            {
                Text = "새로고침",
                AutoSize = true,
                Anchor = AnchorStyles.Right
            };
            btnRefresh.Click += async (s, e) => await RefreshSummaryAsync();

            tlpMeta.Controls.Add(btnRefresh, 2, 0);
            tlpMeta.SetRowSpan(btnRefresh, 4); // 세로로 가운데쯤 보이도록

            grpMeta.Controls.Add(tlpMeta);
        }

        // ------------------------------------------------------------------
        // 2) Summary 출력 영역
        // ------------------------------------------------------------------
        private void InitializeSummaryArea()
        {
            txtSummary = new TextBox
            {
                Dock = DockStyle.Fill,
                Multiline = true,
                ScrollBars = ScrollBars.Both,
                ReadOnly = true,
                Font = new System.Drawing.Font("Consolas", 9f),
                WordWrap = false
            };

            txtSummary.Text = "필터를 선택하면 raw / mart / session 테이블의 통계를 조회합니다.";
        }

        // ------------------------------------------------------------------
        // 3) 세션 CRUD 영역
        // ------------------------------------------------------------------
        private void InitializeSessionCrudArea()
        {
            splitSession = new SplitContainer
            {
                Dock = DockStyle.Fill,
                Orientation = Orientation.Vertical,
                SplitterDistance = 450
            };

            // 왼쪽: 세션 리스트
            dgvSession = new DataGridView
            {
                Dock = DockStyle.Fill,
                AutoGenerateColumns = true,
                ReadOnly = true,
                AllowUserToAddRows = false,
                SelectionMode = DataGridViewSelectionMode.FullRowSelect,
                MultiSelect = false
            };
            dgvSession.DataSource = _sessionBinding;
            dgvSession.SelectionChanged += DgvSession_SelectionChanged;

            var grpList = new GroupBox
            {
                Text = "Session 목록 (session 테이블)",
                Dock = DockStyle.Fill
            };
            grpList.Controls.Add(dgvSession);
            splitSession.Panel1.Controls.Add(grpList);

            // 오른쪽: 상세 + 버튼
            var grpDetail = new GroupBox
            {
                Text = "Session 상세 / 편집",
                Dock = DockStyle.Fill
            };

            var tlpDetail = new TableLayoutPanel
            {
                Dock = DockStyle.Fill,
                ColumnCount = 2,
                RowCount = 11,
                AutoSize = true
            };
            tlpDetail.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));
            tlpDetail.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100f));

            int r = 0;

            void AddRow(string labelText, Control ctl)
            {
                var lbl = new Label { Text = labelText, AutoSize = true, Anchor = AnchorStyles.Left };
                ctl.Anchor = AnchorStyles.Left | AnchorStyles.Right;
                tlpDetail.RowStyles.Add(new RowStyle(SizeType.AutoSize));
                tlpDetail.Controls.Add(lbl, 0, r);
                tlpDetail.Controls.Add(ctl, 1, r);
                r++;
            }

            txtSessionId = new TextBox { ReadOnly = true };
            AddRow("Id", txtSessionId);

            txtDeviceId = new TextBox();
            AddRow("Device Id", txtDeviceId);

            txtRawFilePath = new TextBox();
            AddRow("Raw File Path", txtRawFilePath);

            txtSampleRate = new TextBox();
            AddRow("Sample Rate Hz", txtSampleRate);

            txtChannelCount = new TextBox();
            AddRow("Channel Count", txtChannelCount);

            cboTaskTypeDetail = new ComboBox { DropDownStyle = ComboBoxStyle.DropDownList };
            cboTaskTypeDetail.Items.AddRange(new object[] { "", "anomaly_detection", "fault_diagnosis" });
            AddRow("Task Type", cboTaskTypeDetail);

            cboLabelTypeDetail = new ComboBox { DropDownStyle = ComboBoxStyle.DropDownList };
            cboLabelTypeDetail.Items.AddRange(new object[] { "", "normal", "anomaly", "fault_A", "fault_B" });
            AddRow("Label Type", cboLabelTypeDetail);

            cboSplitDetail = new ComboBox { DropDownStyle = ComboBoxStyle.DropDownList };
            cboSplitDetail.Items.AddRange(new object[] { "", "train", "val", "test" });
            AddRow("Data Split", cboSplitDetail);

            txtOperatorDetail = new TextBox();
            AddRow("Operator", txtOperatorDetail);

            txtCreatedAt = new TextBox { ReadOnly = true };
            AddRow("Created At", txtCreatedAt);

            // 버튼 영역 (오른쪽 아래)
            var pnlButtons = new FlowLayoutPanel
            {
                Dock = DockStyle.Bottom,
                FlowDirection = FlowDirection.RightToLeft,
                AutoSize = true
            };

            btnDeleteAll = new Button { Text = "모든 Session 초기화 (DB+MinIO)", Width = 210 };
            btnDeleteSession = new Button { Text = "선택 삭제 (DB+파일)", Width = 150 };
            btnSaveSession = new Button { Text = "저장 (INSERT/UPDATE)", Width = 150 };
            btnNewSession = new Button { Text = "새 세션", Width = 90 };

            btnDeleteAll.Click += async (s, e) => await OnDeleteAllSessionsAsync();
            btnDeleteSession.Click += async (s, e) => await OnDeleteSessionAsync();
            btnSaveSession.Click += async (s, e) => await OnSaveSessionAsync();
            btnNewSession.Click += (s, e) => ClearSessionDetailForNew();

            pnlButtons.Controls.Add(btnDeleteAll);
            pnlButtons.Controls.Add(btnDeleteSession);
            pnlButtons.Controls.Add(btnSaveSession);
            pnlButtons.Controls.Add(btnNewSession);

            var panelRight = new Panel { Dock = DockStyle.Fill };
            panelRight.Controls.Add(tlpDetail);
            panelRight.Controls.Add(pnlButtons);

            grpDetail.Controls.Add(panelRight);
            splitSession.Panel2.Controls.Add(grpDetail);
        }

        // ------------------------------------------------------------------
        // 4) 폼 로드 시 초기 조회
        // ------------------------------------------------------------------
        private async Task OnFormLoadedAsync()
        {
            await RefreshSummaryAsync();
        }

        // 필터 변경 → 재조회
        private async Task OnFilterChangedAsync()
        {
            if (_isLoading) return;
            await RefreshSummaryAsync();
        }

        // ------------------------------------------------------------------
        // 5) DB 조회 로직 (raw + mart + session 목록)
        // ------------------------------------------------------------------
        private async Task RefreshSummaryAsync()
        {
            _isLoading = true;
            try
            {
                var taskType = cboTaskType.SelectedItem?.ToString() ?? "all";
                var labelType = cboLabelType.SelectedItem?.ToString() ?? "all";
                var split = cboSplit.SelectedItem?.ToString() ?? "all";
                var oper = txtOperator.Text?.Trim();

                txtSummary.Text = "[조회 중...]\r\n";

                var sb = new StringBuilder();

                sb.AppendLine("[필터]");
                sb.AppendLine($"  TaskType  = {taskType}");
                sb.AppendLine($"  LabelType = {labelType}");
                sb.AppendLine($"  DataSplit = {split}");
                sb.AppendLine($"  Operator  = {oper}");
                sb.AppendLine();

                using (var conn = new NpgsqlConnection(PgConnectionString))
                {
                    await conn.OpenAsync();

                    // raw.vibration_frame
                    sb.AppendLine("[DB 요약 - raw.vibration_frame]");
                    sb.AppendLine();

                    var sqlCountRaw = new StringBuilder(@"
                        SELECT COUNT(*) 
                        FROM raw.vibration_frame
                        WHERE 1=1
                    ");

                    if (!IsAll(taskType))
                        sqlCountRaw.Append(" AND task_type = @task_type");
                    if (!IsAll(labelType))
                        sqlCountRaw.Append(" AND label_type = @label_type");
                    if (!IsAll(split))
                        sqlCountRaw.Append(" AND data_split = @split");
                    if (!string.IsNullOrEmpty(oper))
                        sqlCountRaw.Append(" AND operator = @operator");

                    using (var cmd = new NpgsqlCommand(sqlCountRaw.ToString(), conn))
                    {
                        if (sqlCountRaw.ToString().Contains("@task_type"))
                            cmd.Parameters.AddWithValue("task_type", taskType);
                        if (sqlCountRaw.ToString().Contains("@label_type"))
                            cmd.Parameters.AddWithValue("label_type", labelType);
                        if (sqlCountRaw.ToString().Contains("@split"))
                            cmd.Parameters.AddWithValue("split", split);
                        if (sqlCountRaw.ToString().Contains("@operator"))
                            cmd.Parameters.AddWithValue("operator", oper ?? (object)DBNull.Value);

                        var totalCountRaw = (long)await cmd.ExecuteScalarAsync();
                        sb.AppendLine($"  - 선택 조건에 해당하는 frame 수: {totalCountRaw:N0} rows");
                    }

                    sb.AppendLine();

                    var sqlGroupRaw = new StringBuilder(@"
                        SELECT task_type, label_type, data_split, COUNT(*) AS cnt
                        FROM raw.vibration_frame
                        WHERE 1=1
                    ");

                    if (!IsAll(taskType))
                        sqlGroupRaw.Append(" AND task_type = @task_type");
                    if (!IsAll(labelType))
                        sqlGroupRaw.Append(" AND label_type = @label_type");
                    if (!IsAll(split))
                        sqlGroupRaw.Append(" AND data_split = @split");
                    if (!string.IsNullOrEmpty(oper))
                        sqlGroupRaw.Append(" AND operator = @operator");

                    sqlGroupRaw.Append(@"
                        GROUP BY task_type, label_type, data_split
                        ORDER BY task_type, label_type, data_split
                    ");

                    using (var cmd = new NpgsqlCommand(sqlGroupRaw.ToString(), conn))
                    {
                        if (sqlGroupRaw.ToString().Contains("@task_type"))
                            cmd.Parameters.AddWithValue("task_type", taskType);
                        if (sqlGroupRaw.ToString().Contains("@label_type"))
                            cmd.Parameters.AddWithValue("label_type", labelType);
                        if (sqlGroupRaw.ToString().Contains("@split"))
                            cmd.Parameters.AddWithValue("split", split);
                        if (sqlGroupRaw.ToString().Contains("@operator"))
                            cmd.Parameters.AddWithValue("operator", oper ?? (object)DBNull.Value);

                        using (var reader = await cmd.ExecuteReaderAsync())
                        {
                            if (!reader.HasRows)
                            {
                                sb.AppendLine("  - 그룹별 통계: (행 없음)");
                            }
                            else
                            {
                                sb.AppendLine("  - 그룹별 통계 (task_type, label_type, data_split, count):");
                                while (await reader.ReadAsync())
                                {
                                    var tt = reader["task_type"] as string ?? "(null)";
                                    var lt = reader["label_type"] as string ?? "(null)";
                                    var ds = reader["data_split"] as string ?? "(null)";
                                    var cnt = (long)reader["cnt"];
                                    sb.AppendLine($"      {tt,-18} {lt,-10} {ds,-8} {cnt,10:N0}");
                                }
                            }
                        }
                    }

                    sb.AppendLine();
                    sb.AppendLine("  ※ raw.vibration_frame 는 프레임 당 1 row 입니다.");
                    sb.AppendLine();

                    // mart.vibration_frame_features
                    sb.AppendLine("[DB 요약 - mart.vibration_frame_features]");
                    sb.AppendLine();

                    var sqlCountMart = new StringBuilder(@"
                        SELECT COUNT(*) 
                        FROM mart.vibration_frame_features
                        WHERE 1=1
                    ");

                    if (!IsAll(taskType))
                        sqlCountMart.Append(" AND task_type = @task_type");
                    if (!IsAll(labelType))
                        sqlCountMart.Append(" AND label_type = @label_type");
                    if (!IsAll(split))
                        sqlCountMart.Append(" AND data_split = @split");
                    if (!string.IsNullOrEmpty(oper))
                        sqlCountMart.Append(" AND operator = @operator");

                    using (var cmd = new NpgsqlCommand(sqlCountMart.ToString(), conn))
                    {
                        if (sqlCountMart.ToString().Contains("@task_type"))
                            cmd.Parameters.AddWithValue("task_type", taskType);
                        if (sqlCountMart.ToString().Contains("@label_type"))
                            cmd.Parameters.AddWithValue("label_type", labelType);
                        if (sqlCountMart.ToString().Contains("@split"))
                            cmd.Parameters.AddWithValue("split", split);
                        if (sqlCountMart.ToString().Contains("@operator"))
                            cmd.Parameters.AddWithValue("operator", oper ?? (object)DBNull.Value);

                        var totalCountMart = (long)await cmd.ExecuteScalarAsync();
                        sb.AppendLine($"  - 선택 조건에 해당하는 feature row 수: {totalCountMart:N0} rows");
                    }

                    sb.AppendLine();

                    var sqlGroupMart = new StringBuilder(@"
                        SELECT task_type, label_type, data_split, COUNT(*) AS cnt
                        FROM mart.vibration_frame_features
                        WHERE 1=1
                    ");

                    if (!IsAll(taskType))
                        sqlGroupMart.Append(" AND task_type = @task_type");
                    if (!IsAll(labelType))
                        sqlGroupMart.Append(" AND label_type = @label_type");
                    if (!IsAll(split))
                        sqlGroupMart.Append(" AND data_split = @split");
                    if (!string.IsNullOrEmpty(oper))
                        sqlGroupMart.Append(" AND operator = @operator");

                    sqlGroupMart.Append(@"
                        GROUP BY task_type, label_type, data_split
                        ORDER BY task_type, label_type, data_split
                    ");

                    using (var cmd = new NpgsqlCommand(sqlGroupMart.ToString(), conn))
                    {
                        if (sqlGroupMart.ToString().Contains("@task_type"))
                            cmd.Parameters.AddWithValue("task_type", taskType);
                        if (sqlGroupMart.ToString().Contains("@label_type"))
                            cmd.Parameters.AddWithValue("label_type", labelType);
                        if (sqlGroupMart.ToString().Contains("@split"))
                            cmd.Parameters.AddWithValue("split", split);
                        if (sqlGroupMart.ToString().Contains("@operator"))
                            cmd.Parameters.AddWithValue("operator", oper ?? (object)DBNull.Value);

                        using (var reader = await cmd.ExecuteReaderAsync())
                        {
                            if (!reader.HasRows)
                            {
                                sb.AppendLine("  - 그룹별 통계: (행 없음)");
                            }
                            else
                            {
                                sb.AppendLine("  - 그룹별 통계 (task_type, label_type, data_split, count):");
                                while (await reader.ReadAsync())
                                {
                                    var tt = reader["task_type"] as string ?? "(null)";
                                    var lt = reader["label_type"] as string ?? "(null)";
                                    var ds = reader["data_split"] as string ?? "(null)";
                                    var cnt = (long)reader["cnt"];
                                    sb.AppendLine($"      {tt,-18} {lt,-10} {ds,-8} {cnt,10:N0}");
                                }
                            }
                        }
                    }

                    sb.AppendLine();
                    sb.AppendLine("  ※ mart.vibration_frame_features 는 축(axis)별 feature row 입니다.");
                    sb.AppendLine("     대략 frame 수 ≒ feature row 수 / 3 (x,y,z) 로 추정할 수 있습니다.");
                    sb.AppendLine();

                    // session 목록
                    await LoadSessionListAsync(conn, taskType, labelType, split, oper);
                }

                txtSummary.Text = sb.ToString();

                AppEvents.RaiseLog("[SessionManager] summary + session list refreshed.");
            }
            catch (Exception ex)
            {
                txtSummary.Text = "DB 조회 중 에러:\r\n" + ex.Message;
                AppEvents.RaiseLog("[SessionManager][ERROR] " + ex);
            }
            finally
            {
                _isLoading = false;
            }
        }

        private static bool IsAll(string value)
        {
            return string.IsNullOrEmpty(value) || value.Equals("all", StringComparison.OrdinalIgnoreCase);
        }

        // ------------------------------------------------------------------
        // 6) session 목록 로딩
        // ------------------------------------------------------------------
        private async Task LoadSessionListAsync(
            NpgsqlConnection conn,
            string taskType,
            string labelType,
            string split,
            string oper)
        {
            var sql = new StringBuilder(@"
                SELECT id, device_id, raw_file_path, sample_rate_hz, channel_count,
                       task_type, label_type, data_split, operator, created_at
                FROM session
                WHERE 1=1
            ");

            if (!IsAll(taskType))
                sql.Append(" AND task_type = @task_type");
            if (!IsAll(labelType))
                sql.Append(" AND label_type = @label_type");
            if (!IsAll(split))
                sql.Append(" AND data_split = @split");
            if (!string.IsNullOrEmpty(oper))
                sql.Append(" AND operator = @operator");

            sql.Append(" ORDER BY id DESC");

            using (var cmd = new NpgsqlCommand(sql.ToString(), conn))
            {
                if (sql.ToString().Contains("@task_type"))
                    cmd.Parameters.AddWithValue("task_type", taskType);
                if (sql.ToString().Contains("@label_type"))
                    cmd.Parameters.AddWithValue("label_type", labelType);
                if (sql.ToString().Contains("@split"))
                    cmd.Parameters.AddWithValue("split", split);
                if (sql.ToString().Contains("@operator"))
                    cmd.Parameters.AddWithValue("operator", oper ?? (object)DBNull.Value);

                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    var list = new BindingList<SessionDto>();

                    while (await reader.ReadAsync())
                    {
                        var dto = new SessionDto
                        {
                            Id = reader.GetInt64(reader.GetOrdinal("id")),
                            DeviceId = reader["device_id"] as string,
                            RawFilePath = reader["raw_file_path"] as string,
                            TaskType = reader["task_type"] as string,
                            LabelType = reader["label_type"] as string,
                            DataSplit = reader["data_split"] as string,
                            Operator = reader["operator"] as string,
                            CreatedAt = reader.GetDateTime(reader.GetOrdinal("created_at")),
                            SampleRateHz = reader.IsDBNull(reader.GetOrdinal("sample_rate_hz"))
                                ? (double?)null
                                : reader.GetDouble(reader.GetOrdinal("sample_rate_hz")),
                            ChannelCount = reader.IsDBNull(reader.GetOrdinal("channel_count"))
                                ? (int?)null
                                : reader.GetInt32(reader.GetOrdinal("channel_count"))
                        };

                        list.Add(dto);
                    }

                    _sessionBinding.DataSource = list;
                }
            }
        }

        // ------------------------------------------------------------------
        // 7) 그리드 선택 → 상세 편집 로딩
        // ------------------------------------------------------------------
        private void DgvSession_SelectionChanged(object sender, EventArgs e)
        {
            if (_sessionBinding.Current is SessionDto dto)
            {
                LoadSessionToDetail(dto);
            }
        }

        private void LoadSessionToDetail(SessionDto dto)
        {
            txtSessionId.Text = dto.Id.ToString();
            txtDeviceId.Text = dto.DeviceId;
            txtRawFilePath.Text = dto.RawFilePath;
            txtSampleRate.Text = dto.SampleRateHz?.ToString() ?? "";
            txtChannelCount.Text = dto.ChannelCount?.ToString() ?? "";
            cboTaskTypeDetail.Text = dto.TaskType ?? "";
            cboLabelTypeDetail.Text = dto.LabelType ?? "";
            cboSplitDetail.Text = dto.DataSplit ?? "";
            txtOperatorDetail.Text = dto.Operator ?? "";
            txtCreatedAt.Text = dto.CreatedAt.ToString("yyyy-MM-dd HH:mm:ss");
        }

        private void ClearSessionDetailForNew()
        {
            txtSessionId.Text = "(new)";
            txtDeviceId.Text = "";
            txtRawFilePath.Text = "";
            txtSampleRate.Text = "";
            txtChannelCount.Text = "";
            cboTaskTypeDetail.SelectedIndex = -1;
            cboLabelTypeDetail.SelectedIndex = -1;
            cboSplitDetail.SelectedIndex = -1;
            txtOperatorDetail.Text = "";
            txtCreatedAt.Text = "";
        }

        // ------------------------------------------------------------------
        // 8) 저장 (INSERT/UPDATE)
        // ------------------------------------------------------------------
        private async Task OnSaveSessionAsync()
        {
            try
            {
                using (var conn = new NpgsqlConnection(PgConnectionString))
                {
                    await conn.OpenAsync();

                    bool isNew = txtSessionId.Text == "(new)" ||
                                 string.IsNullOrWhiteSpace(txtSessionId.Text);

                    if (isNew)
                    {
                        const string sqlInsert = @"
                            INSERT INTO session
                            (device_id, raw_file_path, sample_rate_hz, channel_count,
                             task_type, label_type, data_split, operator)
                            VALUES
                            (@device_id, @raw_file_path, @sample_rate_hz, @channel_count,
                             @task_type, @label_type, @data_split, @operator)
                            RETURNING id, created_at;
                        ";

                        using (var cmd = new NpgsqlCommand(sqlInsert, conn))
                        {
                            AddSessionParameters(cmd);

                            using (var reader = await cmd.ExecuteReaderAsync())
                            {
                                if (await reader.ReadAsync())
                                {
                                    txtSessionId.Text = reader.GetInt64(0).ToString();
                                    txtCreatedAt.Text = reader.GetDateTime(1)
                                        .ToString("yyyy-MM-dd HH:mm:ss");
                                }
                            }
                        }
                    }
                    else
                    {
                        const string sqlUpdate = @"
                            UPDATE session SET
                                device_id = @device_id,
                                raw_file_path = @raw_file_path,
                                sample_rate_hz = @sample_rate_hz,
                                channel_count = @channel_count,
                                task_type = @task_type,
                                label_type = @label_type,
                                data_split = @data_split,
                                operator = @operator
                            WHERE id = @id;
                        ";

                        using (var cmd = new NpgsqlCommand(sqlUpdate, conn))
                        {
                            cmd.Parameters.AddWithValue("id", long.Parse(txtSessionId.Text));
                            AddSessionParameters(cmd);
                            await cmd.ExecuteNonQueryAsync();
                        }
                    }
                }

                await RefreshSummaryAsync();
                MessageBox.Show("저장 완료", "Session",
                    MessageBoxButtons.OK, MessageBoxIcon.Information);
            }
            catch (Exception ex)
            {
                MessageBox.Show("저장 중 오류: " + ex.Message, "Error",
                    MessageBoxButtons.OK, MessageBoxIcon.Error);
                AppEvents.RaiseLog("[SessionManager][SaveSession][ERROR] " + ex);
            }
        }

        private void AddSessionParameters(NpgsqlCommand cmd)
        {
            cmd.Parameters.AddWithValue("device_id",
                string.IsNullOrWhiteSpace(txtDeviceId.Text)
                    ? (object)DBNull.Value
                    : txtDeviceId.Text);

            cmd.Parameters.AddWithValue("raw_file_path",
                string.IsNullOrWhiteSpace(txtRawFilePath.Text)
                    ? (object)DBNull.Value
                    : txtRawFilePath.Text);

            if (double.TryParse(txtSampleRate.Text, out var sr))
                cmd.Parameters.AddWithValue("sample_rate_hz", sr);
            else
                cmd.Parameters.AddWithValue("sample_rate_hz", DBNull.Value);

            if (int.TryParse(txtChannelCount.Text, out var ch))
                cmd.Parameters.AddWithValue("channel_count", ch);
            else
                cmd.Parameters.AddWithValue("channel_count", DBNull.Value);

            cmd.Parameters.AddWithValue("task_type",
                string.IsNullOrWhiteSpace(cboTaskTypeDetail.Text)
                    ? (object)DBNull.Value
                    : cboTaskTypeDetail.Text);

            cmd.Parameters.AddWithValue("label_type",
                string.IsNullOrWhiteSpace(cboLabelTypeDetail.Text)
                    ? (object)DBNull.Value
                    : cboLabelTypeDetail.Text);

            cmd.Parameters.AddWithValue("data_split",
                string.IsNullOrWhiteSpace(cboSplitDetail.Text)
                    ? (object)DBNull.Value
                    : cboSplitDetail.Text);

            cmd.Parameters.AddWithValue("operator",
                string.IsNullOrWhiteSpace(txtOperatorDetail.Text)
                    ? (object)DBNull.Value
                    : txtOperatorDetail.Text);
        }

        // ------------------------------------------------------------------
        // 9) 개별 삭제 (DB + MinIO)
        // ------------------------------------------------------------------
        private async Task OnDeleteSessionAsync()
        {
            if (!(_sessionBinding.Current is SessionDto dto))
            {
                MessageBox.Show("삭제할 Session을 선택해 주세요.", "Session",
                    MessageBoxButtons.OK, MessageBoxIcon.Warning);
                return;
            }

            var confirm = MessageBox.Show(
                $"선택한 세션(ID={dto.Id})을 삭제하시겠습니까?\r\n" +
                $"관련 raw jsonl 파일도 MinIO에서 같이 삭제됩니다.\r\n\r\n" +
                $"raw_file_path: {dto.RawFilePath}",
                "세션 삭제",
                MessageBoxButtons.YesNo,
                MessageBoxIcon.Warning);

            if (confirm != DialogResult.Yes)
                return;

            // MinIO 삭제
            try
            {
                int deletedCount =
                    await MinioRawFileHelper.DeleteSessionFilesByRawPathAsync(dto.RawFilePath);

                AppEvents.RaiseLog(
                    $"[SessionManager] MinIO raw 삭제 완료. raw_file_path={dto.RawFilePath}, deleted={deletedCount}");
            }
            catch (Exception exFile)
            {
                MessageBox.Show(
                    "MinIO에서 raw 파일 삭제 중 오류가 발생했습니다.\r\n" +
                    exFile.Message + "\r\n\r\n" +
                    "그래도 DB 기록(session)은 삭제합니다.",
                    "파일 삭제 오류",
                    MessageBoxButtons.OK,
                    MessageBoxIcon.Warning);

                AppEvents.RaiseLog("[SessionManager][DeleteRawFromMinIO][ERROR] " + exFile);
            }

            // DB 삭제
            try
            {
                using (var conn = new NpgsqlConnection(PgConnectionString))
                {
                    await conn.OpenAsync();
                    using (var cmd = new NpgsqlCommand("DELETE FROM session WHERE id = @id", conn))
                    {
                        cmd.Parameters.AddWithValue("id", dto.Id);
                        await cmd.ExecuteNonQueryAsync();
                    }
                }

                await RefreshSummaryAsync();
                MessageBox.Show("세션 삭제 완료", "Session",
                    MessageBoxButtons.OK, MessageBoxIcon.Information);
            }
            catch (Exception ex)
            {
                MessageBox.Show("DB 삭제 중 오류: " + ex.Message, "Error",
                    MessageBoxButtons.OK, MessageBoxIcon.Error);
                AppEvents.RaiseLog("[SessionManager][DeleteSession][ERROR] " + ex);
            }
        }

        // ------------------------------------------------------------------
        // 10) 전체 초기화 (모든 Session + MinIO 파일 삭제)
        // ------------------------------------------------------------------
        private async Task OnDeleteAllSessionsAsync()
        {
            var confirm = MessageBox.Show(
                "모든 Session 레코드와 관련 MinIO raw 파일을 전부 삭제합니다.\r\n" +
                "raw.vibration_frame / mart.vibration_frame_features 테이블 데이터도 전부 삭제됩니다.\r\n" +
                "이 작업은 되돌릴 수 없습니다. 계속하시겠습니까?",
                "전체 초기화",
                MessageBoxButtons.YesNo,
                MessageBoxIcon.Warning);

            if (confirm != DialogResult.Yes)
                return;

            try
            {
                int sessionCount = 0;
                int totalObjectsDeleted = 0;

                using (var conn = new NpgsqlConnection(PgConnectionString))
                {
                    await conn.OpenAsync();

                    // 1) 모든 session 가져와서 MinIO 파일 삭제
                    using (var cmdSelect = new NpgsqlCommand(
                        "SELECT id, raw_file_path FROM session", conn))
                    using (var reader = await cmdSelect.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            sessionCount++;
                            var rawPath = reader["raw_file_path"] as string;

                            if (!string.IsNullOrWhiteSpace(rawPath))
                            {
                                try
                                {
                                    int deleted =
                                        await MinioRawFileHelper.DeleteSessionFilesByRawPathAsync(rawPath);
                                    totalObjectsDeleted += deleted;
                                }
                                catch (Exception exFile)
                                {
                                    AppEvents.RaiseLog(
                                        "[SessionManager][DeleteAll][MinIO ERROR] " + exFile);
                                }
                            }
                        }
                    }

                    // 2) DB에서 session / raw / mart 전부 삭제
                    using (var cmdDelete = new NpgsqlCommand(@"
                TRUNCATE TABLE raw.vibration_frame        CASCADE;
                TRUNCATE TABLE mart.vibration_frame_features CASCADE;
                TRUNCATE TABLE session                     CASCADE;
            ", conn))
                    {
                        await cmdDelete.ExecuteNonQueryAsync();
                    }
                }

                await RefreshSummaryAsync();

                MessageBox.Show(
                    $"모든 Session 및 관련 데이터 초기화 완료.\r\n" +
                    $"삭제된 Session 수: {sessionCount}\r\n" +
                    $"MinIO 삭제 오브젝트 수(대략): {totalObjectsDeleted}",
                    "전체 초기화 완료",
                    MessageBoxButtons.OK,
                    MessageBoxIcon.Information);

                AppEvents.RaiseLog(
                    $"[SessionManager] DeleteAllSessions: sessions={sessionCount}, objects={totalObjectsDeleted}");
            }
            catch (Exception ex)
            {
                MessageBox.Show("전체 초기화 중 오류: " + ex.Message, "Error",
                    MessageBoxButtons.OK, MessageBoxIcon.Error);
                AppEvents.RaiseLog("[SessionManager][DeleteAllSessions][ERROR] " + ex);
            }
        }
    }
}
