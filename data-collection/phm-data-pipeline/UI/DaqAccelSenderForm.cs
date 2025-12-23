using System;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Xml.Serialization;
using WeifenLuo.WinFormsUI.Docking;
using phm_data_pipeline.Models;
using phm_data_pipeline.Services;
using phm_data_pipeline.Services.Daq;
using phm_data_pipeline.Services.Inference;
using System.Collections.Generic;
using System.Collections.Concurrent;

namespace phm_data_pipeline.UI
{
    public class DaqAccelSenderForm : DockContent
    {
        private GroupBox grpMode;
        private RadioButton rdoModeDaq;
        private RadioButton rdoModeStub;

        private GroupBox grpDaq;
        private Label lblDevice;
        private ComboBox cboDevice;
        private Label lblChannels;
        private TextBox txtChannels;
        private Label lblSamplingRate;
        private NumericUpDown numSamplingRate;
        private Label lblSamplingRateUnit;
        private Label lblFrameSize;
        private NumericUpDown numFrameSize;

        private GroupBox grpKafka;
        private Label lblBootstrap;
        private TextBox txtBootstrap;
        private Label lblTopic;
        private TextBox txtTopic;
        private Label lblMachineId;
        private TextBox txtMachineId;

        private GroupBox grpMinio;
        private Label lblMinioEndpoint;
        private TextBox txtMinioEndpoint;
        private Label lblAccessKey;
        private TextBox txtAccessKey;
        private Label lblSecretKey;
        private TextBox txtSecretKey;
        private Label lblBucket;
        private TextBox txtBucket;
        private Label lblSaveType;
        private ComboBox cboSaveType;
        private Label lblSaveInterval;
        private NumericUpDown numSaveInterval;
        private Label lblSaveIntervalUnit;
        private CheckBox chkEnableRawSave;

        private GroupBox grpMeta;
        private Label lblTaskType;
        private ComboBox cboTaskType;
        private Label lblLabelType;
        private ComboBox cboLabelType;
        private Label lblSplit;
        private ComboBox cboSplit;
        private Label lblOperator;
        private TextBox txtOperator;
        private Label lblNote;
        private TextBox txtNote;

        private GroupBox grpControl;
        private Button btnStart;
        private Button btnStop;
        private Label lblStatusCaption;
        private Label lblStatus;

        private TableLayoutPanel mainLayout;

        private Label lblSensX;
        private NumericUpDown numSensX;
        private Label lblSensY;
        private NumericUpDown numSensY;
        private Label lblSensZ;
        private NumericUpDown numSensZ;

        private Label lblIepeCurrent;
        private ComboBox cboIepeCurrent;

        private Label lblMinG;
        private NumericUpDown numMinG;
        private Label lblMaxG;
        private NumericUpDown numMaxG;

        private Label lblAcqUnit;
        private RadioButton rdoUnitFrame;
        private RadioButton rdoUnitEvent;

        private Label lblEventRmsThreshold;
        private NumericUpDown numEventRmsThreshold;
        private Label lblEventMinGapMs;
        private NumericUpDown numEventMinGapMs;
        private CheckBox chkEnableRealtimeInference;

        // 실시간 추론 on/off 상태 (백그라운드 스레드에서 읽으므로 bool 대신 volatile)
        private volatile bool _enableRealtimeInference = true;

        private const string ConfigFileName = "daq_accel_config.xml";

        // 상단 필드 영역
        private IAccelFrameSource _frameSource;
        private IAcquisitionComponent _daqComponent;
        private KafkaSenderAcquisitionComponent _kafkaComponent;
        private MinioSenderAcquisitionComponent _minioComponent;

        private AcquisitionHandle _daqHandle;
        private AcquisitionHandle _kafkaHandle;
        private AcquisitionHandle _minioHandle;

        private CancellationTokenSource _acquisitionCts;

        private PgFrameSink _pgSink;

        private OnnxClassificationModel _anomalyModel;
        private OnnxClassificationModel _faultModel;

        // ★ 이벤트 단위 감지용 상태
        private SampleEventDetector _eventDetector;
        private long _currentSessionId = -1;
        private long _frameCountForLog = 0;

        private class RealtimeInferenceRequest
        {
            public AccelFrame Frame { get; set; }
            public string TaskType { get; set; }
        }

        private BlockingCollection<RealtimeInferenceRequest> _inferenceQueue;
        private Task _inferenceWorker;
        private CancellationTokenSource _inferenceCts;

        public DaqAccelSenderForm()
        {
            InitializeComponent();
            LoadConfigSafe();

            // TODO: 실제 환경에 맞는 연결 문자열로 교체
            var connString = "Host=localhost;Port=15432;Database=phm;Username=phm_user;Password=phm-password";
            _pgSink = new PgFrameSink(connString);

            TryLoadOnnxModels();
        }

        private void TryLoadOnnxModels()
        {
            try
            {
                var baseDir = AppDomain.CurrentDomain.BaseDirectory;
                var modelDir = Path.Combine(baseDir, "models");

                if (!Directory.Exists(modelDir))
                {
                    AppEvents.RaiseLog("[UI] models 디렉토리가 없습니다: " + modelDir);
                    return;
                }

                var anomalyPath = Path.Combine(modelDir, "anomaly_model_latest.onnx");
                var anomalyMetaPath = Path.Combine(modelDir, "anomaly_latest_meta.json");

                if (File.Exists(anomalyPath))
                {
                    string[] labels = null;

                    if (File.Exists(anomalyMetaPath))
                    {
                        try
                        {
                            var json = File.ReadAllText(anomalyMetaPath);
                            dynamic meta = Newtonsoft.Json.JsonConvert.DeserializeObject(json);
                            if (meta.class_labels != null)
                            {
                                var list = new List<string>();
                                foreach (var s in meta.class_labels)
                                    list.Add((string)s);
                                labels = list.ToArray();
                            }

                            AppEvents.RaiseLog("[UI] anomaly meta loaded: class_labels=" +
                                (labels != null ? string.Join(",", labels) : "(null)"));
                        }
                        catch (Exception ex)
                        {
                            AppEvents.RaiseLog("[UI] anomaly meta 읽기 실패: " + ex);
                        }
                    }

                    // meta에서 못 읽었으면, 일단 기본값이나 임시 배열을 사용
                    if (labels == null)
                    {
                        labels = new[] { "anomaly", "normal" }; // 모델.classes_ 추정순서에 맞게
                    }

                    _anomalyModel = new OnnxClassificationModel(anomalyPath, labels);
                    AppEvents.RaiseLog("[UI] ONNX anomaly model loaded: " + anomalyPath);
                }
                else
                {
                    AppEvents.RaiseLog("[UI] anomaly_model_latest.onnx 찾지 못함 (dir=" + modelDir + ")");
                }

                var faultPath = GetLatestModelPath(modelDir, "fault_model_latest*.onnx");
                var faultMetaPath = Path.Combine(modelDir, "fault_latest_meta.json");
                if (faultPath != null)
                {
                    string[] labels = null;

                    if (File.Exists(faultMetaPath))
                    {
                        try
                        {
                            var json = File.ReadAllText(faultMetaPath);
                            dynamic meta = Newtonsoft.Json.JsonConvert.DeserializeObject(json);
                            if (meta.class_labels != null)
                            {
                                var list = new List<string>();
                                foreach (var s in meta.class_labels)
                                    list.Add((string)s);
                                labels = list.ToArray();
                            }

                            AppEvents.RaiseLog("[UI] fault meta loaded: class_labels=" +
                                (labels != null ? string.Join(",", labels) : "(null)"));
                        }
                        catch (Exception ex)
                        {
                            AppEvents.RaiseLog("[UI] fault meta 읽기 실패: " + ex);
                        }
                    }

                    // meta에서 못 읽었으면, 임시 기본값으로 fallback
                    if (labels == null)
                    {
                        labels = new[] { "normal", "fault_A", "fault_B", "fault_C" };
                        AppEvents.RaiseLog("[UI] fault meta class_labels 미존재 → 기본 labels 사용");
                    }

                    _faultModel = new OnnxClassificationModel(faultPath, labels);
                    AppEvents.RaiseLog("[UI] ONNX fault model loaded: " + faultPath);
                }
                else
                {
                    AppEvents.RaiseLog("[UI] fault_model_*.onnx 찾지 못함 (dir=" + modelDir + ")");
                }
            }
            catch (Exception ex)
            {
                AppEvents.RaiseLog("[UI] ONNX model load failed: " + ex);
            }
        }

        private string GetLatestModelPath(string dir, string pattern)
        {
            try
            {
                if (!Directory.Exists(dir))
                    return null;

                var files = Directory.GetFiles(dir, pattern);
                if (files == null || files.Length == 0)
                    return null;

                return files
                    .Select(path => new FileInfo(path))
                    .OrderByDescending(fi => fi.LastWriteTimeUtc)
                    .First()
                    .FullName;
            }
            catch
            {
                return null;
            }
        }

        private void InitializeComponent()
        {
            // ===== 기본 폼 설정 =====
            this.Text = "가속도 DAQ 송신기";
            this.Font = new Font("맑은 고딕", 9F, FontStyle.Regular, GraphicsUnit.Point, 129);
            this.BackColor = Color.White;
            this.Padding = new Padding(8);
            this.ClientSize = new Size(1200, 750);

            // ===== 메인 레이아웃: 상단(설정) / 하단(제어) =====
            mainLayout = new TableLayoutPanel();
            mainLayout.Dock = DockStyle.Fill;
            mainLayout.ColumnCount = 1;
            mainLayout.RowCount = 2;
            mainLayout.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100F));
            mainLayout.RowStyles.Add(new RowStyle(SizeType.Percent, 100F)); // 설정 영역
            mainLayout.RowStyles.Add(new RowStyle(SizeType.AutoSize));      // 제어/상태
            this.Controls.Add(mainLayout);

            // ===== 상단 설정 레이아웃: 1열 세로 나열 =====
            var topLayout = new TableLayoutPanel();
            topLayout.Dock = DockStyle.Fill;
            topLayout.ColumnCount = 1;
            topLayout.RowCount = 5; // Mode, DAQ, MinIO, Meta, Kafka
            topLayout.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100F));
            topLayout.RowStyles.Add(new RowStyle(SizeType.AutoSize));
            topLayout.RowStyles.Add(new RowStyle(SizeType.AutoSize));
            topLayout.RowStyles.Add(new RowStyle(SizeType.AutoSize));
            topLayout.RowStyles.Add(new RowStyle(SizeType.AutoSize));
            topLayout.RowStyles.Add(new RowStyle(SizeType.AutoSize));
            mainLayout.Controls.Add(topLayout, 0, 0);

            // ------------------------------------------------------------------
            // 1) 모드 (가속도 수집 방식)
            // ------------------------------------------------------------------
            grpMode = new GroupBox();
            grpMode.Text = "모드 (가속도 수집 방식)";
            grpMode.Dock = DockStyle.Top;
            grpMode.Padding = new Padding(10);
            grpMode.AutoSize = true;
            grpMode.AutoSizeMode = AutoSizeMode.GrowAndShrink;

            var tlpMode = new TableLayoutPanel();
            tlpMode.Dock = DockStyle.Fill;
            tlpMode.ColumnCount = 2;
            tlpMode.RowCount = 1;
            tlpMode.AutoSize = true;
            tlpMode.AutoSizeMode = AutoSizeMode.GrowAndShrink;
            tlpMode.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));
            tlpMode.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));

            rdoModeDaq = new RadioButton();
            rdoModeDaq.Text = "DAQ (실제 NI-DAQ)";
            rdoModeDaq.AutoSize = true;

            rdoModeStub = new RadioButton();
            rdoModeStub.Text = "Stub (랜덤 데이터)";
            rdoModeStub.AutoSize = true;
            rdoModeStub.Checked = true; // 기본 Stub

            tlpMode.Controls.Add(rdoModeDaq, 0, 0);
            tlpMode.Controls.Add(rdoModeStub, 1, 0);

            MakeCollapsibleGroup(grpMode, tlpMode, startCollapsed: false);
            topLayout.Controls.Add(grpMode, 0, 0);

            // ------------------------------------------------------------------
            // 2) 수집 목적 (메타데이터)
            // ------------------------------------------------------------------
            grpMeta = new GroupBox();
            grpMeta.Text = "수집 목적 (작업 메타데이터)";
            grpMeta.Dock = DockStyle.Top;
            grpMeta.Padding = new Padding(10);
            grpMeta.AutoSize = true;
            grpMeta.AutoSizeMode = AutoSizeMode.GrowAndShrink;

            var tlpMeta = new TableLayoutPanel();
            tlpMeta.Dock = DockStyle.Fill;
            tlpMeta.AutoSize = true;
            tlpMeta.AutoSizeMode = AutoSizeMode.GrowAndShrink;
            tlpMeta.ColumnCount = 2;
            tlpMeta.RowCount = 5;
            tlpMeta.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));
            tlpMeta.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100F));

            // Task Type
            lblTaskType = new Label();
            lblTaskType.Text = "Task Type";
            lblTaskType.AutoSize = true;

            cboTaskType = new ComboBox();
            cboTaskType.DropDownStyle = ComboBoxStyle.DropDownList;
            cboTaskType.Items.AddRange(new object[] { "anomaly_detection", "fault_diagnosis", "rul", "etc" });
            cboTaskType.SelectedIndex = 0;
            cboTaskType.Width = 130;

            cboTaskType.SelectedIndexChanged += (_, __) =>
            {
                var task = (string)cboTaskType.SelectedItem;
                cboLabelType.Items.Clear();

                if (task == "anomaly_detection")
                {
                    cboLabelType.Items.AddRange(new object[] { "normal", "anomaly" });
                }
                else if (task == "fault_diagnosis")
                {
                    cboLabelType.Items.AddRange(new object[] { "normal", "fault_A", "fault_B", "..." });
                }
                else
                {
                    cboLabelType.Items.Add("normal");
                }

                if (cboLabelType.Items.Count > 0)
                    cboLabelType.SelectedIndex = 0;
            };

            tlpMeta.Controls.Add(lblTaskType, 0, 0);
            tlpMeta.Controls.Add(cboTaskType, 1, 0);

            // Label Type
            lblLabelType = new Label();
            lblLabelType.Text = "Label Type";
            lblLabelType.AutoSize = true;

            cboLabelType = new ComboBox();
            cboLabelType.DropDownStyle = ComboBoxStyle.DropDownList;
            cboLabelType.Items.AddRange(new object[] { "normal", "anomaly" });
            cboLabelType.SelectedIndex = 0;
            cboLabelType.Width = 80;

            tlpMeta.Controls.Add(lblLabelType, 0, 1);
            tlpMeta.Controls.Add(cboLabelType, 1, 1);

            // Data Split
            lblSplit = new Label();
            lblSplit.Text = "Data Split";
            lblSplit.AutoSize = true;

            cboSplit = new ComboBox();
            cboSplit.DropDownStyle = ComboBoxStyle.DropDownList;
            cboSplit.Items.AddRange(new object[] { "train", "val", "test" });
            cboSplit.SelectedIndex = 0;
            cboSplit.Width = 80;

            tlpMeta.Controls.Add(lblSplit, 0, 2);
            tlpMeta.Controls.Add(cboSplit, 1, 2);

            // Operator
            lblOperator = new Label();
            lblOperator.Text = "Operator";
            lblOperator.AutoSize = true;

            txtOperator = new TextBox();
            txtOperator.Text = "user01";
            txtOperator.Width = 80;

            tlpMeta.Controls.Add(lblOperator, 0, 3);
            tlpMeta.Controls.Add(txtOperator, 1, 3);

            // Note
            lblNote = new Label();
            lblNote.Text = "Note";
            lblNote.AutoSize = true;

            txtNote = new TextBox();
            txtNote.Width = 200;
            txtNote.Dock = DockStyle.Fill;

            tlpMeta.Controls.Add(lblNote, 0, 4);
            tlpMeta.Controls.Add(txtNote, 1, 4);

            MakeCollapsibleGroup(grpMeta, tlpMeta, startCollapsed: false);
            topLayout.Controls.Add(grpMeta, 0, 1);

            // ------------------------------------------------------------------
            // 3) DAQ 설정
            // ------------------------------------------------------------------
            grpDaq = new GroupBox();
            grpDaq.Text = "DAQ 설정";
            grpDaq.Dock = DockStyle.Top;
            grpDaq.Padding = new Padding(10);
            grpDaq.AutoSize = true;
            grpDaq.AutoSizeMode = AutoSizeMode.GrowAndShrink;

            var tlpDaq = new TableLayoutPanel();
            tlpDaq.Dock = DockStyle.Fill;
            tlpDaq.AutoSize = true;
            tlpDaq.AutoSizeMode = AutoSizeMode.GrowAndShrink;
            tlpDaq.ColumnCount = 3;
            tlpDaq.RowCount = 13;
            tlpDaq.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));          // Label
            tlpDaq.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100F));     // Control
            tlpDaq.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));          // Unit

            // Device
            lblDevice = new Label();
            lblDevice.Text = "Device";
            lblDevice.AutoSize = true;

            cboDevice = new ComboBox();
            cboDevice.DropDownStyle = ComboBoxStyle.DropDownList;
            cboDevice.Items.AddRange(new object[] { "cDAQ1Mod1", "cDAQ1Mod2" });
            cboDevice.SelectedIndex = 0;
            cboDevice.Dock = DockStyle.Left;

            tlpDaq.Controls.Add(lblDevice, 0, 0);
            tlpDaq.Controls.Add(cboDevice, 1, 0);

            // Channels
            lblChannels = new Label();
            lblChannels.Text = "Channels";
            lblChannels.AutoSize = true;

            txtChannels = new TextBox();
            txtChannels.Text = "ai0:2";
            txtChannels.Dock = DockStyle.Fill;

            tlpDaq.Controls.Add(lblChannels, 0, 1);
            tlpDaq.Controls.Add(txtChannels, 1, 1);
            tlpDaq.SetColumnSpan(txtChannels, 2);

            // Sampling Rate
            lblSamplingRate = new Label();
            lblSamplingRate.Text = "Sampling Rate";
            lblSamplingRate.AutoSize = true;

            numSamplingRate = new NumericUpDown();
            numSamplingRate.Maximum = 1000000;
            numSamplingRate.Minimum = 1;
            numSamplingRate.Value = 25600;
            numSamplingRate.ThousandsSeparator = true;
            numSamplingRate.Dock = DockStyle.Left;

            lblSamplingRateUnit = new Label();
            lblSamplingRateUnit.Text = "Hz";
            lblSamplingRateUnit.AutoSize = true;

            tlpDaq.Controls.Add(lblSamplingRate, 0, 2);
            tlpDaq.Controls.Add(numSamplingRate, 1, 2);
            tlpDaq.Controls.Add(lblSamplingRateUnit, 2, 2);

            // Frame Size
            lblFrameSize = new Label();
            lblFrameSize.Text = "Frame Size";
            lblFrameSize.AutoSize = true;

            numFrameSize = new NumericUpDown();
            numFrameSize.Maximum = 1000000;
            numFrameSize.Minimum = 1;
            numFrameSize.Value = 4096;
            numFrameSize.ThousandsSeparator = true;
            numFrameSize.Dock = DockStyle.Left;

            tlpDaq.Controls.Add(lblFrameSize, 0, 3);
            tlpDaq.Controls.Add(numFrameSize, 1, 3);

            // 수집 단위: 프레임 / 이벤트
            lblAcqUnit = new Label();
            lblAcqUnit.Text = "수집 단위";
            lblAcqUnit.AutoSize = true;

            var pnlAcqUnit = new FlowLayoutPanel();
            pnlAcqUnit.Dock = DockStyle.Fill;
            pnlAcqUnit.AutoSize = true;
            pnlAcqUnit.FlowDirection = FlowDirection.LeftToRight;

            rdoUnitFrame = new RadioButton();
            rdoUnitFrame.Text = "프레임 단위";
            rdoUnitFrame.AutoSize = true;
            rdoUnitFrame.Checked = true;

            rdoUnitEvent = new RadioButton();
            rdoUnitEvent.Text = "이벤트 단위";
            rdoUnitEvent.AutoSize = true;

            pnlAcqUnit.Controls.Add(rdoUnitFrame);
            pnlAcqUnit.Controls.Add(rdoUnitEvent);

            tlpDaq.Controls.Add(lblAcqUnit, 0, 4);
            tlpDaq.Controls.Add(pnlAcqUnit, 1, 4);
            tlpDaq.SetColumnSpan(pnlAcqUnit, 2);

            // 이벤트 파라미터
            lblEventRmsThreshold = new Label();
            lblEventRmsThreshold.Text = "이벤트 RMS Threshold";
            lblEventRmsThreshold.AutoSize = true;

            numEventRmsThreshold = new NumericUpDown();
            numEventRmsThreshold.Minimum = 0;
            numEventRmsThreshold.Maximum = 1000;
            numEventRmsThreshold.DecimalPlaces = 2;
            numEventRmsThreshold.Increment = 0.1M;
            numEventRmsThreshold.Value = 0.50M;
            numEventRmsThreshold.Width = 80;

            tlpDaq.Controls.Add(lblEventRmsThreshold, 0, 5);
            tlpDaq.Controls.Add(numEventRmsThreshold, 1, 5);

            lblEventMinGapMs = new Label();
            lblEventMinGapMs.Text = "이벤트 최소 간격 (ms)";
            lblEventMinGapMs.AutoSize = true;

            numEventMinGapMs = new NumericUpDown();
            numEventMinGapMs.Minimum = 1;
            numEventMinGapMs.Maximum = 10000;
            numEventMinGapMs.Value = 200;
            numEventMinGapMs.Width = 80;

            tlpDaq.Controls.Add(lblEventMinGapMs, 0, 6);
            tlpDaq.Controls.Add(numEventMinGapMs, 1, 6);

            // IEPE 전류
            lblIepeCurrent = new Label();
            lblIepeCurrent.Text = "IEPE 전류 (mA)";
            lblIepeCurrent.AutoSize = true;

            cboIepeCurrent = new ComboBox();
            cboIepeCurrent.DropDownStyle = ComboBoxStyle.DropDownList;
            cboIepeCurrent.Items.AddRange(new object[] { "2", "4", "10" });
            cboIepeCurrent.SelectedIndex = 1;
            cboIepeCurrent.Width = 80;

            tlpDaq.Controls.Add(lblIepeCurrent, 0, 7);
            tlpDaq.Controls.Add(cboIepeCurrent, 1, 7);

            // Sensitivity X
            lblSensX = new Label();
            lblSensX.Text = "Sensitivity X (mV/g)";
            lblSensX.AutoSize = true;

            numSensX = new NumericUpDown();
            numSensX.Minimum = 1;
            numSensX.Maximum = 100000;
            numSensX.DecimalPlaces = 1;
            numSensX.Increment = 0.1M;
            numSensX.Value = 100;
            numSensX.Dock = DockStyle.Left;

            tlpDaq.Controls.Add(lblSensX, 0, 8);
            tlpDaq.Controls.Add(numSensX, 1, 8);

            // Sensitivity Y
            lblSensY = new Label();
            lblSensY.Text = "Sensitivity Y (mV/g)";
            lblSensY.AutoSize = true;

            numSensY = new NumericUpDown();
            numSensY.Minimum = 1;
            numSensY.Maximum = 100000;
            numSensY.DecimalPlaces = 1;
            numSensY.Increment = 0.1M;
            numSensY.Value = 100;
            numSensY.Dock = DockStyle.Left;

            tlpDaq.Controls.Add(lblSensY, 0, 9);
            tlpDaq.Controls.Add(numSensY, 1, 9);

            // Sensitivity Z
            lblSensZ = new Label();
            lblSensZ.Text = "Sensitivity Z (mV/g)";
            lblSensZ.AutoSize = true;

            numSensZ = new NumericUpDown();
            numSensZ.Minimum = 1;
            numSensZ.Maximum = 100000;
            numSensZ.DecimalPlaces = 1;
            numSensZ.Increment = 0.1M;
            numSensZ.Value = 100;
            numSensZ.Dock = DockStyle.Left;

            tlpDaq.Controls.Add(lblSensZ, 0, 10);
            tlpDaq.Controls.Add(numSensZ, 1, 10);

            // Min G
            lblMinG = new Label();
            lblMinG.Text = "Min G";
            lblMinG.AutoSize = true;

            numMinG = new NumericUpDown();
            numMinG.Minimum = -1000;
            numMinG.Maximum = 0;
            numMinG.DecimalPlaces = 1;
            numMinG.Increment = 0.5M;
            numMinG.Value = -25;
            numMinG.Dock = DockStyle.Left;

            tlpDaq.Controls.Add(lblMinG, 0, 11);
            tlpDaq.Controls.Add(numMinG, 1, 11);

            // Max G
            lblMaxG = new Label();
            lblMaxG.Text = "Max G";
            lblMaxG.AutoSize = true;

            numMaxG = new NumericUpDown();
            numMaxG.Minimum = 0;
            numMaxG.Maximum = 1000;
            numMaxG.DecimalPlaces = 1;
            numMaxG.Increment = 0.5M;
            numMaxG.Value = 25;
            numMaxG.Dock = DockStyle.Left;

            tlpDaq.Controls.Add(lblMaxG, 0, 12);
            tlpDaq.Controls.Add(numMaxG, 1, 12);

            MakeCollapsibleGroup(grpDaq, tlpDaq, startCollapsed: false);
            topLayout.Controls.Add(grpDaq, 0, 2);

            // ------------------------------------------------------------------
            // 4) MinIO 설정
            // ------------------------------------------------------------------
            grpMinio = new GroupBox();
            grpMinio.Text = "MinIO 설정";
            grpMinio.Dock = DockStyle.Top;
            grpMinio.Padding = new Padding(10);
            grpMinio.AutoSize = true;
            grpMinio.AutoSizeMode = AutoSizeMode.GrowAndShrink;

            var tlpMinio = new TableLayoutPanel();
            tlpMinio.Dock = DockStyle.Fill;
            tlpMinio.AutoSize = true;
            tlpMinio.AutoSizeMode = AutoSizeMode.GrowAndShrink;
            tlpMinio.ColumnCount = 3;
            tlpMinio.RowCount = 6;
            tlpMinio.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));
            tlpMinio.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100F));
            tlpMinio.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));

            lblMinioEndpoint = new Label();
            lblMinioEndpoint.Text = "MinIO Endpoint";
            lblMinioEndpoint.AutoSize = true;

            txtMinioEndpoint = new TextBox();
            txtMinioEndpoint.Text = "http://localhost:19000";
            txtMinioEndpoint.Dock = DockStyle.Fill;

            tlpMinio.Controls.Add(lblMinioEndpoint, 0, 0);
            tlpMinio.Controls.Add(txtMinioEndpoint, 1, 0);
            tlpMinio.SetColumnSpan(txtMinioEndpoint, 2);

            lblAccessKey = new Label();
            lblAccessKey.Text = "AccessKey";
            lblAccessKey.AutoSize = true;

            txtAccessKey = new TextBox();
            txtAccessKey.Dock = DockStyle.Fill;

            tlpMinio.Controls.Add(lblAccessKey, 0, 1);
            tlpMinio.Controls.Add(txtAccessKey, 1, 1);
            tlpMinio.SetColumnSpan(txtAccessKey, 2);

            lblSecretKey = new Label();
            lblSecretKey.Text = "SecretKey";
            lblSecretKey.AutoSize = true;

            txtSecretKey = new TextBox();
            txtSecretKey.UseSystemPasswordChar = true;
            txtSecretKey.Dock = DockStyle.Fill;

            tlpMinio.Controls.Add(lblSecretKey, 0, 2);
            tlpMinio.Controls.Add(txtSecretKey, 1, 2);
            tlpMinio.SetColumnSpan(txtSecretKey, 2);

            lblBucket = new Label();
            lblBucket.Text = "Bucket";
            lblBucket.AutoSize = true;

            txtBucket = new TextBox();
            txtBucket.Text = "phm-raw";
            txtBucket.Dock = DockStyle.Fill;

            tlpMinio.Controls.Add(lblBucket, 0, 3);
            tlpMinio.Controls.Add(txtBucket, 1, 3);
            tlpMinio.SetColumnSpan(txtBucket, 2);

            lblSaveType = new Label();
            lblSaveType.Text = "Save Type";
            lblSaveType.AutoSize = true;

            cboSaveType = new ComboBox();
            cboSaveType.DropDownStyle = ComboBoxStyle.DropDownList;
            cboSaveType.Items.AddRange(new object[] { "jsonl", "npy", "bin" });
            cboSaveType.SelectedIndex = 0;
            cboSaveType.Width = 100;

            lblSaveInterval = new Label();
            lblSaveInterval.Text = "Save Interval";
            lblSaveInterval.AutoSize = true;

            numSaveInterval = new NumericUpDown();
            numSaveInterval.DecimalPlaces = 2;
            numSaveInterval.Increment = 0.05M;
            numSaveInterval.Minimum = 0.05M;
            numSaveInterval.Maximum = 60M;
            numSaveInterval.Value = 1.00M;
            numSaveInterval.Width = 70;

            lblSaveIntervalUnit = new Label();
            lblSaveIntervalUnit.Text = "(분 단위, 소수 허용)";
            lblSaveIntervalUnit.AutoSize = true;

            chkEnableRawSave = new CheckBox();
            chkEnableRawSave.Text = "Raw 저장 활성화";
            chkEnableRawSave.AutoSize = true;

            tlpMinio.Controls.Add(lblSaveType, 0, 4);
            tlpMinio.Controls.Add(cboSaveType, 1, 4);
            tlpMinio.Controls.Add(chkEnableRawSave, 2, 4);

            tlpMinio.Controls.Add(lblSaveInterval, 0, 5);
            tlpMinio.Controls.Add(numSaveInterval, 1, 5);
            tlpMinio.Controls.Add(lblSaveIntervalUnit, 2, 5);

            MakeCollapsibleGroup(grpMinio, tlpMinio, startCollapsed: false);
            topLayout.Controls.Add(grpMinio, 0, 3);

            // ------------------------------------------------------------------
            // 5) Kafka 설정
            // ------------------------------------------------------------------
            grpKafka = new GroupBox();
            grpKafka.Text = "Kafka 설정";
            grpKafka.Dock = DockStyle.Top;
            grpKafka.Padding = new Padding(10);
            grpKafka.AutoSize = true;
            grpKafka.AutoSizeMode = AutoSizeMode.GrowAndShrink;

            var tlpKafka = new TableLayoutPanel();
            tlpKafka.Dock = DockStyle.Fill;
            tlpKafka.AutoSize = true;
            tlpKafka.AutoSizeMode = AutoSizeMode.GrowAndShrink;
            tlpKafka.ColumnCount = 2;
            tlpKafka.RowCount = 3;
            tlpKafka.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));
            tlpKafka.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100F));

            lblBootstrap = new Label();
            lblBootstrap.Text = "Bootstrap";
            lblBootstrap.AutoSize = true;

            txtBootstrap = new TextBox();
            txtBootstrap.Text = "localhost:9093";
            txtBootstrap.Dock = DockStyle.Fill;

            tlpKafka.Controls.Add(lblBootstrap, 0, 0);
            tlpKafka.Controls.Add(txtBootstrap, 1, 0);

            lblTopic = new Label();
            lblTopic.Text = "Topic";
            lblTopic.AutoSize = true;

            txtTopic = new TextBox();
            txtTopic.Text = "sensor.raw";
            txtTopic.Dock = DockStyle.Fill;

            tlpKafka.Controls.Add(lblTopic, 0, 1);
            tlpKafka.Controls.Add(txtTopic, 1, 1);

            lblMachineId = new Label();
            lblMachineId.Text = "Machine ID";
            lblMachineId.AutoSize = true;

            txtMachineId = new TextBox();
            txtMachineId.Text = "M001";
            txtMachineId.Dock = DockStyle.Fill;

            tlpKafka.Controls.Add(lblMachineId, 0, 2);
            tlpKafka.Controls.Add(txtMachineId, 1, 2);

            MakeCollapsibleGroup(grpKafka, tlpKafka, startCollapsed: false);
            topLayout.Controls.Add(grpKafka, 0, 4);

            // ------------------------------------------------------------------
            // 6) 제어 / 상태 (하단 전체)
            // ------------------------------------------------------------------
            grpControl = new GroupBox();
            grpControl.Text = "제어 / 상태";
            grpControl.Dock = DockStyle.Fill;
            grpControl.Padding = new Padding(10);
            grpControl.AutoSize = true;
            grpControl.AutoSizeMode = AutoSizeMode.GrowAndShrink;
            mainLayout.Controls.Add(grpControl, 0, 1);

            var tlpControl = new TableLayoutPanel();
            tlpControl.Dock = DockStyle.Fill;
            tlpControl.AutoSize = true;
            tlpControl.AutoSizeMode = AutoSizeMode.GrowAndShrink;
            tlpControl.ColumnCount = 5;
            tlpControl.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));          // Start
            tlpControl.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));          // Stop
            tlpControl.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));          // Realtime chk
            tlpControl.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));          // "Status :"
            tlpControl.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100F));     // status text

            btnStart = new Button();
            btnStart.Text = "Start";
            btnStart.Width = 80;
            btnStart.Click += BtnStart_Click;

            btnStop = new Button();
            btnStop.Text = "Stop";
            btnStop.Width = 80;
            btnStop.Click += BtnStop_Click;

            // ★ 실시간 추론 ON/OFF 체크박스
            chkEnableRealtimeInference = new CheckBox();
            chkEnableRealtimeInference.Text = "실시간 추론";
            chkEnableRealtimeInference.AutoSize = true;
            chkEnableRealtimeInference.Checked = true;   // 기본 ON
            chkEnableRealtimeInference.CheckedChanged += (s, e) =>
            {
                _enableRealtimeInference = chkEnableRealtimeInference.Checked;
                AppEvents.RaiseLog("[UI] 실시간 추론 " +
                    (_enableRealtimeInference ? "ON" : "OFF"));
            };

            lblStatusCaption = new Label();
            lblStatusCaption.Text = "Status :";
            lblStatusCaption.AutoSize = true;
            lblStatusCaption.Anchor = AnchorStyles.Left;

            lblStatus = new Label();
            lblStatus.Text = "대기 중...";
            lblStatus.AutoSize = true;
            lblStatus.Anchor = AnchorStyles.Left;

            tlpControl.Controls.Add(btnStart, 0, 0);
            tlpControl.Controls.Add(btnStop, 1, 0);
            tlpControl.Controls.Add(chkEnableRealtimeInference, 2, 0);
            tlpControl.Controls.Add(lblStatusCaption, 3, 0);
            tlpControl.Controls.Add(lblStatus, 4, 0);

            grpControl.Controls.Add(tlpControl);
        }

        // 공통: GroupBox 내용 접었다/펼치는 헬퍼
        private void MakeCollapsibleGroup(GroupBox groupBox, Control innerContent, bool startCollapsed = false)
        {
            groupBox.Padding = new Padding(8, 10, 8, 8);

            var outer = new TableLayoutPanel
            {
                Dock = DockStyle.Fill,
                ColumnCount = 1,
                RowCount = 2,
                AutoSize = true,
                AutoSizeMode = AutoSizeMode.GrowAndShrink
            };
            outer.RowStyles.Add(new RowStyle(SizeType.AutoSize));
            outer.RowStyles.Add(new RowStyle(SizeType.Percent, 100F));

            var headerPanel = new Panel
            {
                Dock = DockStyle.Top,
                Height = 24
            };

            var lblTitle = new Label
            {
                Text = groupBox.Text,
                AutoSize = true,
                Dock = DockStyle.Left,
                TextAlign = ContentAlignment.MiddleLeft
            };

            var btnToggle = new Button
            {
                Text = startCollapsed ? "▼" : "▲",
                Width = 28,
                Dock = DockStyle.Right,
                Margin = new Padding(0),
                Padding = new Padding(0)
            };

            var contentPanel = new Panel
            {
                Dock = DockStyle.Fill,
                AutoSize = true,
                AutoSizeMode = AutoSizeMode.GrowAndShrink
            };
            innerContent.Dock = DockStyle.Fill;
            contentPanel.Controls.Add(innerContent);

            btnToggle.Tag = contentPanel;
            btnToggle.Click += ToggleSection_Click;

            headerPanel.Controls.Add(btnToggle);
            headerPanel.Controls.Add(lblTitle);

            outer.Controls.Add(headerPanel, 0, 0);
            outer.Controls.Add(contentPanel, 0, 1);

            contentPanel.Visible = !startCollapsed;

            groupBox.Text = string.Empty;
            groupBox.Controls.Clear();
            groupBox.Controls.Add(outer);
        }

        private void ToggleSection_Click(object sender, EventArgs e)
        {
            if (sender is Button btn && btn.Tag is Panel pnl)
            {
                pnl.Visible = !pnl.Visible;
                btn.Text = pnl.Visible ? "▲" : "▼";
            }
        }

        private void SetStatus(string text)
        {
            if (lblStatus.InvokeRequired)
            {
                lblStatus.Invoke(new Action<string>(SetStatus), text);
                return;
            }

            lblStatus.Text = text;
        }

        private void UpdateRunningState(bool isRunning)
        {
            if (btnStart.InvokeRequired)
            {
                btnStart.Invoke(new Action<bool>(UpdateRunningState), isRunning);
                return;
            }

            btnStart.Enabled = !isRunning;
            btnStop.Enabled = isRunning;
        }

        /// <summary>
        /// ONNX 실시간 추론 전용 워커 스레드.
        /// FrameGenerated에서 큐에 넣어준 프레임을 순서대로 꺼내서
        /// ProcessRealtimeFrame(...)을 호출한다.
        /// </summary>
        private void InferenceLoop(CancellationToken ct)
        {
            try
            {
                foreach (var req in _inferenceQueue.GetConsumingEnumerable(ct))
                {
                    if (ct.IsCancellationRequested)
                        break;

                    try
                    {
                        // 기존 로직 그대로 사용 (특징 계산 + ONNX + UI 이벤트 + 로그)
                        ProcessRealtimeFrame(req.Frame, req.TaskType);
                    }
                    catch (Exception ex)
                    {
                        AppEvents.RaiseLog("[UI] InferenceLoop 예외: " + ex);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // 정상 종료
            }
            catch (ObjectDisposedException)
            {
                // 종료 과정에서 발생할 수 있음 → 무시
            }
            catch (Exception ex)
            {
                AppEvents.RaiseLog("[UI] InferenceLoop 치명적 예외: " + ex);
            }
            finally
            {
                AppEvents.RaiseLog("[UI] Inference worker 종료");
            }
        }

        private async void BtnStart_Click(object sender, EventArgs e)
        {
            try
            {
                if (_daqHandle != null && _daqHandle.State == AcquisitionState.Running)
                {
                    AppEvents.RaiseLog("[UI] 이미 수집 중입니다.");
                    return;
                }

                var cfg = GetConfig();
                SaveConfigSafe();

                string taskType = (cfg.Meta?.TaskType ?? "").ToLowerInvariant();
                string acqUnit = cfg.Daq?.AcquisitionUnit ?? "frame";

                AppEvents.RaiseLog(
                    $"[UI] taskType={taskType}, acq_unit={acqUnit}, " +
                    $"anomaly_model={(_anomalyModel != null)}, fault_model={(_faultModel != null)}");

                double sampleRateHz = 12800.0;
                if (cfg.Daq != null && cfg.Daq.SamplingRate > 0)
                    sampleRateHz = cfg.Daq.SamplingRate;

                var sessionInfo = BuildSessionInfo(cfg, sampleRateHz);

                try
                {
                    var sessionId = _pgSink?.OnSessionStarted(sessionInfo) ?? -1;
                    _currentSessionId = sessionId;
                    AppEvents.RaiseLog($"[UI] DB 세션 시작 기록 완료 (session_id={sessionId})");
                }
                catch (Exception dbEx)
                {
                    _currentSessionId = -1;
                    AppEvents.RaiseLog("[UI] DB 세션 기록 실패: " + dbEx);
                }

                _acquisitionCts = new CancellationTokenSource();
                var ct = _acquisitionCts.Token;

                // ★ 프레임 단위 추론용 큐/워커 준비 (event 모드가 아닐 때만)
                _inferenceCts?.Cancel();
                _inferenceQueue = null;
                _inferenceWorker = null;

                if (!string.Equals(acqUnit, "event", StringComparison.OrdinalIgnoreCase))
                {
                    _inferenceCts = new CancellationTokenSource();
                    _inferenceQueue = new BlockingCollection<RealtimeInferenceRequest>(boundedCapacity: 32);
                    _inferenceWorker = Task.Run(() => InferenceLoop(_inferenceCts.Token));
                    AppEvents.RaiseLog("[UI] Inference worker 시작 (queue size = 32)");
                }

                UpdateRunningState(true);
                SetStatus("시작 중...");
                AppEvents.RaiseLog("[UI] Start 클릭 - 수집/송신 시작");

                IAcquisitionComponent daqComp;
                IAccelFrameSource frameSource;

                int samplesPerBatch =
                    (cfg.Daq != null && cfg.Daq.FrameSize > 0) ? cfg.Daq.FrameSize : 1000;

                var secondsPerFrame = samplesPerBatch / sampleRateHz;
                var batchInterval = TimeSpan.FromSeconds(secondsPerFrame);

                // ★ 이벤트 감지기 초기화 (세션 단위 상태)
                _eventDetector = null;
                bool isEventMode = string.Equals(acqUnit, "event", StringComparison.OrdinalIgnoreCase);

                if (isEventMode && _currentSessionId > 0)
                {
                    _eventDetector = new SampleEventDetector(_currentSessionId, sampleRateHz, cfg);

                    var taskTypeLocalForEvent = taskType;

                    _eventDetector.EventDetected += ev =>
                    {
                        AppEvents.RaiseLog(
                            $"[Event] session={ev.SessionId}, idx={ev.EventIndex}, " +
                            $"samples={ev.StartSampleIndex}~{ev.EndSampleIndex}, " +
                            $"maxRms={ev.MaxRms:F3}, maxAbs={ev.MaxAbsValue:F3}");

                        _minioComponent?.OnEventDetected(ev);

                        // ★ 이벤트 구간 전체 샘플로 ONNX 진단
                        ProcessEventForInference(ev, taskTypeLocalForEvent);
                    };

                    AppEvents.RaiseLog(
                        $"[UI] Event mode 활성화: threshold={cfg.Daq.EventRmsThreshold}, gapMs={cfg.Daq.EventMinGapMs}");
                }

                if (cfg.Mode == "DAQ")
                {
                    var dev = new NiDaqDeviceAcquisitionComponent("DeviceDAQ");
                    dev.SetSampleRate(sampleRateHz);

                    if (cfg.Daq != null)
                    {
                        if (cfg.Daq.SensitivityX_mVpg > 0)
                            dev.SensitivityX_mVpg = cfg.Daq.SensitivityX_mVpg;

                        if (cfg.Daq.SensitivityY_mVpg > 0)
                            dev.SensitivityY_mVpg = cfg.Daq.SensitivityY_mVpg;

                        if (cfg.Daq.SensitivityZ_mVpg > 0)
                            dev.SensitivityZ_mVpg = cfg.Daq.SensitivityZ_mVpg;

                        if (cfg.Daq.IepeCurrent_mA > 0)
                            dev.IepeMilliAmps = cfg.Daq.IepeCurrent_mA;

                        dev.MinG = cfg.Daq.MinG;
                        dev.MaxG = cfg.Daq.MaxG;
                    }

                    var taskTypeLocal = taskType;
                    dev.FrameGenerated += frame =>
                    {
                        if (!isEventMode)
                        {
                            // ★ 추론 전용 큐에 프레임 enqueue (꽉 차 있으면 조용히 버림)
                            var q = _inferenceQueue;
                            if (q != null && !q.IsAddingCompleted)
                            {
                                q.TryAdd(new RealtimeInferenceRequest
                                {
                                    Frame = frame,
                                    TaskType = taskTypeLocal
                                });
                            }
                        }
                    };

                    daqComp = dev;
                    frameSource = dev;
                }
                else
                {
                    var stub = new NiDaqStubAcquisitionComponent(
                        batchInterval,
                        samplesPerBatch,
                        "StubDAQ");

                    stub.SetSampleRate(sampleRateHz);

                    var taskTypeLocal = taskType;

                    // Stub 프레임당 모델 추론 + 로그 → 전용 큐로 위임
                    stub.FrameGenerated += frame =>
                    {
                        if (!isEventMode)
                        {
                            var q = _inferenceQueue;
                            if (q != null && !q.IsAddingCompleted)
                            {
                                q.TryAdd(new RealtimeInferenceRequest
                                {
                                    Frame = frame,
                                    TaskType = taskTypeLocal
                                });
                            }
                        }
                    };

                    daqComp = stub;
                    frameSource = stub;
                }

                _daqComponent = daqComp;
                _frameSource = frameSource;

                // ★ 프레임 소스에 이벤트 감지기 연결 (Stub/DAQ 공통)
                if (isEventMode && _eventDetector != null && _frameSource != null)
                {
                    _frameSource.FrameGenerated += frame =>
                    {
                        _eventDetector.OnFrame(frame);
                    };
                }

                AppEvents.RaiseFrameSourceChanged(_frameSource);

                var progress = new Progress<AcquisitionProgress>(p =>
                {
                    string msg = string.Format(
                        "{0} | Total={1:N0} @ {2:HH:mm:ss}",
                        p.Message,
                        p.TotalSamplesSent,
                        p.LastBatchAt.LocalDateTime);

                    SetStatus(msg);
                });

                _daqHandle = await _daqComponent.StartAcquisitionAsync(
                    cfg,
                    progress,
                    ct);

                _kafkaComponent = new KafkaSenderAcquisitionComponent("KafkaSender", frameSource);

                _kafkaHandle = await _kafkaComponent.StartAcquisitionAsync(
                    cfg,
                    null,
                    ct);

                _minioComponent = new MinioSenderAcquisitionComponent("MinioSender", frameSource);

                _minioHandle = await _minioComponent.StartAcquisitionAsync(
                    cfg,
                    null,
                    ct);

                var modeLabel = (cfg.Mode == "DAQ") ? "DAQ" : "Stub";
                SetStatus(string.Format("{0} + Kafka + MinIO 동작 중...", modeLabel));
                AppEvents.RaiseLog("[UI] 모든 컴포넌트 시작 완료");
            }
            catch (Exception ex)
            {
                UpdateRunningState(false);
                SetStatus("시작 실패: " + ex.Message);
                AppEvents.RaiseLog("[UI] 시작 실패: " + ex);
            }
        }

        private MeasurementSessionInfo BuildSessionInfo(DaqAccelConfig cfg, double sampleRateHz)
        {
            int channelCount = 0;
            if (!string.IsNullOrWhiteSpace(cfg.Daq?.Channels))
            {
                var txt = cfg.Daq.Channels.Trim();
                if (txt.Contains(":"))
                {
                    var normalized = txt.ToLower();
                    var parts = normalized.Replace("ai", "").Split(':');
                    if (parts.Length == 2 &&
                        int.TryParse(parts[0], out int start) &&
                        int.TryParse(parts[1], out int end) &&
                        end >= start)
                    {
                        channelCount = (end - start + 1);
                    }
                }
                else if (txt.Contains(","))
                {
                    channelCount = txt.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries).Length;
                }
                else
                {
                    channelCount = 1;
                }
            }

            if (channelCount <= 0)
                channelCount = 1;

            var nowLocal = DateTime.Now;
            var nowUtc = DateTime.UtcNow;

            var autoName = string.Format(
                "{0}_{1}_{2}_{3:yyyyMMdd_HHmmss}",
                cfg.Kafka?.MachineId ?? "Unknown",
                cfg.Meta?.TaskType ?? "task",
                cfg.Meta?.LabelType ?? "label",
                nowLocal
            );

            var session = new MeasurementSessionInfo
            {
                DeviceId = cfg.Kafka?.MachineId ?? "Unknown",
                Name = autoName,
                StartedAtUtc = nowUtc,
                SampleRateHz = sampleRateHz,
                ChannelCount = channelCount,
                DeviceName = cfg.Daq?.Device,
                Location = cfg.Kafka?.MachineId,
                RawFilePath = null,
                Comment = cfg.Meta?.Note,

                TaskType = cfg.Meta?.TaskType,
                LabelType = cfg.Meta?.LabelType,
                DataSplit = cfg.Meta?.DataSplit,
                Operator = cfg.Meta?.Operator,
                Note = cfg.Meta?.Note,

                // ★ frame / event 구분 저장
                AcquisitionUnit = cfg.Daq?.AcquisitionUnit ?? "frame"
            };

            return session;
        }

        private static double ComputeRms(double[] a)
        {
            if (a == null || a.Length == 0) return 0;
            double s = 0;
            for (int i = 0; i < a.Length; i++)
                s += a[i] * a[i];
            return Math.Sqrt(s / a.Length);
        }

        private static double ComputePeak(double[] a)
        {
            if (a == null || a.Length == 0) return 0;
            double maxAbs = 0;
            for (int i = 0; i < a.Length; i++)
            {
                double v = Math.Abs(a[i]);
                if (v > maxAbs) maxAbs = v;
            }
            return maxAbs;
        }

        private static double ComputeMeanAbs(double[] a)
        {
            if (a == null || a.Length == 0) return 0;
            double s = 0;
            for (int i = 0; i < a.Length; i++)
                s += Math.Abs(a[i]);
            return s / a.Length;
        }

        private static double ComputeStd(double[] a)
        {
            if (a == null || a.Length == 0) return 0;
            double mean = 0;
            for (int i = 0; i < a.Length; i++)
                mean += a[i];
            mean /= a.Length;

            double s2 = 0;
            for (int i = 0; i < a.Length; i++)
            {
                double d = a[i] - mean;
                s2 += d * d;
            }
            return Math.Sqrt(s2 / a.Length);
        }

        private static double ComputeCrestFactor(double[] a)
        {
            double rms = ComputeRms(a);
            double peak = ComputePeak(a);
            if (rms <= 1e-12) return 0;
            return peak / rms;
        }

        /// <summary>
        /// 한 프레임에 대해 실시간 특징 계산 + 이상 탐지 + (필요 시) 결함 진단까지 수행.
        /// Stub/DAQ 공통으로 사용.
        /// </summary>
        private void ProcessRealtimeFrame(AccelFrame frame, string taskTypeLocal)
        {
            var rmsX = ComputeRms(frame.Ax);
            var rmsY = ComputeRms(frame.Ay);
            var rmsZ = ComputeRms(frame.Az);

            var peakX = ComputePeak(frame.Ax);
            var peakY = ComputePeak(frame.Ay);
            var peakZ = ComputePeak(frame.Az);

            var meanAbsX = ComputeMeanAbs(frame.Ax);
            var meanAbsY = ComputeMeanAbs(frame.Ay);
            var meanAbsZ = ComputeMeanAbs(frame.Az);

            var stdX = ComputeStd(frame.Ax);
            var stdY = ComputeStd(frame.Ay);
            var stdZ = ComputeStd(frame.Az);

            var crestX = ComputeCrestFactor(frame.Ax);
            var crestY = ComputeCrestFactor(frame.Ay);
            var crestZ = ComputeCrestFactor(frame.Az);

            double maxRms = Math.Max(rmsX, Math.Max(rmsY, rmsZ));

            // Rule 기반 1차 이상 여부
            bool isAnomaly = maxRms > 5.0;

            string anomalyLabel = null;
            float anomalyScore = 0.0f;

            // --- 이상 탐지 모델 (15차원 특징) ---
            try
            {
                if (_enableRealtimeInference && _anomalyModel != null)
                {
                    var features = new float[]
                    {
                        // rms_x, rms_y, rms_z
                        (float)rmsX, (float)rmsY, (float)rmsZ,
                        // peak_x, peak_y, peak_z
                        (float)peakX, (float)peakY, (float)peakZ,
                        // mean_abs_x, mean_abs_y, mean_abs_z
                        (float)meanAbsX, (float)meanAbsY, (float)meanAbsZ,
                        // std_x, std_y, std_z
                        (float)stdX, (float)stdY, (float)stdZ,
                        // crest_factor_x, crest_factor_y, crest_factor_z
                        (float)crestX, (float)crestY, (float)crestZ,
                    };

                    (anomalyLabel, anomalyScore) = _anomalyModel.Predict(features);

                    if (!string.IsNullOrEmpty(anomalyLabel))
                    {
                        bool isModelAnomaly =
                            !string.Equals(anomalyLabel, "normal", StringComparison.OrdinalIgnoreCase);

                        isAnomaly = isAnomaly || isModelAnomaly;
                    }
                }
            }
            catch (Exception ex)
            {
                AppEvents.RaiseLog("[UI] Anomaly ONNX inference failed: " + ex);
            }

            // --- 결함 진단 2단계 (필요할 때만) ---
            string faultLabel = null;
            float faultScore = 0.0f;

            bool needFaultStage =
                isAnomaly &&
                !string.IsNullOrEmpty(taskTypeLocal) &&
                taskTypeLocal.StartsWith("fault", StringComparison.OrdinalIgnoreCase);

            if (_enableRealtimeInference && needFaultStage && _faultModel != null)
            {
                try
                {
                    var features = new float[]
                    {
                        (float)rmsX, (float)rmsY, (float)rmsZ,
                        (float)peakX, (float)peakY, (float)peakZ,
                        (float)meanAbsX, (float)meanAbsY, (float)meanAbsZ,
                        (float)stdX, (float)stdY, (float)stdZ,
                        (float)crestX, (float)crestY, (float)crestZ,
                    };

                    (faultLabel, faultScore) = _faultModel.Predict(features);
                }
                catch (Exception ex)
                {
                    AppEvents.RaiseLog("[UI] Fault ONNX inference failed: " + ex);
                }
            }

            // --- 최종 라벨 결정 ---
            string finalLabel;
            float finalScore;

            if (!isAnomaly)
            {
                finalLabel = "normal";
                finalScore = anomalyScore;
            }
            else
            {
                if (!string.IsNullOrEmpty(faultLabel))
                {
                    finalLabel = faultLabel;
                    finalScore = faultScore;
                }
                else if (!string.IsNullOrEmpty(anomalyLabel))
                {
                    finalLabel = anomalyLabel;
                    finalScore = anomalyScore;
                }
                else
                {
                    finalLabel = "anomaly";
                    finalScore = 1.0f;
                }
            }

            var result = new RealtimeResult
            {
                Timestamp = DateTime.Now,
                RmsX = rmsX,
                RmsY = rmsY,
                RmsZ = rmsZ,
                IsAnomaly = isAnomaly,
                PredictedLabel = finalLabel,
                ModelScore = finalScore,
                Note = string.Format(
                    "seq={0}, maxRMS={1:F3}, anomaly=({2}, {3:F3}), fault=({4}, {5:F3})",
                    frame.Seq,
                    maxRms,
                    anomalyLabel ?? "-",
                    anomalyScore,
                    faultLabel ?? "-",
                    faultScore
                )
            };

            var n = Interlocked.Increment(ref _frameCountForLog);
            if (n % 10 == 0) // 10프레임마다
            {
                AppEvents.RaiseLog(string.Format(
                    "[Frame] seq={0}, RMS=({1:F3}, {2:F3}, {3:F3}), anomaly=({4}, {5:F3}), fault=({6}, {7:F3}), final=({8}, {9:F3})",
                    frame.Seq,
                    rmsX, rmsY, rmsZ,
                    anomalyLabel ?? "-", anomalyScore,
                    faultLabel ?? "-", faultScore,
                    finalLabel, finalScore
                ));
            }
        }

        /// <summary>
        /// 이벤트 단위 (StartSampleIndex~EndSampleIndex 구간)에 대해
        /// 3축 신호로 15개 특징 계산 + 이상 탐지 + (필요 시) 결함 진단.
        /// </summary>
        private void ProcessEventForInference(AccelEvent ev, string taskTypeLocal)
        {
            var ax = ev.Ax ?? Array.Empty<double>();
            var ay = ev.Ay ?? Array.Empty<double>();
            var az = ev.Az ?? Array.Empty<double>();

            if (ax.Length == 0 && ay.Length == 0 && az.Length == 0)
            {
                AppEvents.RaiseLog("[UI] [Event] AccelEvent에 raw 샘플이 없어 이벤트 단위 추론을 건너뜁니다.");
                return;
            }

            // ---- 1) 축별 특징 계산 (이벤트 전체 구간) ----
            var rmsX = ComputeRms(ax);
            var rmsY = ComputeRms(ay);
            var rmsZ = ComputeRms(az);

            var peakX = ComputePeak(ax);
            var peakY = ComputePeak(ay);
            var peakZ = ComputePeak(az);

            var meanAbsX = ComputeMeanAbs(ax);
            var meanAbsY = ComputeMeanAbs(ay);
            var meanAbsZ = ComputeMeanAbs(az);

            var stdX = ComputeStd(ax);
            var stdY = ComputeStd(ay);
            var stdZ = ComputeStd(az);

            var crestX = ComputeCrestFactor(ax);
            var crestY = ComputeCrestFactor(ay);
            var crestZ = ComputeCrestFactor(az);

            double maxRms = Math.Max(rmsX, Math.Max(rmsY, rmsZ));
            bool isAnomaly = maxRms > 5.0;   // rule-based 1차 이상 여부

            string anomalyLabel = null;
            float anomalyScore = 0.0f;

            // ---- 2) 이상 탐지 모델 (15차원 특징) ----
            try
            {
                if (_enableRealtimeInference && _anomalyModel != null)
                {
                    var features = new float[]
                    {
                        // rms_x, rms_y, rms_z
                        (float)rmsX, (float)rmsY, (float)rmsZ,
                        // peak_x, peak_y, peak_z
                        (float)peakX, (float)peakY, (float)peakZ,
                        // mean_abs_x, mean_abs_y, mean_abs_z
                        (float)meanAbsX, (float)meanAbsY, (float)meanAbsZ,
                        // std_x, std_y, std_z
                        (float)stdX, (float)stdY, (float)stdZ,
                        // crest_factor_x, crest_factor_y, crest_factor_z
                        (float)crestX, (float)crestY, (float)crestZ,
                    };

                    (anomalyLabel, anomalyScore) = _anomalyModel.Predict(features);

                    if (!string.IsNullOrEmpty(anomalyLabel))
                    {
                        bool isModelAnomaly =
                            !string.Equals(anomalyLabel, "normal", StringComparison.OrdinalIgnoreCase);

                        isAnomaly = isAnomaly || isModelAnomaly;
                    }
                }
            }
            catch (Exception ex)
            {
                AppEvents.RaiseLog("[UI] [Event] Anomaly ONNX inference failed: " + ex);
            }

            // ---- 3) 결함 진단 (이상일 때만) ----
            string faultLabel = null;
            float faultScore = 0.0f;

            bool needFaultStage =
                isAnomaly &&
                !string.IsNullOrEmpty(taskTypeLocal) &&
                taskTypeLocal.StartsWith("fault", StringComparison.OrdinalIgnoreCase);

            if (needFaultStage && _faultModel != null)
            {
                try
                {
                    var features = new float[]
                    {
                        (float)rmsX, (float)rmsY, (float)rmsZ,
                        (float)peakX, (float)peakY, (float)peakZ,
                        (float)meanAbsX, (float)meanAbsY, (float)meanAbsZ,
                        (float)stdX, (float)stdY, (float)stdZ,
                        (float)crestX, (float)crestY, (float)crestZ,
                    };

                    (faultLabel, faultScore) = _faultModel.Predict(features);
                }
                catch (Exception ex)
                {
                    AppEvents.RaiseLog("[UI] [Event] Fault ONNX inference failed: " + ex);
                }
            }

            // ---- 4) 최종 라벨 결정 ----
            string finalLabel;
            float finalScore;

            if (!isAnomaly)
            {
                finalLabel = "normal";
                finalScore = anomalyScore;
            }
            else
            {
                if (!string.IsNullOrEmpty(faultLabel))
                {
                    finalLabel = faultLabel;
                    finalScore = faultScore;
                }
                else if (!string.IsNullOrEmpty(anomalyLabel))
                {
                    finalLabel = anomalyLabel;
                    finalScore = anomalyScore;
                }
                else
                {
                    finalLabel = "anomaly";
                    finalScore = 1.0f;
                }
            }

            // ---- 5) 이벤트 단위 결과를 UI에 반영 ----
            var result = new RealtimeResult
            {
                Timestamp = DateTime.Now,
                RmsX = rmsX,
                RmsY = rmsY,
                RmsZ = rmsZ,
                IsAnomaly = isAnomaly,
                PredictedLabel = finalLabel,
                ModelScore = finalScore,
                Note = string.Format(
                    "event={0}, samples={1}~{2}, maxRMS={3:F3}, anomaly=({4}, {5:F3}), fault=({6}, {7:F3})",
                    ev.EventIndex,
                    ev.StartSampleIndex,
                    ev.EndSampleIndex,
                    maxRms,
                    anomalyLabel ?? "-",
                    anomalyScore,
                    faultLabel ?? "-",
                    faultScore
                )
            };

            AppEvents.RaiseRealtimeResultUpdated(result);

            AppEvents.RaiseLog(string.Format(
                "[Event] idx={0}, samples={1}~{2}, RMS=({3:F3},{4:F3},{5:F3}), anomaly=({6},{7:F3}), fault=({8},{9:F3}), final=({10},{11:F3})",
                ev.EventIndex,
                ev.StartSampleIndex,
                ev.EndSampleIndex,
                rmsX, rmsY, rmsZ,
                anomalyLabel ?? "-", anomalyScore,
                faultLabel ?? "-", faultScore,
                finalLabel, finalScore
            ));
        }

        private async void BtnStop_Click(object sender, EventArgs e)
        {
            try
            {
                if ((_daqHandle == null || _daqHandle.IsFinished) &&
                    (_kafkaHandle == null || _kafkaHandle.IsFinished) &&
                    (_minioHandle == null || _minioHandle.IsFinished))
                {
                    SetStatus("중단할 세션이 없습니다.");
                    AppEvents.RaiseLog("[UI] Stop 요청 - 실행 중인 세션 없음");
                    return;
                }

                SetStatus("중단 중...");
                AppEvents.RaiseLog("[UI] Stop 클릭 - 수집/송신 중단 요청");

                try
                {
                    if (_inferenceCts != null && !_inferenceCts.IsCancellationRequested)
                        _inferenceCts.Cancel();

                    if (_inferenceQueue != null && !_inferenceQueue.IsAddingCompleted)
                        _inferenceQueue.CompleteAdding();

                    if (_inferenceWorker != null)
                        await _inferenceWorker;
                }
                catch (Exception ex)
                {
                    AppEvents.RaiseLog("[UI] inference worker 중단 실패: " + ex);
                }
                finally
                {
                    _inferenceWorker = null;
                    _inferenceQueue = null;
                    _inferenceCts = null;
                }

                if (_acquisitionCts != null && !_acquisitionCts.IsCancellationRequested)
                    _acquisitionCts.Cancel();

                if (_daqHandle != null && !_daqHandle.IsFinished)
                    await _daqHandle.StopAsync();

                if (_kafkaHandle != null && !_kafkaHandle.IsFinished)
                    await _kafkaHandle.StopAsync();

                if (_minioHandle != null)
                    await _minioHandle.StopAsync();

                // ★ 마지막에 열려 있는 이벤트가 있으면 Flush
                try
                {
                    if (_eventDetector != null)
                    {
                        _eventDetector.Flush();
                    }
                }
                catch (Exception ex)
                {
                    AppEvents.RaiseLog("[UI] EventDetector Flush 실패: " + ex);
                }

                try
                {
                    var rawInfo = _minioComponent as MinioSenderAcquisitionComponent;

                    if (rawInfo != null &&
                        rawInfo.HasSuccessfulUpload &&
                        !string.IsNullOrEmpty(rawInfo.SessionRawPath))
                    {
                        _pgSink?.UpdateRawFilePath(rawInfo.SessionRawPath);
                        AppEvents.RaiseLog("[UI] DB raw_file_path 업데이트: " + rawInfo.SessionRawPath);
                    }
                    else
                    {
                        AppEvents.RaiseLog("[UI] MinIO 업로드 성공 기록 없음 → raw_file_path 업데이트 생략");
                    }
                }
                catch (Exception ex)
                {
                    AppEvents.RaiseLog("[UI] raw_file_path 업데이트 실패: " + ex);
                }

                UpdateRunningState(false);
                SetStatus("중단 완료.");

                _frameSource = null;
                AppEvents.RaiseFrameSourceChanged(null);

                try
                {
                    _pgSink?.OnSessionStopped();
                    AppEvents.RaiseLog("[UI] DB 세션 종료 기록 완료");
                }
                catch (Exception dbEx)
                {
                    AppEvents.RaiseLog("[UI] DB 세션 종료 기록 실패: " + dbEx);
                }

                _currentSessionId = -1;
                _eventDetector = null;

                AppEvents.RaiseLog("[UI] 모든 컴포넌트 중단 완료");
            }
            catch (Exception ex)
            {
                UpdateRunningState(false);
                SetStatus("중단 실패: " + ex.Message);
                AppEvents.RaiseLog("[UI] 중단 실패: " + ex);
            }
        }

        public DaqAccelConfig GetConfig()
        {
            var cfg = new DaqAccelConfig();

            // Mode
            cfg.Mode = rdoModeDaq.Checked ? "DAQ" : "Stub";

            // DAQ
            cfg.Daq.Device = cboDevice.Text;
            cfg.Daq.Channels = txtChannels.Text;
            cfg.Daq.SamplingRate = (int)numSamplingRate.Value;
            cfg.Daq.FrameSize = (int)numFrameSize.Value;

            cfg.Daq.SensitivityX_mVpg = (double)numSensX.Value;
            cfg.Daq.SensitivityY_mVpg = (double)numSensY.Value;
            cfg.Daq.SensitivityZ_mVpg = (double)numSensZ.Value;

            double iepe;
            if (double.TryParse(cboIepeCurrent.Text, out iepe))
                cfg.Daq.IepeCurrent_mA = iepe;
            else
                cfg.Daq.IepeCurrent_mA = 4.0;

            cfg.Daq.MinG = (double)numMinG.Value;
            cfg.Daq.MaxG = (double)numMaxG.Value;

            cfg.Daq.AcquisitionUnit = rdoUnitEvent.Checked ? "event" : "frame";
            cfg.Daq.EventRmsThreshold = (double)numEventRmsThreshold.Value;
            cfg.Daq.EventMinGapMs = (int)numEventMinGapMs.Value;

            // Kafka
            cfg.Kafka.Bootstrap = txtBootstrap.Text;
            cfg.Kafka.Topic = txtTopic.Text;
            cfg.Kafka.MachineId = txtMachineId.Text;

            // MinIO
            cfg.Minio.Endpoint = txtMinioEndpoint.Text;
            cfg.Minio.AccessKey = txtAccessKey.Text;
            cfg.Minio.SecretKey = txtSecretKey.Text;
            cfg.Minio.Bucket = txtBucket.Text;
            cfg.Minio.SaveType = cboSaveType.Text;
            cfg.Minio.SaveIntervalMinutes = (double)numSaveInterval.Value;
            cfg.Minio.EnableRawSave = chkEnableRawSave.Checked;

            // Meta
            cfg.Meta.TaskType = cboTaskType.Text;
            cfg.Meta.LabelType = cboLabelType.Text;
            cfg.Meta.DataSplit = cboSplit.Text;
            cfg.Meta.Operator = txtOperator.Text;
            cfg.Meta.Note = txtNote != null ? txtNote.Text : null;

            return cfg;
        }

        private void ApplyConfig(DaqAccelConfig cfg)
        {
            if (cfg == null) return;

            // Mode
            if (cfg.Mode == "DAQ")
                rdoModeDaq.Checked = true;
            else
                rdoModeStub.Checked = true;

            // DAQ
            if (cfg.Daq != null)
            {
                if (!string.IsNullOrEmpty(cfg.Daq.Device))
                    cboDevice.Text = cfg.Daq.Device;

                txtChannels.Text = cfg.Daq.Channels ?? txtChannels.Text;

                if (cfg.Daq.SamplingRate > 0)
                    numSamplingRate.Value = ClampNumeric(numSamplingRate, cfg.Daq.SamplingRate);

                if (cfg.Daq.FrameSize > 0)
                    numFrameSize.Value = ClampNumeric(numFrameSize, cfg.Daq.FrameSize);

                if (!string.IsNullOrEmpty(cfg.Daq.AcquisitionUnit))
                {
                    if (string.Equals(cfg.Daq.AcquisitionUnit, "event", StringComparison.OrdinalIgnoreCase))
                        rdoUnitEvent.Checked = true;
                    else
                        rdoUnitFrame.Checked = true;
                }

                if (cfg.Daq.EventRmsThreshold > 0)
                {
                    var v = (decimal)cfg.Daq.EventRmsThreshold;
                    if (v < numEventRmsThreshold.Minimum) v = numEventRmsThreshold.Minimum;
                    if (v > numEventRmsThreshold.Maximum) v = numEventRmsThreshold.Maximum;
                    numEventRmsThreshold.Value = v;
                }

                if (cfg.Daq.EventMinGapMs > 0)
                {
                    var v = (decimal)cfg.Daq.EventMinGapMs;
                    if (v < numEventMinGapMs.Minimum) v = numEventMinGapMs.Minimum;
                    if (v > numEventMinGapMs.Maximum) v = numEventMinGapMs.Maximum;
                    numEventMinGapMs.Value = v;
                }

                if (cfg.Daq.SensitivityX_mVpg > 0)
                    numSensX.Value = ClampNumeric(numSensX, (int)Math.Round(cfg.Daq.SensitivityX_mVpg));

                if (cfg.Daq.SensitivityY_mVpg > 0)
                    numSensY.Value = ClampNumeric(numSensY, (int)Math.Round(cfg.Daq.SensitivityY_mVpg));

                if (cfg.Daq.SensitivityZ_mVpg > 0)
                    numSensZ.Value = ClampNumeric(numSensZ, (int)Math.Round(cfg.Daq.SensitivityZ_mVpg));

                if (cfg.Daq.IepeCurrent_mA > 0)
                {
                    var target = cfg.Daq.IepeCurrent_mA.ToString("0");
                    int idx = cboIepeCurrent.Items.IndexOf(target);
                    if (idx >= 0)
                        cboIepeCurrent.SelectedIndex = idx;
                }

                if (cfg.Daq.MinG != 0)
                {
                    var minG = (decimal)cfg.Daq.MinG;
                    if (minG < numMinG.Minimum)
                        numMinG.Value = numMinG.Minimum;
                    else if (minG > numMinG.Maximum)
                        numMinG.Value = numMinG.Maximum;
                    else
                        numMinG.Value = minG;
                }

                if (cfg.Daq.MaxG != 0)
                {
                    var maxG = (decimal)cfg.Daq.MaxG;
                    if (maxG < numMaxG.Minimum)
                        numMaxG.Value = numMaxG.Minimum;
                    else if (maxG > numMaxG.Maximum)
                        numMaxG.Value = numMaxG.Maximum;
                    else
                        numMaxG.Value = maxG;
                }
            }

            // Kafka
            if (cfg.Kafka != null)
            {
                txtBootstrap.Text = cfg.Kafka.Bootstrap ?? txtBootstrap.Text;
                txtTopic.Text = cfg.Kafka.Topic ?? txtTopic.Text;
                txtMachineId.Text = cfg.Kafka.MachineId ?? txtMachineId.Text;
            }

            // MinIO
            if (cfg.Minio != null)
            {
                txtMinioEndpoint.Text = cfg.Minio.Endpoint ?? txtMinioEndpoint.Text;
                txtAccessKey.Text = cfg.Minio.AccessKey ?? txtAccessKey.Text;
                txtSecretKey.Text = cfg.Minio.SecretKey ?? txtSecretKey.Text;
                txtBucket.Text = cfg.Minio.Bucket ?? txtBucket.Text;

                if (!string.IsNullOrEmpty(cfg.Minio.SaveType))
                    cboSaveType.Text = cfg.Minio.SaveType;

                if (cfg.Minio.SaveIntervalMinutes > 0)
                {
                    var v = (decimal)cfg.Minio.SaveIntervalMinutes;
                    if (v < numSaveInterval.Minimum)
                        v = numSaveInterval.Minimum;
                    else if (v > numSaveInterval.Maximum)
                        v = numSaveInterval.Maximum;
                    numSaveInterval.Value = v;
                }

                chkEnableRawSave.Checked = cfg.Minio.EnableRawSave;
            }

            // Meta
            if (cfg.Meta != null)
            {
                if (!string.IsNullOrEmpty(cfg.Meta.TaskType))
                    cboTaskType.Text = cfg.Meta.TaskType;

                if (!string.IsNullOrEmpty(cfg.Meta.LabelType))
                    cboLabelType.Text = cfg.Meta.LabelType;

                if (!string.IsNullOrEmpty(cfg.Meta.DataSplit))
                    cboSplit.Text = cfg.Meta.DataSplit;

                if (!string.IsNullOrEmpty(cfg.Meta.Operator))
                    txtOperator.Text = cfg.Meta.Operator;

                if (txtNote != null && cfg.Meta.Note != null)
                    txtNote.Text = cfg.Meta.Note;
            }
        }

        private decimal ClampNumeric(NumericUpDown control, int value)
        {
            if (value < control.Minimum) return control.Minimum;
            if (value > control.Maximum) return control.Maximum;
            return value;
        }

        private string GetConfigFilePath()
        {
            var baseDir = AppDomain.CurrentDomain.BaseDirectory;
            return Path.Combine(baseDir, ConfigFileName);
        }

        public void SaveConfigSafe()
        {
            try
            {
                var cfg = GetConfig();
                var path = GetConfigFilePath();

                var serializer = new XmlSerializer(typeof(DaqAccelConfig));
                using (var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None))
                {
                    serializer.Serialize(fs, cfg);
                }
            }
            catch { }
        }

        private void LoadConfigSafe()
        {
            try
            {
                var path = GetConfigFilePath();
                if (!File.Exists(path))
                    return;

                var serializer = new XmlSerializer(typeof(DaqAccelConfig));
                using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    var cfg = serializer.Deserialize(fs) as DaqAccelConfig;
                    ApplyConfig(cfg);
                }
            }
            catch
            {
                // 필요하면 로그
            }
        }

        protected override void OnFormClosing(FormClosingEventArgs e)
        {
            try
            {
                try
                {
                    if (_inferenceCts != null && !_inferenceCts.IsCancellationRequested)
                        _inferenceCts.Cancel();

                    if (_inferenceQueue != null && !_inferenceQueue.IsAddingCompleted)
                        _inferenceQueue.CompleteAdding();

                    _inferenceWorker?.Wait(500);
                }
                catch { }

                if (_acquisitionCts != null && !_acquisitionCts.IsCancellationRequested)
                    _acquisitionCts.Cancel();

                if (_daqHandle != null && !_daqHandle.IsFinished)
                    _daqHandle.StopAsync().Wait(500);

                if (_kafkaHandle != null && !_kafkaHandle.IsFinished)
                    _kafkaHandle.StopAsync().Wait(500);

                if (_minioHandle != null && !_minioHandle.IsFinished)
                    _minioHandle.StopAsync().Wait(500);
            }
            catch
            {
                // 무시
            }

            SaveConfigSafe();
            base.OnFormClosing(e);
        }
    }
}
