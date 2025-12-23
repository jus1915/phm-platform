using System;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Windows.Forms;
using System.Xml;
using WeifenLuo.WinFormsUI.Docking;
using phm_data_pipeline.Services;
using phm_data_pipeline.UI;

namespace phm_data_pipeline
{
    public partial class MainForm : Form
    {
        #region Constants

        private const string LayoutFileName = "layout.xml";

        private const int MinFormWidth = 200;
        private const int MinFormHeight = 150;

        private const int DefaultFormWidth = 1200;
        private const int DefaultFormHeight = 800;

        // 화면 안에 있다고 판단할 최소 가시 영역
        private const int MinVisibleWidth = 50;
        private const int MinVisibleHeight = 50;

        #endregion

        #region Fields

        private readonly DockPanel _dockPanel;
        private readonly VS2015BlueTheme _theme = new VS2015BlueTheme();

        private LogGraphForm _logGraph;
        private LogWriterForm _logWriter;
        private DaqAccelSenderForm _daqAccelSender;
        private MlflowMonitorForm _mlflowMonitor;
        private AirflowMonitorForm _airflowMonitor;
        private SessionManagerForm _trainControl;
        private RealtimeResultForm _realtimeResult;

        // 어떤 DockContent(Type)가 어떤 메뉴에 매핑되는지 관리
        private readonly Dictionary<Type, ToolStripMenuItem> _dockMenuMap =
            new Dictionary<Type, ToolStripMenuItem>();

        #endregion

        public MainForm()
        {
            InitializeComponent();

            _dockPanel = CreateDockPanel();
            Controls.Add(_dockPanel);

            InitializeMenu();
            WireEvents();
        }

        #region Initialization

        private DockPanel CreateDockPanel()
        {
            var panel = new DockPanel
            {
                Dock = DockStyle.Fill,
                Theme = _theme
            };
            return panel;
        }

        private void InitializeMenu()
        {
            var menuStrip = new MenuStrip();

            // 상위: 데이터 수집
            var menuDataCollection = new ToolStripMenuItem("데이터 수집");

            // └ 가속도
            var menuAccel = new ToolStripMenuItem("가속도");
            menuAccel.DropDownItems.Add(
                CreateDockMenuItem<DaqAccelSenderForm>(
                    "가속도 DAQ 송신기",      // 메뉴에 보일 텍스트
                    DockState.Document));     // 기본 도킹 위치 (문서 영역)

            // └ 로그 관리
            var menuLog = new ToolStripMenuItem("로그 관리");
            menuLog.DropDownItems.Add(CreateDockMenuItem<LogWriterForm>("Log Writer", DockState.DockBottom));
            menuLog.DropDownItems.Add(CreateDockMenuItem<LogGraphForm>("Log Graph", DockState.Document));

            // └ 모델 모니터링 (MLflow)
            var menuModel = new ToolStripMenuItem("모델 모니터링");
            menuModel.DropDownItems.Add(
                CreateDockMenuItem<MlflowMonitorForm>(
                    "MLflow Monitor",
                    DockState.DockRight));
            menuModel.DropDownItems.Add(
                CreateDockMenuItem<AirflowMonitorForm>(
                    "Airflow Monitor",
                    DockState.DockRight));
            menuModel.DropDownItems.Add(
                CreateDockMenuItem<RealtimeResultForm>(
                    "Reatime Results",
                    DockState.DockRight));
            menuModel.DropDownItems.Add(
            CreateDockMenuItem<SessionManagerForm>(
                    "Session Manager", 
                    DockState.DockRight));

            // └ 환경 설정 (임시 더미 항목)
            var menuConfig = new ToolStripMenuItem("환경 설정");
            menuConfig.DropDownItems.Add(new ToolStripMenuItem("축 설정 관리", null, (s, e) => { /* TODO */ }));
            menuConfig.DropDownItems.Add(new ToolStripMenuItem("연결 설정", null, (s, e) => { /* TODO */ }));

            menuDataCollection.DropDownItems.Add(menuAccel);
            menuDataCollection.DropDownItems.Add(menuLog);
            menuDataCollection.DropDownItems.Add(menuModel);   // ★ 모델 모니터링 메뉴 추가
            menuDataCollection.DropDownItems.Add(menuConfig);

            // 종료
            var menuExit = new ToolStripMenuItem("프로그램 종료", null, (s, e) => Close());

            menuStrip.Items.Add(menuDataCollection);
            menuStrip.Items.Add(menuExit);

            MainMenuStrip = menuStrip;
            Controls.Add(menuStrip);
        }

        private void WireEvents()
        {
            Load += MainForm_Load;
            FormClosing += MainForm_FormClosing;
        }

        #endregion

        #region Event Handlers

        private void MainForm_Load(object sender, EventArgs e)
        {
            try
            {
                if (File.Exists(LayoutFileName))
                {
                    var doc = new XmlDocument();
                    doc.Load(LayoutFileName);

                    RestoreWindowBounds(doc);
                    RestoreDockLayout(doc);
                }
                else
                {
                    OpenDefaultLayout();
                }

                SyncMenuCheckedState();
            }
            catch (Exception ex)
            {
                AppEvents.RaiseLog("[로드 실패] 레이아웃 로드 중 오류: " + ex.Message);
                // 문제 발생 시 기본 레이아웃으로 안전하게 복구
                OpenDefaultLayout();
                SyncMenuCheckedState();
            }
        }

        private void MainForm_FormClosing(object sender, FormClosingEventArgs e)
        {
            try
            {
                // 🔍 DockPanel에서 DaqAccelSenderForm 인스턴스 찾아 저장
                foreach (var content in _dockPanel.Contents)
                {
                    var accelForm = content as DaqAccelSenderForm;
                    if (accelForm != null)
                    {
                        accelForm.SaveConfigSafe();   // ← 설정 저장
                    }
                }
            }
            catch { }

            try
            {
                var doc = new XmlDocument();
                var root = doc.CreateElement("Layout");
                doc.AppendChild(root);

                SaveWindowBounds(root);
                SaveDockLayout(doc, root);

                doc.Save(LayoutFileName);
            }
            catch (Exception ex)
            {
                AppEvents.RaiseLog("[저장 실패] 레이아웃 저장 중 오류: " + ex.Message);
            }

            // 필요하다면 여기서 IDisposable 자원 정리
            // try { _daq?.Dispose(); } catch { }
            // try { _httpSender?.StopStreaming(); _httpSender?.Dispose(); } catch { }
        }

        #endregion

        #region Layout - Window Bounds

        private void RestoreWindowBounds(XmlDocument doc)
        {
            var root = doc.SelectSingleNode("Layout") as XmlElement;
            if (root == null)
                return;

            int x, y, width, height;
            if (!TryParseAttribute(root, "X", out x) ||
                !TryParseAttribute(root, "Y", out y) ||
                !TryParseAttribute(root, "Width", out width) ||
                !TryParseAttribute(root, "Height", out height))
            {
                // 값이 이상하면 그냥 기본값 사용
                ApplyDefaultBounds();
                return;
            }

            var stateAttr = root.GetAttribute("State");
            FormWindowState savedState;
            if (!Enum.TryParse(stateAttr, out savedState))
            {
                savedState = FormWindowState.Normal;
            }

            StartPosition = FormStartPosition.Manual;
            Location = new Point(x, y);
            Size = new Size(
                Math.Max(MinFormWidth, width),
                Math.Max(MinFormHeight, height));
            WindowState = savedState;

            // 화면 밖이면 중앙으로 리셋
            if (!IsOnAnyScreen(new Rectangle(Location, Size)))
            {
                ApplyDefaultBounds();
            }
        }

        private void SaveWindowBounds(XmlElement root)
        {
            // 최소화 상태는 Normal로 저장 → 최소화된 상태로 시작 방지
            var saveState = WindowState == FormWindowState.Minimized
                ? FormWindowState.Normal
                : WindowState;

            root.SetAttribute("X", Location.X.ToString());
            root.SetAttribute("Y", Location.Y.ToString());
            root.SetAttribute("Width", Size.Width.ToString());
            root.SetAttribute("Height", Size.Height.ToString());
            root.SetAttribute("State", saveState.ToString());
        }

        private void ApplyDefaultBounds()
        {
            WindowState = FormWindowState.Normal;
            StartPosition = FormStartPosition.CenterScreen;
            Size = new Size(DefaultFormWidth, DefaultFormHeight);
            CenterToScreen();
        }

        private static bool TryParseAttribute(XmlElement element, string attrName, out int value)
        {
            value = 0;
            var attr = element.GetAttribute(attrName);
            return int.TryParse(attr, out value);
        }

        private static bool IsOnAnyScreen(Rectangle rect)
        {
            foreach (var screen in Screen.AllScreens)
            {
                var workingArea = screen.WorkingArea;
                var intersection = Rectangle.Intersect(workingArea, rect);

                if (intersection.Width >= MinVisibleWidth &&
                    intersection.Height >= MinVisibleHeight)
                {
                    return true;
                }
            }

            return false;
        }

        #endregion

        #region Layout - DockPanel

        private void RestoreDockLayout(XmlDocument doc)
        {
            var dockNode = doc.SelectSingleNode("Layout/DockPanel") as XmlElement;
            if (dockNode == null)
                return;

            // DockPanelSuite는 XML 파일 자체를 요구하므로 임시 파일 사용
            var tempFile = Path.GetTempFileName();
            try
            {
                File.WriteAllText(tempFile, dockNode.InnerXml);
                _dockPanel.LoadFromXml(tempFile, DeserializeDockContent);
            }
            catch (Exception ex)
            {
                AppEvents.RaiseLog("[복원 실패] DockPanel 로드 중 오류: " + ex.Message);
            }
            finally
            {
                if (File.Exists(tempFile))
                    File.Delete(tempFile);
            }
        }

        private void SaveDockLayout(XmlDocument doc, XmlElement root)
        {
            // DockPanel 상태를 임시 파일로 저장 후 XML 문자열만 추출
            var tempFile = Path.GetTempFileName();
            try
            {
                _dockPanel.SaveAsXml(tempFile);
                var dockXml = File.ReadAllText(tempFile);

                // XML 선언 제거
                if (dockXml.StartsWith("<?xml", StringComparison.Ordinal))
                {
                    var idx = dockXml.IndexOf("?>", StringComparison.Ordinal);
                    if (idx != -1 && idx + 2 < dockXml.Length)
                    {
                        dockXml = dockXml.Substring(idx + 2).Trim();
                    }
                }

                var dockNode = doc.CreateElement("DockPanel");
                dockNode.InnerXml = dockXml;
                root.AppendChild(dockNode);
            }
            finally
            {
                if (File.Exists(tempFile))
                    File.Delete(tempFile);
            }
        }

        private IDockContent DeserializeDockContent(string persistString)
        {
            // DockPanelSuite가 저장한 전체 타입명으로 구분
            if (persistString == typeof(LogGraphForm).ToString())
            {
                if (_logGraph == null)
                    _logGraph = new LogGraphForm();
                return _logGraph;
            }

            if (persistString == typeof(LogWriterForm).ToString())
            {
                if (_logWriter == null)
                    _logWriter = new LogWriterForm();
                return _logWriter;
            }

            if (persistString == typeof(DaqAccelSenderForm).ToString())
            {
                if (_daqAccelSender == null)
                    _daqAccelSender = new DaqAccelSenderForm();
                return _daqAccelSender;
            }

            if (persistString == typeof(MlflowMonitorForm).ToString())
            {
                if (_mlflowMonitor == null)
                    _mlflowMonitor = new MlflowMonitorForm();
                return _mlflowMonitor;
            }

            if (persistString == typeof(AirflowMonitorForm).ToString())
            {
                if (_airflowMonitor == null)
                    _airflowMonitor = new AirflowMonitorForm();
                return _airflowMonitor;
            }

            if (persistString == typeof(SessionManagerForm).ToString())
            {
                if (_trainControl == null)
                    _trainControl = new SessionManagerForm();
                return _trainControl;
            }

            if (persistString == typeof(RealtimeResultForm).ToString())
            {
                if (_realtimeResult == null)
                    _realtimeResult = new RealtimeResultForm();
                return _realtimeResult;

            }
            return null;
        }

        private void OpenDefaultLayout()
        {
            // 초기 실행 시 기본으로 띄울 창들
            if (_logGraph == null)
                _logGraph = new LogGraphForm();
            _logGraph.Show(_dockPanel, DockState.DockBottom);

            if (_logWriter == null)
                _logWriter = new LogWriterForm();
            _logWriter.Show(_dockPanel, DockState.Document);

            if (_daqAccelSender == null)
                _daqAccelSender = new DaqAccelSenderForm();
            _daqAccelSender.Show(_dockPanel, DockState.Document);

            // 필요하면 기본 레이아웃에서 MLflow 모니터도 자동으로 띄우고 싶을 때:
            // if (_mlflowMonitor == null)
            //     _mlflowMonitor = new MlflowMonitorForm();
            // _mlflowMonitor.Show(_dockPanel, DockState.DockRight);
        }

        #endregion

        #region Menu / DockContent Binding

        private ToolStripMenuItem CreateDockMenuItem<T>(string title, DockState defaultState)
            where T : DockContent, new()
        {
            return CreateDockMenuItem(title, defaultState, typeof(T), () => new T());
        }

        private ToolStripMenuItem CreateDockMenuItem(
            string title,
            DockState defaultState,
            Type formType,
            Func<DockContent> factory)
        {
            var menuItem = new ToolStripMenuItem(title)
            {
                CheckOnClick = true
            };

            menuItem.CheckedChanged += (sender, args) =>
            {
                if (menuItem.Checked)
                {
                    OpenDockContent(formType, factory, defaultState, menuItem);
                }
                else
                {
                    CloseDockContent(formType);
                }
            };

            _dockMenuMap[formType] = menuItem;

            // 처음 로드 시: 실제로 화면에 보이는 DockContent가 있으면 체크
            menuItem.Checked = _dockPanel.Contents
                .OfType<DockContent>()
                .Any(c => c.GetType() == formType && c.Visible && !c.IsDisposed);

            return menuItem;
        }

        private void OpenDockContent(
            Type formType,
            Func<DockContent> factory,
            DockState defaultState,
            ToolStripMenuItem menuItem)
        {
            // 이미 같은 타입의 창이 도킹 패널에 존재하는지 확인
            var existing = _dockPanel.Contents
                .OfType<DockContent>()
                .FirstOrDefault(c => c.GetType() == formType);

            if (existing == null || existing.IsDisposed)
            {
                var form = factory();

                // 🔴 여기서 꼭 false 로 (닫으면 실제 Close 되도록)
                form.HideOnClose = false;

                // 창이 완전히 닫힐 때 메뉴 체크 해제
                form.FormClosed += (s, e) => menuItem.Checked = false;

                form.Show(_dockPanel, defaultState);
            }
            else
            {
                existing.Show();
            }
        }

        private void CloseDockContent(Type formType)
        {
            var dockContent = _dockPanel.Contents
                .OfType<DockContent>()
                .FirstOrDefault(c => c.GetType() == formType);

            if (dockContent != null && !dockContent.IsDisposed)
            {
                dockContent.Close();   // HideOnClose=false 이므로 진짜 Close + FormClosed 호출
            }
        }

        private void SyncMenuCheckedState()
        {
            foreach (var kvp in _dockMenuMap)
            {
                var type = kvp.Key;
                var menuItem = kvp.Value;

                var isOpen = _dockPanel.Contents
                    .OfType<DockContent>()
                    .Any(c => c.GetType() == type && c.Visible && !c.IsDisposed);

                menuItem.Checked = isOpen;
            }
        }

        #endregion
    }
}
