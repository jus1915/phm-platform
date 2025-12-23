using System;
using System.Drawing;
using System.Windows.Forms;
using WeifenLuo.WinFormsUI.Docking;
using phm_data_pipeline.Models;
using phm_data_pipeline.Services;

namespace phm_data_pipeline.UI
{
    public class RealtimeResultForm : DockContent
    {
        private Label lblRmsX;
        private Label lblRmsY;
        private Label lblRmsZ;
        private Label lblStatus;
        private Label lblLastUpdate;
        private Panel pnlAnomalyIndicator;

        private Label lblModelLabel;
        private Label lblModelScore;

        public RealtimeResultForm()
        {
            InitializeComponent();

            // 이벤트 구독
            AppEvents.RealtimeResultUpdated += OnRealtimeResultUpdated;
        }

        protected override void OnFormClosing(FormClosingEventArgs e)
        {
            // DockForm 닫힐 때 이벤트 구독 해제
            AppEvents.RealtimeResultUpdated -= OnRealtimeResultUpdated;
            base.OnFormClosing(e);
        }

        private void InitializeComponent()
        {
            this.Text = "실시간 결과";
            this.Font = new Font("맑은 고딕", 9F);
            this.BackColor = Color.White;
            this.ClientSize = new Size(320, 230);

            var layout = new TableLayoutPanel();
            layout.Dock = DockStyle.Fill;
            layout.ColumnCount = 2;
            layout.RowCount = 7;
            layout.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));
            layout.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100F));
            for (int i = 0; i < 7; i++)
                layout.RowStyles.Add(new RowStyle(SizeType.AutoSize));

            int row = 0;

            // RMS X
            layout.Controls.Add(new Label { Text = "RMS X (g)", AutoSize = true, Anchor = AnchorStyles.Left }, 0, row);
            lblRmsX = new Label { Text = "-", AutoSize = true, Anchor = AnchorStyles.Left };
            layout.Controls.Add(lblRmsX, 1, row++);

            // RMS Y
            layout.Controls.Add(new Label { Text = "RMS Y (g)", AutoSize = true, Anchor = AnchorStyles.Left }, 0, row);
            lblRmsY = new Label { Text = "-", AutoSize = true, Anchor = AnchorStyles.Left };
            layout.Controls.Add(lblRmsY, 1, row++);

            // RMS Z
            layout.Controls.Add(new Label { Text = "RMS Z (g)", AutoSize = true, Anchor = AnchorStyles.Left }, 0, row);
            lblRmsZ = new Label { Text = "-", AutoSize = true, Anchor = AnchorStyles.Left };
            layout.Controls.Add(lblRmsZ, 1, row++);

            // Rule 상태
            layout.Controls.Add(new Label { Text = "Rule 상태", AutoSize = true, Anchor = AnchorStyles.Left }, 0, row);

            var statusPanel = new FlowLayoutPanel
            {
                FlowDirection = FlowDirection.LeftToRight,
                AutoSize = true,
                WrapContents = false
            };

            pnlAnomalyIndicator = new Panel
            {
                Width = 16,
                Height = 16,
                Margin = new Padding(0, 2, 8, 2),
                BackColor = Color.Gray
            };

            lblStatus = new Label
            {
                Text = "대기 중...",
                AutoSize = true,
                Anchor = AnchorStyles.Left
            };

            statusPanel.Controls.Add(pnlAnomalyIndicator);
            statusPanel.Controls.Add(lblStatus);

            layout.Controls.Add(statusPanel, 1, row++);

            // Model Label
            layout.Controls.Add(new Label { Text = "Model Label", AutoSize = true, Anchor = AnchorStyles.Left }, 0, row);
            lblModelLabel = new Label { Text = "-", AutoSize = true, Anchor = AnchorStyles.Left };
            layout.Controls.Add(lblModelLabel, 1, row++);

            // Model Score
            layout.Controls.Add(new Label { Text = "Model Score", AutoSize = true, Anchor = AnchorStyles.Left }, 0, row);
            lblModelScore = new Label { Text = "-", AutoSize = true, Anchor = AnchorStyles.Left };
            layout.Controls.Add(lblModelScore, 1, row++);

            // Last Update
            layout.Controls.Add(new Label { Text = "Last Update", AutoSize = true, Anchor = AnchorStyles.Left }, 0, row);
            lblLastUpdate = new Label { Text = "-", AutoSize = true, Anchor = AnchorStyles.Left };
            layout.Controls.Add(lblLastUpdate, 1, row++);

            this.Controls.Add(layout);
        }

        private void OnRealtimeResultUpdated(RealtimeResult result)
        {
            if (this.IsDisposed) return;

            if (this.InvokeRequired)
            {
                this.BeginInvoke(new Action<RealtimeResult>(OnRealtimeResultUpdated), result);
                return;
            }

            lblRmsX.Text = result.RmsX.ToString("F3");
            lblRmsY.Text = result.RmsY.ToString("F3");
            lblRmsZ.Text = result.RmsZ.ToString("F3");

            // Rule 기반 상태
            lblStatus.Text = result.IsAnomaly ? "anomaly (rule/model)" : "normal (rule/model)";

            if (result.IsAnomaly)
            {
                pnlAnomalyIndicator.BackColor = Color.Red;
                lblStatus.ForeColor = Color.Red;
            }
            else
            {
                pnlAnomalyIndicator.BackColor = Color.LimeGreen;
                lblStatus.ForeColor = Color.Black;
            }

            // 모델 결과
            if (!string.IsNullOrEmpty(result.PredictedLabel))
            {
                lblModelLabel.Text = result.PredictedLabel;
                lblModelScore.Text = result.ModelScore.ToString("F3");
            }
            else
            {
                lblModelLabel.Text = "(모델 없음)";
                lblModelScore.Text = "-";
            }

            lblLastUpdate.Text = result.Timestamp.ToString("yyyy-MM-dd HH:mm:ss");
        }
    }
}
