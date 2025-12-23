using System;

namespace phm_data_pipeline.Models
{
    public class RealtimeResult
    {
        public DateTime Timestamp { get; set; }

        public double RmsX { get; set; }
        public double RmsY { get; set; }
        public double RmsZ { get; set; }

        // Rule 기반 간단 이상 여부
        public bool IsAnomaly { get; set; }

        // === ML 모델 결과 추가 ===
        public string PredictedLabel { get; set; }   // 예: "normal", "bearing_fault"
        public float ModelScore { get; set; }        // 예: 확률 또는 score

        public string Note { get; set; }
    }
}
