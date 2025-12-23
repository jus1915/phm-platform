using System;

namespace phm_data_pipeline.Models
{
    public class AccelEvent
    {
        public long SessionId { get; set; }

        public long EventIndex { get; set; }          // 1,2,3,...
        public long StartSampleIndex { get; set; }    // 세션 시작 기준 sample index
        public long EndSampleIndex { get; set; }

        // 시간 정보(필요한 경우)
        public DateTime? StartTimeUtc { get; set; }
        public DateTime? EndTimeUtc { get; set; }

        // 이벤트 특성값
        public double MaxAbsValue { get; set; }
        public int? MaxAbsChannel { get; set; }       // 0=X, 1=Y, 2=Z (없으면 null)

        public double MaxRms { get; set; }
        public double RmsX { get; set; }
        public double RmsY { get; set; }
        public double RmsZ { get; set; }

        // 메타데이터 (세션과 동일하게)
        public string TaskType { get; set; }
        public string LabelType { get; set; }
        public string DataSplit { get; set; }

        public double[] Ax { get; set; }
        public double[] Ay { get; set; }
        public double[] Az { get; set; }
    }
}