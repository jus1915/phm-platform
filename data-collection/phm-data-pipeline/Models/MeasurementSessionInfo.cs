using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace phm_data_pipeline.Models
{
    public class MeasurementSessionInfo
    {
        // === 기본 정보 ===
        public string DeviceId { get; set; }         // Kafka.MachineId
        public string Name { get; set; }             // 세션 이름 (지금은 비워둬도 됨)
        public DateTime StartedAtUtc { get; set; }
        public DateTime? EndedAtUtc { get; set; }

        public double SampleRateHz { get; set; }
        public int ChannelCount { get; set; }
        public string DeviceName { get; set; }       // DAQ Device (예: cDAQ1Mod1)
        public string Location { get; set; }         // 필요 시 나중에 사용
        public string RawFilePath { get; set; }      // MinIO / 로컬 경로 등
        public string Comment { get; set; }

        // === 수집 목적(메타데이터) ===
        public string TaskType { get; set; }         // anomaly / fault_diag / ...
        public string LabelType { get; set; }        // normal / fault
        public string DataSplit { get; set; }        // train / val / test
        public string Operator { get; set; }         // 작업자 ID
        public string Note { get; set; }             // 비고

        public string AcquisitionUnit { get; set; }  // 세션 수집 단위 (frame / event)
    }
}
