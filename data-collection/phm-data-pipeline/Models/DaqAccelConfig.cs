using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace phm_data_pipeline.Models
{
    [Serializable]
    public class DaqAccelConfig
    {
        public string Mode { get; set; }              // "DAQ" or "Stub"

        public DaqSettings Daq { get; set; }
        public KafkaSettings Kafka { get; set; }
        public MinioSettings Minio { get; set; }
        public MetaSettings Meta { get; set; }

        public DaqAccelConfig()
        {
            Daq = new DaqSettings();
            Kafka = new KafkaSettings();
            Minio = new MinioSettings();
            Meta = new MetaSettings();
        }

        [Serializable]
        public class DaqSettings
        {
            public string Device { get; set; }
            public string Channels { get; set; }
            public int SamplingRate { get; set; }
            public int FrameSize { get; set; }
            public int IntervalMs { get; set; }

            public double SensitivityX_mVpg { get; set; } = 100.0;
            public double SensitivityY_mVpg { get; set; } = 100.0;
            public double SensitivityZ_mVpg { get; set; } = 100.0;

            public double IepeCurrent_mA { get; set; } = 4.0;

            public double MinG { get; set; } = -25.0;
            public double MaxG { get; set; } = 25.0;

            public string AcquisitionUnit { get; set; } = "frame";
            public double EventRmsThreshold { get; set; } = 0.001;
            public int EventMinGapMs { get; set; } = 1;
        }

        [Serializable]
        public class KafkaSettings
        {
            public string Bootstrap { get; set; }
            public string Topic { get; set; }
            public string MachineId { get; set; }
        }

        [Serializable]
        public class MinioSettings
        {
            public string Endpoint { get; set; }
            public string AccessKey { get; set; }
            public string SecretKey { get; set; }
            public string Bucket { get; set; }
            public string SaveType { get; set; }          // "jsonl" / "npy" / "bin"
            public double SaveIntervalMinutes { get; set; }  // 1/10/60 ...
            public bool EnableRawSave { get; set; }
        }

        [Serializable]
        public class MetaSettings
        {
            public string TaskType { get; set; }   // anomaly, fault_diag, ...
            public string LabelType { get; set; }  // normal / fault
            public string DataSplit { get; set; }  // train / val / test
            public string Operator { get; set; }
            public string Note { get; set; }
        }
    }
}
