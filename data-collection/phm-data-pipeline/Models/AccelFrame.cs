using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace phm_data_pipeline.Models
{
    /// <summary>
    /// 가속도 프레임 1개 (3축, 동일 길이).
    /// </summary>
    public sealed class AccelFrame
    {
        public long Seq { get; set; }
        public DateTimeOffset Timestamp { get; set; }

        public double[] Ax { get; set; }
        public double[] Ay { get; set; }
        public double[] Az { get; set; }
    }

    /// <summary>
    /// 가속도 프레임을 발행하는 소스(DAQ Stub, 실제 NI-DAQ 등)가 구현할 인터페이스.
    /// </summary>
    public interface IAccelFrameSource
    {
        event Action<AccelFrame> FrameGenerated;
        double SampleRate { get; }
    }
}
