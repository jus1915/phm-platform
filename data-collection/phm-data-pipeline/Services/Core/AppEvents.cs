using phm_data_pipeline.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace phm_data_pipeline.Services
{
    public static class AppEvents
    {
        public static event Action<string> LogRequested;

        public static event Action<RealtimeResult> RealtimeResultUpdated;

        public static void RaiseRealtimeResultUpdated(RealtimeResult result)
        {
            try
            {
                RealtimeResultUpdated?.Invoke(result);
            }
            catch (Exception ex)
            {
                // 이벤트 구독 쪽에서 예외 터져도 전체 죽지 않게
                RaiseLog("[AppEvents] RealtimeResultUpdated 처리 중 예외: " + ex);
            }
        }

        public static void RaiseLog(string message) => LogRequested?.Invoke(message);

        public static IAccelFrameSource CurrentFrameSource { get; private set; }

        public static event Action<IAccelFrameSource> FrameSourceChanged;

        public static void RaiseFrameSourceChanged(IAccelFrameSource source)
        {
            CurrentFrameSource = source;

            var h = FrameSourceChanged;
            if (h != null) h(source);

            // 디버깅 로그도 하나 찍어두면 좋아요
            var name = (source == null) ? "null" : source.GetType().Name;
            RaiseLog("[Graph] FrameSourceChanged: " + name);
        }
    }
}
