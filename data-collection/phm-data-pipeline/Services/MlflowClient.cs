// 파일: Services/MlflowClient.cs
using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace phm_data_pipeline.Services
{
    public class MlflowRunInfo
    {
        public string RunId { get; set; }
        public string Status { get; set; }
        public string RunName { get; set; }
        public DateTimeOffset? StartTime { get; set; }
        public DateTimeOffset? EndTime { get; set; }
        public Dictionary<string, double> Metrics { get; } = new Dictionary<string, double>();
    }

    public class MlflowExperimentInfo
    {
        public string ExperimentId { get; set; } = "";
        public string Name { get; set; } = "";
        public string? LifecycleStage { get; set; }
    }

    public class MlflowClient
    {
        private readonly HttpClient _http;

        public MlflowClient(string baseUrl)
        {
            if (string.IsNullOrWhiteSpace(baseUrl))
                throw new ArgumentNullException(nameof(baseUrl));

            _http = new HttpClient
            {
                BaseAddress = new Uri(baseUrl.TrimEnd('/') + "/")
            };
        }

        /// <summary>이름으로 Experiment ID 조회 (필요시 계속 사용 가능)</summary>
        public async Task<string?> GetExperimentIdByNameAsync(string experimentName)
        {
            var url = $"api/2.0/mlflow/experiments/get-by-name?experiment_name={Uri.EscapeDataString(experimentName)}";

            var resp = await _http.GetAsync(url);
            var body = await resp.Content.ReadAsStringAsync();

            if (!resp.IsSuccessStatusCode)
            {
                if ((int)resp.StatusCode == 404 && body.Contains("RESOURCE_DOES_NOT_EXIST"))
                    return null;

                throw new Exception($"MLflow API 에러: {(int)resp.StatusCode} {resp.ReasonPhrase}\nURL: {url}\nBody: {body}");
            }

            using var stream = new MemoryStream(Encoding.UTF8.GetBytes(body));
            using var doc = await JsonDocument.ParseAsync(stream);

            if (!doc.RootElement.TryGetProperty("experiment", out var exp))
                return null;

            return exp.GetProperty("experiment_id").GetString();
        }

        /// <summary>Experiment 전체 리스트 조회 (기본: deleted 제외)</summary>
        public async Task<List<MlflowExperimentInfo>> ListExperimentsAsync(bool includeDeleted = false)
        {
            var url = "api/2.0/mlflow/experiments/search";

            HttpResponseMessage resp = null;
            string body = null;

            try
            {
                // MLflow 공식 문서 예시: experiments/search (POST)
                // view_type = "ALL" 로 삭제된 것도 포함해서 가져오고,
                // includeDeleted=false 인 경우 여기서 필터링
                var payload = new
                {
                    max_results = 1000,
                    view_type = "ALL"
                };

                var json = JsonSerializer.Serialize(payload);
                var content = new StringContent(json, Encoding.UTF8, "application/json");

                AppEvents.RaiseLog($"[MLflow][ExpSearch] 요청: {_http.BaseAddress}{url}");

                resp = await _http.PostAsync(url, content);
                body = await resp.Content.ReadAsStringAsync();

                if (!resp.IsSuccessStatusCode)
                {
                    AppEvents.RaiseLog(
                        $"[MLflow][ExpSearch][FAIL] Status={(int)resp.StatusCode} {resp.ReasonPhrase}, Body={body}");
                    throw new Exception(
                        $"MLflow API 에러 (experiments/search): {(int)resp.StatusCode} {resp.ReasonPhrase}\n" +
                        $"URL: {url}\nBody: {body}");
                }

                using var stream = new MemoryStream(Encoding.UTF8.GetBytes(body));
                using var doc = await JsonDocument.ParseAsync(stream);

                var result = new List<MlflowExperimentInfo>();

                if (!doc.RootElement.TryGetProperty("experiments", out var exps) ||
                    exps.ValueKind != JsonValueKind.Array)
                {
                    AppEvents.RaiseLog("[MLflow][ExpSearch] experiments 배열 없음 또는 Array 아님");
                    return result;
                }

                foreach (var e in exps.EnumerateArray())
                {
                    var id = e.GetProperty("experiment_id").GetString() ?? "";
                    var name = e.GetProperty("name").GetString() ?? "";

                    string? lifecycle = null;
                    if (e.TryGetProperty("lifecycle_stage", out var lcElem) &&
                        lcElem.ValueKind == JsonValueKind.String)
                    {
                        lifecycle = lcElem.GetString();
                    }

                    if (!includeDeleted &&
                        string.Equals(lifecycle, "deleted", StringComparison.OrdinalIgnoreCase))
                    {
                        // 삭제된 Experiment는 기본적으로 숨김
                        continue;
                    }

                    result.Add(new MlflowExperimentInfo
                    {
                        ExperimentId = id,
                        Name = name,
                        LifecycleStage = lifecycle
                    });
                }

                AppEvents.RaiseLog($"[MLflow][ExpSearch] 로드 완료: count={result.Count}");

                return result;
            }
            catch (Exception ex)
            {
                AppEvents.RaiseLog($"[MLflow][ExpSearch][EXCEPTION] ex={ex}");
                throw;
            }
        }

        /// <summary>특정 experiment의 가장 최신 run 정보</summary>
        public async Task<MlflowRunInfo?> GetLatestRunInfoAsync(string experimentId)
        {
            var url = "api/2.0/mlflow/runs/search";

            var payload = new
            {
                experiment_ids = new[] { experimentId },
                max_results = 1,
                order_by = new[] { "attributes.start_time DESC" }
            };

            var json = JsonSerializer.Serialize(payload);
            var content = new StringContent(json, Encoding.UTF8, "application/json");

            var resp = await _http.PostAsync(url, content);
            resp.EnsureSuccessStatusCode();

            var body = await resp.Content.ReadAsStringAsync();

            using var stream = new MemoryStream(Encoding.UTF8.GetBytes(body));
            using var doc = await JsonDocument.ParseAsync(stream);

            if (!doc.RootElement.TryGetProperty("runs", out var runs) ||
                runs.GetArrayLength() == 0)
            {
                return null;
            }

            var runElem = runs[0];
            return ParseRunInfo(runElem);
        }

        /// <summary>특정 experiment의 최근 run 목록</summary>
        public async Task<List<MlflowRunInfo>> ListRecentRunsAsync(string experimentId, int maxResults = 20)
        {
            var url = "api/2.0/mlflow/runs/search";

            var payload = new
            {
                experiment_ids = new[] { experimentId },
                max_results = maxResults,
                order_by = new[] { "attributes.start_time DESC" }
            };

            var json = JsonSerializer.Serialize(payload);
            var content = new StringContent(json, Encoding.UTF8, "application/json");

            var resp = await _http.PostAsync(url, content);
            resp.EnsureSuccessStatusCode();

            var body = await resp.Content.ReadAsStringAsync();

            using var stream = new MemoryStream(Encoding.UTF8.GetBytes(body));
            using var doc = await JsonDocument.ParseAsync(stream);

            var list = new List<MlflowRunInfo>();

            if (!doc.RootElement.TryGetProperty("runs", out var runs) ||
                runs.GetArrayLength() == 0)
            {
                return list;
            }

            foreach (var runElem in runs.EnumerateArray())
            {
                list.Add(ParseRunInfo(runElem));
            }

            return list;
        }

        /// <summary>run_id로 run 상세 정보</summary>
        public async Task<MlflowRunInfo?> GetRunInfoAsync(string runId)
        {
            var url = $"api/2.0/mlflow/runs/get?run_id={runId}";

            var resp = await _http.GetAsync(url);
            resp.EnsureSuccessStatusCode();

            var body = await resp.Content.ReadAsStringAsync();

            using var stream = new MemoryStream(Encoding.UTF8.GetBytes(body));
            using var doc = await JsonDocument.ParseAsync(stream);

            if (!doc.RootElement.TryGetProperty("run", out var runElem))
                return null;

            return ParseRunInfo(runElem);
        }

        private MlflowRunInfo ParseRunInfo(JsonElement runElem)
        {
            var info = runElem.GetProperty("info");
            var runId = info.GetProperty("run_id").GetString();
            var status = info.GetProperty("status").GetString();

            string runName = null;
            if (info.TryGetProperty("run_name", out var rnElem) && rnElem.ValueKind == JsonValueKind.String)
                runName = rnElem.GetString();

            DateTimeOffset? start = null;
            if (info.TryGetProperty("start_time", out var stElem) && stElem.ValueKind == JsonValueKind.Number)
            {
                var ms = stElem.GetInt64();
                start = DateTimeOffset.FromUnixTimeMilliseconds(ms);
            }

            DateTimeOffset? end = null;
            if (info.TryGetProperty("end_time", out var etElem) && etElem.ValueKind == JsonValueKind.Number)
            {
                var ms = etElem.GetInt64();
                end = DateTimeOffset.FromUnixTimeMilliseconds(ms);
            }

            var result = new MlflowRunInfo
            {
                RunId = runId,
                Status = status,
                RunName = runName,
                StartTime = start,
                EndTime = end
            };

            if (runElem.TryGetProperty("data", out var data) &&
                data.TryGetProperty("metrics", out var metrics))
            {
                foreach (var m in metrics.EnumerateArray())
                {
                    var key = m.GetProperty("key").GetString();
                    var val = m.GetProperty("value").GetDouble();

                    if (!string.IsNullOrEmpty(key))
                        result.Metrics[key] = val;
                }
            }

            return result;
        }

        /// <summary>특정 run 의 metric 히스토리</summary>
        public async Task<List<(DateTime time, double value)>> GetMetricHistoryAsync(string runId, string metricKey)
        {
            var url = $"api/2.0/mlflow/metrics/get-history?run_id={runId}&metric_key={Uri.EscapeDataString(metricKey)}";

            HttpResponseMessage resp = null;
            string body = null;

            try
            {
                resp = await _http.GetAsync(url);
                body = await resp.Content.ReadAsStringAsync();

                if (!resp.IsSuccessStatusCode)
                {
                    //AppEvents.RaiseLog(
                    //    $"[MLflow][History][FAIL] run_id={runId}, metric={metricKey}, " +
                    //    $"Status={(int)resp.StatusCode} {resp.ReasonPhrase}, Body={body}");

                    return new List<(DateTime time, double value)>();
                }

                //AppEvents.RaiseLog(
                //    $"[MLflow][History][OK] run_id={runId}, metric={metricKey}, Status={(int)resp.StatusCode}");

                using var stream = new MemoryStream(Encoding.UTF8.GetBytes(body));
                using var doc = await JsonDocument.ParseAsync(stream);

                var list = new List<(DateTime time, double value)>();

                if (!doc.RootElement.TryGetProperty("metrics", out var metrics))
                {
                    AppEvents.RaiseLog($"[MLflow][History] metrics 배열 없음 run_id={runId}, metric={metricKey}");
                    return list;
                }

                foreach (var m in metrics.EnumerateArray())
                {
                    var val = m.GetProperty("value").GetDouble();
                    var ts = m.GetProperty("timestamp").GetInt64(); // ms since epoch
                    var time = DateTimeOffset.FromUnixTimeMilliseconds(ts).LocalDateTime;
                    list.Add((time, val));
                }

                //AppEvents.RaiseLog(
                //    $"[MLflow][History] run_id={runId}, metric={metricKey}, count={list.Count}");

                return list;
            }
            catch (Exception ex)
            {
                AppEvents.RaiseLog(
                    $"[MLflow][History][EXCEPTION] run_id={runId}, metric={metricKey}, ex={ex}");

                return new List<(DateTime time, double value)>();
            }
        }
    }
}
