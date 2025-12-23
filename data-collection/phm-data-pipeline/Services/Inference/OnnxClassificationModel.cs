using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.ML.OnnxRuntime;
using Microsoft.ML.OnnxRuntime.Tensors;

namespace phm_data_pipeline.Services.Inference
{
    /// <summary>
    /// ONNX 분류 모델 래퍼 (다중 클래스 분류 가정)
    /// </summary>
    public class OnnxClassificationModel : IDisposable
    {
        private readonly InferenceSession _session;
        private readonly string _inputName;
        private readonly string[] _labelMap;

        public string ModelPath { get; }

        public OnnxClassificationModel(string modelPath, string[] labelMap = null)
        {
            if (string.IsNullOrWhiteSpace(modelPath))
                throw new ArgumentNullException(nameof(modelPath));

            if (!File.Exists(modelPath))
                throw new FileNotFoundException("ONNX model not found", modelPath);

            ModelPath = modelPath;
            _session = new InferenceSession(modelPath);
            _labelMap = labelMap;

            // ★ 입력 이름 결정 (skl2onnx에서 initial_type을 "input"으로 줬으므로 우선 그것)
            var inputKeys = _session.InputMetadata.Keys.ToList();

            string name = "input";
            if (!inputKeys.Contains(name))
                name = inputKeys.FirstOrDefault(k => !string.IsNullOrEmpty(k));

            if (string.IsNullOrEmpty(name))
                throw new InvalidOperationException("ONNX 모델의 입력 이름을 찾을 수 없습니다.");

            _inputName = name;
        }

        /// <summary>
        /// features: [feature_dim] 형태의 float 배열
        /// </summary>
        public (string label, float score) Predict(float[] features)
        {
            if (features == null)
                throw new ArgumentNullException(nameof(features));
            if (features.Length == 0)
                throw new ArgumentException("features length must be > 0", nameof(features));

            // [1, feature_dim] 텐서
            var tensor = new DenseTensor<float>(new[] { 1, features.Length });
            for (int i = 0; i < features.Length; i++)
                tensor[0, i] = features[i];

            var inputs = new List<NamedOnnxValue>
            {
                NamedOnnxValue.CreateFromTensor(_inputName, tensor)
            };

            using var results = _session.Run(inputs);

            float[] probs = null;
            string[] labelsFromMap = null;

            // ★ 여러 output 중에서, float 벡터 또는 ZipMap(dict<string,float>) 를 찾아서 사용
            foreach (var r in results)
            {
                // 1) 텐서/벡터 형태일 가능성
                try
                {
                    var enumerable = r.AsEnumerable<float>();
                    if (enumerable != null)
                    {
                        var arr = enumerable.ToArray();
                        if (arr.Length > 0)
                        {
                            probs = arr;
                            break;
                        }
                    }
                }
                catch
                {
                    // float로 안 바뀌면 무시하고 다음 output 시도
                }

                // 2) ZipMap(map<string,float>) 형태일 가능성
                try
                {
                    var dict = r.AsDictionary<string, float>();
                    if (dict != null && dict.Count > 0)
                    {
                        probs = dict.Values.ToArray();
                        labelsFromMap = dict.Keys.ToArray();
                        break;
                    }
                }
                catch
                {
                    // dict 타입이 아니면 무시
                }
            }

            if (probs == null || probs.Length == 0)
            {
                // 적절한 float 출력 못 찾았으면 빈 결과
                return ("", 0f);
            }

            // argmax
            int bestIdx = 0;
            float bestScore = probs[0];

            for (int i = 1; i < probs.Length; i++)
            {
                if (probs[i] > bestScore)
                {
                    bestScore = probs[i];
                    bestIdx = i;
                }
            }

            // label 매핑
            string label = null;

            // 우선순위 1: _labelMap (C#에서 지정한 label 배열)
            if (_labelMap != null && bestIdx >= 0 && bestIdx < _labelMap.Length)
            {
                label = _labelMap[bestIdx];
            }
            // 우선순위 2: ZipMap key (ONNX가 map<string,float> 로 준 경우)
            else if (labelsFromMap != null && bestIdx >= 0 && bestIdx < labelsFromMap.Length)
            {
                label = labelsFromMap[bestIdx];
            }
            else
            {
                label = $"class_{bestIdx}";
            }

            return (label, bestScore);
        }


        public void Dispose()
        {
            _session?.Dispose();
        }
    }
}
