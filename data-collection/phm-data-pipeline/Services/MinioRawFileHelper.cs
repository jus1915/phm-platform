// 파일: Services/MinioRawFileHelper.cs
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Minio;
using Minio.ApiEndpoints;
using Minio.DataModel;
using Minio.DataModel.Args;

namespace phm_data_pipeline.Services
{
    public static class MinioRawFileHelper
    {
        // 환경에 맞게 수정
        private const string Endpoint = "localhost:19000";
        private const string AccessKey = "admin";
        private const string SecretKey = "admin12345";
        private const string Bucket = "phm-raw";

        private static readonly IMinioClient _client =
            new MinioClient()
                .WithEndpoint(Endpoint)
                .WithCredentials(AccessKey, SecretKey)
                .WithSSL(false)   // HTTPS 쓰면 true
                .Build();

        public static async Task<int> DeleteSessionFilesByRawPathAsync(string rawFilePath)
        {
            if (string.IsNullOrWhiteSpace(rawFilePath))
                return 0;

            // 1) s3://phm-raw/ 제거 → 순수 key만 남김
            string prefix = ExtractMinioKeyPrefix(rawFilePath);

            // 2) 대표파일의 시퀀스 번호 제거
            prefix = RemoveSequenceNumber(prefix);

            var keysToDelete = new List<string>();

            var listArgs = new ListObjectsArgs()
                .WithBucket(Bucket)
                .WithPrefix(prefix)
                .WithRecursive(true);

            IObservable<Item> observable = _client.ListObjectsAsync(listArgs);
            var completion = new TaskCompletionSource<bool>();

            observable.Subscribe(
                item =>
                {
                    if (!item.IsDir)
                        keysToDelete.Add(item.Key);
                },
                ex => completion.TrySetException(ex),
                () => completion.TrySetResult(true)
            );

            await completion.Task;

            foreach (var key in keysToDelete)
            {
                var rmArgs = new RemoveObjectArgs()
                    .WithBucket(Bucket)
                    .WithObject(key);

                await _client.RemoveObjectAsync(rmArgs);
            }

            return keysToDelete.Count;
        }

        // s3://phm-raw/M001/... → M001/...
        private static string ExtractMinioKeyPrefix(string rawFilePath)
        {
            rawFilePath = rawFilePath.Replace('\\', '/');

            const string prefix = "s3://";
            if (rawFilePath.StartsWith(prefix))
                rawFilePath = rawFilePath.Substring(prefix.Length);

            // "phm-raw/" 제거
            if (rawFilePath.StartsWith(Bucket + "/"))
                rawFilePath = rawFilePath.Substring((Bucket + "/").Length);

            return rawFilePath;
        }

        // ..._000001.jsonl → ..._
        private static string RemoveSequenceNumber(string key)
        {
            int idx = key.LastIndexOf('_');
            if (idx < 0)
                return key;

            return key.Substring(0, idx + 1);
        }
    }
}
