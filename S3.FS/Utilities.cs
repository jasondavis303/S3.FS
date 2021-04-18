using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace S3.FS
{
    static class Utilities
    {
        /// <summary>
        /// 1 Megabyte
        /// </summary>
        public const int DEFAULT_BUFFER_SIZE = 1024 * 1024;

        public static async Task<string> ComputeMD5Async(string filename, IProgress<OperationProgress> progress = null, int bufferSize = DEFAULT_BUFFER_SIZE, CancellationToken cancellationToken = default)
        {
            const string OPERATION = "Computing MD5";

            long totalSize = new FileInfo(filename).Length;
            long totalRead = 0;
            DateTime started = DateTime.Now;

            progress?.Report(OperationProgress.Build(OPERATION, filename, totalSize, 0, started));

            using FileStream fs = new FileStream(filename, FileMode.Open, FileAccess.Read, FileShare.Read, DEFAULT_BUFFER_SIZE, true);
            using MD5 md5 = MD5.Create();
            byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
            int bytesRead;
            do
            {
                bytesRead = await fs.ReadAsync(buffer, 0, DEFAULT_BUFFER_SIZE, cancellationToken).ConfigureAwait(false);
                if (bytesRead > 0)
                    md5.TransformBlock(buffer, 0, bytesRead, null, 0);
                totalRead += bytesRead;
                progress?.Report(OperationProgress.Build(OPERATION, filename, totalSize, totalRead, started));
            } while (bytesRead > 0);
            md5.TransformFinalBlock(buffer, 0, 0);

            progress?.Report(OperationProgress.Build(OPERATION, filename, totalSize, totalSize, started));

            return Convert.ToBase64String(md5.Hash);
        }
    }
}
