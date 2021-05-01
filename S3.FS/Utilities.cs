using Krypto.WonderDog;
using Krypto.WonderDog.Hashers;
using System;
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

            IProgress<KryptoProgress> kprog = new Progress<KryptoProgress>(kp => progress?.Report(OperationProgress.Build(OPERATION, filename, kp)));

            var hasher = HasherFactory.CreateMD5();
            var hash = await hasher.HashFileAsync(filename, kprog, cancellationToken).ConfigureAwait(false);
            return Convert.ToBase64String(hash);
        }

    }
}
