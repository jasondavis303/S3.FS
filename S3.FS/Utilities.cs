using Krypto.WonderDog;
using Krypto.WonderDog.Hashers;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace S3.FS
{
    static class Utilities
    {
        public static async Task<string> ComputeMD5Async(string filename, IProgress<OperationProgress> progress = null, CancellationToken cancellationToken = default)
        {
            const string OPERATION = "Computing MD5";

            IProgress<KryptoProgress> kprog = new Progress<KryptoProgress>(kp => progress?.Report(OperationProgress.Build(OPERATION, filename, kp)));

            var hasher = HasherFactory.CreateMD5();
            var hash = await hasher.HashFileAsync(filename, kprog, cancellationToken).ConfigureAwait(false);
            return Convert.ToBase64String(hash);
        }

    }
}
