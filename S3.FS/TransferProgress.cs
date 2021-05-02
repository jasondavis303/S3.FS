using Krypto.WonderDog;
using System;

namespace S3.FS
{
    public class TransferProgress
    {
        private const string ZERO_SPEED = "0 b/s";

        private TransferProgress() { }

        public string Operation { get; internal set; }
        public string File { get; internal set; }
        public long Size { get; internal set; }
        public long Complete { get; internal set; }
        public string Speed { get; internal set; }
        public double Percent { get; internal set; }
        public bool Done { get; internal set; }

        internal static TransferProgress Build(string op, string file, KryptoProgress prog) =>
            new TransferProgress
            {
                Operation = op,
                File = file,
                Size = prog.TotalBytes,
                Complete = prog.BytesComplete,
                Percent = prog.TotalBytes <= 0 ? 0 : prog.BytesComplete / (double)prog.TotalBytes,
                Speed = prog.Speed,
                Done = prog.Done
            };
        
        internal static TransferProgress Build(string op, string file, long size, long complete, DateTime started, bool done)
        {
            var ret = new TransferProgress
            {
                Operation = op,
                File = file,
                Size = size,
                Complete = complete,
                Percent = size <= 0 ? 0 : complete / (double)size,
                Speed = ZERO_SPEED,
                Done = done
            };

            try
            {
                if (complete > 0)
                {
                    double seconds = (DateTime.Now - started).TotalSeconds;
                    if (seconds > 0)
                    {
                        double val = complete / seconds;
                        string[] exts = new string[] { "B", "Kb", "Mb", "Gb", "Tb", "Pb", "Xb", "Yb", "Zb" };
                        int index = 0;
                        while (val >= 1024)
                        {
                            val /= 1024;
                            index++;
                            if (index >= exts.Length - 1)
                                break;
                        }

                        ret.Speed = $"{val:0.00} {exts[index]}/s";
                    }
                }
            }
            catch
            {
                //If complete / seconds creates a double.NaN (too big)
                //then just swallow the error
            }

            return ret;
        }
    }
}
