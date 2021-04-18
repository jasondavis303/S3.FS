using System;

namespace S3.FS
{
    public class OperationProgress
    {
        private const string ZERO_SPEED = "0 b/s";

        private OperationProgress() { }

        public string Operation { get; internal set; }
        public string File { get; internal set; }
        public long Size { get; internal set; }
        public long Complete { get; internal set; }
        public string Speed { get; internal set; }
        public double Percent { get; internal set; }

        internal static OperationProgress Build(string op, string file, long size, long complete, DateTime started)
        {
            var ret = new OperationProgress
            {
                Operation = op,
                File = file,
                Size = size,
                Complete = complete,
                Percent = size <= 0 ? 0 : complete / (double)size,
                Speed = ZERO_SPEED
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
