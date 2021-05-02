namespace S3.FS
{
    public class LoadProgress
    {
        internal LoadProgress(int count, bool done)
        {
            Count = count;
            Done = done;
        }

        internal LoadProgress(int count) : this(count, false) { }

        public int Count { get; }
        public bool Done { get; }
    }
}
