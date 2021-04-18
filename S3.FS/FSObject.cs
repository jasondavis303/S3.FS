using System;
using System.Collections.Generic;
using System.Linq;

namespace S3.FS
{
    public class FSObject : IComparable
    {
        public const string METADATA_MD5 = "x-amz-meta-md5chksum";

        public string Name { get; set; }
        public FSObject Parent { get; set; }
        public bool IsFolder { get; set; }
        public string Bucket { get; set; }
        public long Size { get; set; }
        public DateTime LastModified { get; set; }
        public List<FSObject> Children { get; } = new List<FSObject>();
        public bool IsBucket { get; set; }
        public string ETag { get; set; }
        public Dictionary<string, string> Metadata { get; } = new Dictionary<string, string>();

        public string Path => IsBucket ? FormattedBucket : $"{Parent.Path}/{Name}";

        public string Key => IsBucket ? string.Empty : Parent.IsBucket ? Name : $"{Parent.Key}/{Name}";

        public void Sort()
        {
            Children.Sort();
            Children.ForEach(item => item.Sort());
        }

        public FSObject FindDescendant(string key, StringComparison comp = StringComparison.CurrentCultureIgnoreCase)
        {
            if (string.IsNullOrWhiteSpace(key))
                return null;

            if (key.StartsWith(FormattedBucket, comp))
                throw new Exception($"{nameof(key)} cannot start with a bucket name");

            if (!IsBucket && Key.Equals(key, comp))
                return this;
           
            string[] parts = key.Split(new char[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
            var currentNode = this;
            for (int i = 0; i < parts.Length; i++)
            {
                var nextNode = currentNode.Children.FirstOrDefault(item => item.Name == parts[i]);
                if (nextNode == null)
                    return null;
                currentNode = nextNode;
            }

            return currentNode;
        }

        private string SortKey => IsBucket ? Bucket : Name;

        private string FormattedBucket => $"[{Bucket}]";

        public int CompareTo(object obj)
        {
            Children.Sort();

            FSObject comp = (FSObject)obj;

            int ret = -IsFolder.CompareTo(comp.IsFolder);

            if (ret == 0)
                ret = SortKey.CompareTo(comp.SortKey);

            return ret;
        }

        public override string ToString() => Path;
    }
}
