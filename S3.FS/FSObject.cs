using System;
using System.Collections.Generic;
using System.Linq;

namespace S3.FS
{
    public class FSObject : IComparable
    {
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
        
        
        /// <summary>
        /// Memory only object to hold extra data associated with this object. It is not persisted anywhere, only exists for convenience.
        /// </summary>
        public object Data { get; set; }

        public string Path => IsBucket ? FormattedBucket : $"{Parent.Path}/{Name}";

        public string Key => IsBucket ? string.Empty : Parent.IsBucket ? Name : $"{Parent.Key}/{Name}";

        public IEnumerable<FSObject> Folders => Children.Where(item => item.IsFolder);

        public IEnumerable<FSObject> Files => Children.Where(item => !item.IsFolder);

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

        public FSObject CreateChildFolders(string key)
        {
            string[] parts = key.Split(new char[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
            string name = parts[0];

            var child = Folders.FirstOrDefault(item => item.Name == name);
            if(child == null)
            {
                child = new FSObject
                {
                    Bucket = Bucket,
                    IsFolder = true,
                    Name = name,
                    Parent = this
                };
                Children.Add(child);
            }

            if (parts.Length > 1)
                child = child.CreateChildFolders(string.Join("/", parts.Skip(1)));
            
            return child;            
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
