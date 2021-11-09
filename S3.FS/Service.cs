using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace S3.FS
{
    public class S3FService : IDisposable
    {
        public const string METADATA_MD5 = "x-amz-meta-md5chksum";

        private readonly AmazonS3Client _client;
        private readonly TransferUtility _transferUtility;

        public S3FService(string serviceUrl, string awsAccessKeyId, string awsSecretAccessKey)
        {
            if (string.IsNullOrWhiteSpace(serviceUrl))
                throw new ArgumentNullException(nameof(serviceUrl));

            if (string.IsNullOrWhiteSpace(awsAccessKeyId))
                throw new ArgumentNullException(nameof(awsAccessKeyId));

            if (string.IsNullOrWhiteSpace(awsSecretAccessKey))
                throw new ArgumentNullException(nameof(awsSecretAccessKey));

            if (!serviceUrl.ToLower().StartsWith("http://") && !serviceUrl.ToLower().StartsWith("https://"))
                serviceUrl = "https://" + serviceUrl;

            _client = new AmazonS3Client(awsAccessKeyId, awsSecretAccessKey, new AmazonS3Config { ServiceURL = serviceUrl });
            _transferUtility = new TransferUtility(_client);
        }

        public void Dispose()
        {
            _client.Dispose();
            _transferUtility.Dispose();
        }

        private string[] CheckFileExtensions(string[] fileExtensions)
        {
            if (fileExtensions != null)
            {
                var distList = new List<string>(fileExtensions);
                for (int i = 0; i < distList.Count; i++)
                {
                    if (!string.IsNullOrWhiteSpace(distList[i]))
                    {
                        distList[i] = distList[i].Trim('*');
                        if (!distList[i].StartsWith("."))
                            distList[i] = '.' + distList[i];
                        distList[i] = distList[i].ToLower();
                    }
                }
                fileExtensions = distList.Distinct().ToArray();
            }

            if (fileExtensions != null && fileExtensions.Length == 0)
                fileExtensions = null;

            return fileExtensions;
        }




        public async Task<List<FSObject>> GetBucketsAsync(CancellationToken cancellationToken = default)
        {
            var response = await _client.ListBucketsAsync(cancellationToken).ConfigureAwait(false);
            var ret = response.Buckets
                .Select(_ => new FSObject
                {
                    Bucket = _.BucketName,
                    IsBucket = true,
                    Name = _.BucketName
                }).ToList();

            ret.Sort();
            return ret;
        }

        /// <summary>
        /// Loads children of the current object
        /// </summary>
        /// <param name="fileExtensions">If null, loads all files</param>
        public async Task LoadChildrenAsync(FSObject parent, bool loadFolders = true, bool loadFiles = true, string[] fileExtensions = null, IProgress<LoadProgress> progress = null, CancellationToken cancellationToken = default)
        {
            if (parent == null)
                throw new ArgumentNullException(nameof(parent));

            if (!(parent.IsFolder || parent.IsBucket))
                return;

            fileExtensions = CheckFileExtensions(fileExtensions);

            parent.Children.Clear();

            string parentPath = parent.Key;
            if (!string.IsNullOrEmpty(parentPath) && !parentPath.EndsWith("/"))
                parentPath += "/";

            var request = new ListObjectsV2Request { BucketName = parent.Bucket, Prefix = parentPath, Delimiter = "/" };
            ListObjectsV2Response response;
            do
            {
                response = await _client.ListObjectsV2Async(request, cancellationToken).ConfigureAwait(false);
                if (loadFolders)
                    foreach (string cp in response.CommonPrefixes)
                        parent.Children.Add(new FSObject
                        {
                            Bucket = parent.Bucket,
                            IsFolder = true,
                            Name = Path.GetFileName(cp.TrimEnd('/')),
                            Parent = parent
                        });

                if (loadFiles)
                    foreach (var s3 in response.S3Objects)
                        if (s3.Key != parentPath)
                            if (fileExtensions == null || fileExtensions.Contains((Path.GetExtension(s3.Key) + string.Empty).ToLower()))
                                parent.Children.Add(new FSObject
                                {
                                    Bucket = parent.Bucket,
                                    LastModified = s3.LastModified,
                                    Name = Path.GetFileName(s3.Key),
                                    Parent = parent,
                                    Size = s3.Size,
                                    ETag = s3.ETag
                                });


                progress?.Report(new LoadProgress(parent.Children.Count));

                request.ContinuationToken = response.NextContinuationToken;
            } while (response.IsTruncated);

            parent.Sort();

            progress?.Report(new LoadProgress(parent.Children.Count, true));
        }


        /// <summary>
        /// Loads all descendants of the current object
        /// </summary>
        /// <param name="fileExtensions">If null, loads all files</param>
        public async Task LoadDescendantsAsync(FSObject parent, string[] fileExtensions = null, IProgress<LoadProgress> progress = null, CancellationToken cancellationToken = default)
        {
            if (parent == null)
                throw new ArgumentNullException(nameof(parent));

            if (!(parent.IsFolder || parent.IsBucket))
                return;

            fileExtensions = CheckFileExtensions(fileExtensions);

            parent.Children.Clear();

            string parentPath = parent.Key;
            if (parentPath != null && !parentPath.EndsWith("/"))
                parentPath += "/";
            if (parent.IsBucket)
                parentPath = null;

            int cnt = 0;

            var request = new ListObjectsV2Request { BucketName = parent.Bucket, Prefix = parentPath };
            ListObjectsV2Response response;
            do
            {
                response = await _client.ListObjectsV2Async(request, cancellationToken).ConfigureAwait(false);

                foreach (var s3 in response.S3Objects)
                {
                    string key = s3.Key.Substring(parent.Key.Length).Trim('/');
                    string[] parts = key.Split(new char[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
                    var currentNode = parent;
                    for (int i = 0; i < parts.Length; i++)
                    {
                        var nextNode = currentNode.Children.FirstOrDefault(item => item.Name == parts[i]);
                        if (nextNode == null)
                        {
                            nextNode = new FSObject
                            {
                                Bucket = parent.Bucket,
                                LastModified = s3.LastModified,
                                Name = parts[i],
                                Parent = currentNode,
                                Size = s3.Size,
                                IsFolder = i < parts.Length - 1 || s3.Key.EndsWith("/"),
                                ETag = i < parts.Length - 1 || s3.Key.EndsWith("/") ? null : s3.ETag
                            };


                            bool add = true;
                            if (!nextNode.IsFolder && fileExtensions != null)
                                add = fileExtensions.Contains((Path.GetExtension(nextNode.Key) + string.Empty).ToLower());

                            if (add)
                            {
                                currentNode.Children.Add(nextNode);
                                cnt++;
                            }
                        }
                        currentNode = nextNode;
                    }
                }

                progress?.Report(new LoadProgress(cnt));

                request.ContinuationToken = response.NextContinuationToken;
            } while (response.IsTruncated);

            parent.Sort();

            progress?.Report(new LoadProgress(cnt, true));
        }

        public async Task<FSObject> GetObjectAsync(FSObject parent, string subKey, CancellationToken cancellationToken = default)
        {
            if (parent == null)
                throw new ArgumentNullException(nameof(parent));

            if (string.IsNullOrWhiteSpace(subKey))
                throw new ArgumentNullException(nameof(subKey));

            if (!(parent.IsFolder || parent.IsBucket))
                return null;

            var existing = parent.FindDescendant(subKey);
            if (existing != null)
                return existing;

            string parentPath = parent.Key;
            if (parentPath != null && !parentPath.EndsWith("/"))
                parentPath += "/";

            subKey = subKey.Trim('/');

            var request = new GetObjectRequest
            {
                BucketName = parent.Bucket,
                Key = (parentPath + subKey).Trim('/')
            };

            GetObjectResponse response = null;
            try
            {
                response = await _client.GetObjectAsync(request, cancellationToken).ConfigureAwait(false);
                if (response == null)
                    return null;
            }
            catch
            {
                //Swallow 'not found' and return null
                return null;
            }

            string key = response.Key.Substring(parent.Key.Length).Trim('/');
            string[] parts = key.Split(new char[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
            var currentNode = parent;
            for (int i = 0; i < parts.Length; i++)
            {
                var nextNode = currentNode.Children.FirstOrDefault(item => item.Name == parts[i]);
                if (nextNode == null)
                {
                    nextNode = new FSObject
                    {
                        Bucket = parent.Bucket,
                        LastModified = response.LastModified,
                        Name = parts[i],
                        Parent = currentNode,
                        Size = response.ContentLength,
                        IsFolder = i < parts.Length - 1 || response.Key.EndsWith("/"),
                        ETag = i < parts.Length - 1 || response.Key.EndsWith("/") ? null : response.ETag
                    };

                    currentNode.Children.Add(nextNode);
                }
                currentNode = nextNode;
            }

            return currentNode;
        }

        public async Task LoadMetaAsync(FSObject s3File, CancellationToken cancellationToken = default)
        {
            if (s3File == null)
                throw new ArgumentNullException(nameof(s3File));

            var result = await _client.GetObjectMetadataAsync(s3File.Bucket, s3File.Key, cancellationToken).ConfigureAwait(false);
            s3File.Metadata.Clear();
            foreach (string key in result.Metadata.Keys)
                s3File.Metadata.Add(key, result.Metadata[key]);
        }

        public Task<FSObject> UploadFileAsync(string filename, FSObject parent, Dictionary<string, string> metadata = null, bool computeMD5 = false, IProgress<TransferProgress> progress = null, CancellationToken cancellationToken = default)
        {
            return UploadFileAsync(filename, parent, Path.GetFileName(filename), metadata, computeMD5, progress, cancellationToken);
        }

        public async Task<FSObject> UploadFileAsync(string filename, FSObject parent, string newFilename, Dictionary<string, string> metadata = null, bool computeMD5 = false, IProgress<TransferProgress> progress = null, CancellationToken cancellationToken = default)
        {
            const string OPERATION = "Uploading";

            if (computeMD5)
            {
                if (metadata == null)
                    metadata = new Dictionary<string, string>();
                metadata[METADATA_MD5] = await Utilities.ComputeMD5Async(filename, progress, cancellationToken).ConfigureAwait(false);
            }

            var req = new TransferUtilityUploadRequest
            {
                BucketName = parent.Bucket,
                FilePath = filename,
                Key = parent.Key + '/' + newFilename
            };
            if (metadata != null)
                foreach (string key in metadata.Keys)
                    req.Metadata.Add(key, metadata[key]);

            DateTime started = DateTime.Now;
            req.UploadProgressEvent += (object sender, UploadProgressArgs e) => progress?.Report(TransferProgress.Build(OPERATION, filename, e.TotalBytes, e.TransferredBytes, started, false));

            await _transferUtility.UploadAsync(req, cancellationToken).ConfigureAwait(false);

            if (progress != null)
            {
                var size = new FileInfo(filename).Length;
                progress.Report(TransferProgress.Build(OPERATION, filename, size, size, started, true));
            }

            parent.Children.RemoveAll(item => !item.IsFolder && item.Name == newFilename);
            var ret = await GetObjectAsync(parent, Path.GetFileName(filename), cancellationToken).ConfigureAwait(false);
            if (metadata != null)
                await LoadMetaAsync(ret, cancellationToken).ConfigureAwait(false);

            return ret;
        }
        
        public async Task DownloadFileAsync(FSObject s3File, string filename, IProgress<TransferProgress> progress = null, CancellationToken cancellationToken = default)
        {
            const string OPERATION = "Downloading";

            if (s3File == null)
                throw new ArgumentNullException(nameof(s3File));

            if (string.IsNullOrWhiteSpace(filename))
                throw new ArgumentNullException(nameof(filename));

            Directory.CreateDirectory(Path.GetDirectoryName(filename));

            var req = new TransferUtilityDownloadRequest
            {
                BucketName = s3File.Bucket,
                FilePath = filename,
                Key = s3File.Key
            };

            DateTime started = DateTime.Now;
            req.WriteObjectProgressEvent += (object sender, WriteObjectProgressArgs e) => progress?.Report(TransferProgress.Build(OPERATION, e.Key, e.TotalBytes, e.TransferredBytes, started, false));

            await _transferUtility.DownloadAsync(req, cancellationToken).ConfigureAwait(false);

            if (progress != null)
            {
                var size = new FileInfo(filename).Length;
                progress.Report(TransferProgress.Build(OPERATION, s3File.Key, size, size, started, true));
            }
        }

        public async Task DeleteFileAsync(FSObject s3File, CancellationToken cancellationToken = default)
        {
            await _client.DeleteObjectAsync(s3File.Bucket, s3File.Key, cancellationToken);
            s3File.Parent.Children.Remove(s3File);
        }

        public async Task<FSObject> CopyFileAsync(FSObject src, FSObject dstParent, string dstName, CancellationToken cancellationToken = default)
        {
            var response = await _client.CopyObjectAsync(src.Bucket, src.Key, dstParent.Bucket, $"{dstParent.Key}/{dstName}", cancellationToken).ConfigureAwait(false);
            dstParent.Children.RemoveAll(item => item.Name == dstName);
            return await GetObjectAsync(dstParent, dstName, cancellationToken).ConfigureAwait(false);
        }

        public async Task<FSObject> MoveFileAsync(FSObject src, FSObject dstParent, string dstName, CancellationToken cancellationToken = default)
        {
            var ret = await CopyFileAsync(src, dstParent, dstName, cancellationToken).ConfigureAwait(false);
            await DeleteFileAsync(src, cancellationToken).ConfigureAwait(false);
            return ret;
        }

        public string GetPreSignedURL(FSObject s3File) => GetPreSignedURL(s3File, DateTime.MaxValue);

        public string GetPreSignedURL(FSObject s3File, DateTime expires)
        {
            if (s3File == null)
                throw new ArgumentNullException(nameof(s3File));

            if (s3File.IsBucket || s3File.IsFolder)
                throw new Exception($"{nameof(s3File)} is not a file");

            var expiryUrlRequest = new GetPreSignedUrlRequest
            {
                BucketName = s3File.Bucket,
                Key = s3File.Key,
                Expires = expires
            };

            return _client.GetPreSignedURL(expiryUrlRequest);
        }

        public string GetPublicURL(FSObject s3File)
        {
            string ret = GetPreSignedURL(s3File);
            ret = ret.Substring(0, ret.IndexOf("?"));
            return ret;
        }
    }
}
