using S3.FS;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Demo
{
    class Program
    {
        private static S3FService service;

        static void Main()
        {
            try
            {
                RunAsync().Wait();
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(ex.Message);
                Console.ResetColor();
            }

            Console.Write("Press any key to exit...");
            Console.Read();
        }

        static async Task RunAsync()
        {
            Console.Write("Enter Endpoint: ");
            string endpoint = Console.ReadLine();

            Console.Write("Enter Access Key: ");
            string accessKey = Console.ReadLine();

            Console.Write("Enter Secret: ");
            string secret = Console.ReadLine();

            Console.Write("Enter Bucket: ");
            string bucket = Console.ReadLine();

            Console.WriteLine();
            Console.WriteLine("Beginning Scan");
            Console.WriteLine();

            service = new S3FService(endpoint, accessKey, secret);
            var buckets = await service.GetBucketsAsync();
            var demoBucket = buckets.FirstOrDefault(item => item.IsBucket && item.Name == bucket);
            await Print(demoBucket, 0);
        }

        static async Task Print(FSObject parent, int indent)
        {
            Console.WriteLine(new string(' ', indent) + "/" + parent.Name);
            await service.LoadChildrenAsync(parent);
            foreach (var folder in parent.Folders)
                await Print(folder, indent + 2);
            foreach (var file in parent.Files)
                Console.WriteLine(new string(' ', indent + 2) + "- " + file.Name);
        }
    }
}
