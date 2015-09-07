using System;

namespace AmazonS3CreatePreSignedUrlExample
{
    /// <summary>
    /// Example code from : http://gauravmantri.com/2014/01/06/create-pre-signed-url-using-c-for-uploading-large-files-in-amazon-s3/#comment-21241
    /// </summary>
    class Program
    {
        static void Main(string[] args)
        {
            string accessKey = "*"; // fill in the accessKey
            string secretKey = "*"; // fill in the secretKey

            string filePath = @"D:\test.dat";
            var requestUri = new Uri("https://*.s3.amazonaws.com/test.dat"); // fill in the correct url
            var expiryDate = DateTime.UtcNow.AddDays(8); // Expire the link 8 days in the future

            var m = new MultipartUploadHelper(accessKey, secretKey);
            string signedGetUrl = m.UploadFile(filePath, requestUri, expiryDate, (part, bytesToBeUploaded) =>
            {
                Console.WriteLine("part[{0}] : bytesToBeUploaded={1}", part, bytesToBeUploaded);
            });

            Console.WriteLine("The signedUrl = {0}\r\nexpires at {1}", signedGetUrl, expiryDate);

            Console.WriteLine("File uploaded successfully. Press any key to terminate the application.");
            Console.ReadLine();
        }
    }
}