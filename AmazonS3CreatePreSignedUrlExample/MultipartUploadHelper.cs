using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Net;
using System.Text;
using System.Web;
using System.Xml.Linq;

namespace AmazonS3CreatePreSignedUrlExample
{
    /// <summary>
    /// http://gauravmantri.com/2014/01/06/create-pre-signed-url-using-c-for-uploading-large-files-in-amazon-s3/#comment-21241
    /// 
    /// **How Large Files are Uploaded in Amazon S3**
    /// Before we talk about using Query String authentication in Amazon S3, let’s take a moment and talk about how large files are uploaded in Amazon S3 and then we will focus on the issue at hand.
    /// 
    /// In order to upload large files, Amazon S3 provides a mechanism called “Multi Part Upload”. Using this mechanism, essentially you chunk a large file into smaller pieces (called “Parts” in Amazon S3 terminology) and upload these chunks. Once all parts are uploaded, you tell Amazon S3 to join these files together and create the desired object.
    /// 
    /// To do a “Multi Part Upload”, one would go through the following steps:
    /// **Initiate Multipart Upload**
    /// This is where you basically tell Amazon S3 that you will be uploading a large file. When Amazon S3 receives this request, it sends back an “Upload Id” which you have to use in subsequent requests.
    /// To learn more about this process, please click here: http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadInitiate.html.
    /// 
    /// **Upload Part** 
    /// This is where you split the large file into parts and upload these parts. A few things to remember are:
    /// Each part must be at least 5 MB in size with the exception of last part.
    /// Each part is assigned a sequential part number (starting from 1) and there can be a maximum of ten thousand (10,000) parts.
    /// In other words maximum number of parts in which a file can be split is ten thousand.
    /// To learn more about this process, please click here: http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPart.html.
    /// 
    /// **Complete Multipart Upload**
    /// Once all parts are uploaded, using this step you basically tell Amazon S3 to join all the parts together to create the object.
    /// To learn more about this process, please click here: http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadComplete.html.
    /// </summary>
    public class MultipartUploadHelper
    {
        private const int FiveMegaBytes = 5 * 1024 * 1024; // Minimum S3 upload-size
        private readonly long _jan1St1970Ticks = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;

        private readonly string _accessKey;
        private readonly string _secretKey;

        public MultipartUploadHelper(string accessKey, string secretKey)
        {
            _accessKey = accessKey;
            _secretKey = secretKey;
        }

        public string UploadFile(string filePath, Uri requestUri, DateTime expiryDate, Action<int, int> callbackForPartDone = null)
        {
            string uploadId = InitiateMultipartUpload(_accessKey, _secretKey, requestUri, DateTime.UtcNow, "application/x-msdownload", null);

            var partNumberETags = UploadParts(_accessKey, _secretKey, requestUri, uploadId, filePath, expiryDate, callbackForPartDone);

            return FinishMultipartUpload(_accessKey, _secretKey, requestUri, uploadId, partNumberETags, expiryDate);
        }

        private string InitiateMultipartUpload(string accessKey, string secretKey, Uri requestUri, DateTime requestDate, string contentType, NameValueCollection requestHeaders)
        {
            string uploadId;
            var uploadIdRequestUrl = new Uri(string.Format("{0}?uploads=", requestUri.AbsoluteUri));
            var uploadIdRequestUrlRequestHeaders = new NameValueCollection();

            if (requestHeaders != null)
            {
                for (int i = 0; i < requestHeaders.Count; i++)
                {
                    var key = requestHeaders.Keys[i];
                    var value = requestHeaders[key];
                    if (key.StartsWith("x-amz-", StringComparison.InvariantCultureIgnoreCase))
                    {
                        uploadIdRequestUrlRequestHeaders.Add(key, value);
                    }
                }
            }

            var stringToSign = SignatureHelper.CreateStringToSign(uploadIdRequestUrl, "POST", string.Empty, contentType, requestDate, requestHeaders);
            var signatureForUploadId = SignatureHelper.CreateSignature(secretKey, stringToSign);
            uploadIdRequestUrlRequestHeaders.Add("Authorization", string.Format("AWS {0}:{1}", accessKey, signatureForUploadId));

            var request = (HttpWebRequest)WebRequest.Create(uploadIdRequestUrl);
            request.Method = "POST";
            request.ContentLength = 0;
            request.Date = requestDate;
            request.ContentType = contentType;
            request.Headers.Add(uploadIdRequestUrlRequestHeaders);

            using (var resp = (HttpWebResponse)request.GetResponse())
            {
                using (var s = new StreamReader(resp.GetResponseStream()))
                {
                    var response = s.ReadToEnd();
                    XElement xe = XElement.Parse(response);
                    uploadId = xe.Element(XName.Get("UploadId", "http://s3.amazonaws.com/doc/2006-03-01/")).Value;
                }
            }

            return uploadId;
        }

        private Dictionary<int, string> UploadParts(string accessKey, string secretKey, Uri requestUri, string uploadId, string filePath, DateTime expiryDate, Action<int, int> callbackForPartDone = null)
        {
            var partNumberETags = new Dictionary<int, string>();
            var timeSpan = new TimeSpan(expiryDate.Ticks - _jan1St1970Ticks);
            long expiry = Convert.ToInt64(timeSpan.TotalSeconds);
            var fileContents = File.ReadAllBytes(filePath);

            int partNumber = 1;
            int startPosition = 0;
            int bytesToBeUploaded = fileContents.Length;

            do
            {
                int bytesToUpload = Math.Min(FiveMegaBytes, bytesToBeUploaded);

                if (callbackForPartDone != null)
                {
                    callbackForPartDone(partNumber, bytesToBeUploaded);
                }

                var partUploadUrl = new Uri(string.Format("{0}?uploadId={1}&partNumber={2}", requestUri.AbsoluteUri, HttpUtility.UrlEncode(uploadId), partNumber));
                string partUploadSignature = SignatureHelper.CreateSignature(secretKey, SignatureHelper.CreateStringToSign(partUploadUrl, "PUT", string.Empty, string.Empty, expiry, null));
                var partUploadPreSignedUrl = new Uri(string.Format("{0}?uploadId={1}&partNumber={2}&AWSAccessKeyId={3}&Signature={4}&Expires={5}", requestUri.AbsoluteUri,
                    HttpUtility.UrlEncode(uploadId), partNumber, accessKey, HttpUtility.UrlEncode(partUploadSignature), expiry));

                var request = (HttpWebRequest)WebRequest.Create(partUploadPreSignedUrl);
                request.Method = "PUT";
                request.Timeout = 1000 * 600;
                request.ContentLength = bytesToUpload;

                using (var stream = request.GetRequestStream())
                {
                    stream.Write(fileContents, startPosition, bytesToUpload);
                }

                using (var resp = (HttpWebResponse)request.GetResponse())
                {
                    using (var streamReader = new StreamReader(resp.GetResponseStream()))
                    {
                        partNumberETags.Add(partNumber, resp.Headers["ETag"]);
                    }
                }

                bytesToBeUploaded = bytesToBeUploaded - bytesToUpload;
                startPosition = bytesToUpload;

                partNumber = partNumber + 1;

            }
            while (bytesToBeUploaded > 0);

            return partNumberETags;
        }

        private string FinishMultipartUpload(string accessKey, string secretKey, Uri requestUri, string uploadId, Dictionary<int, string> partNumberETags, DateTime expiryDate)
        {
            var timeSpan = new TimeSpan(expiryDate.Ticks - _jan1St1970Ticks);
            long expiry = Convert.ToInt64(timeSpan.TotalSeconds);
            var finishOrCancelMultipartUploadUri = new Uri(string.Format("{0}?uploadId={1}", requestUri.AbsoluteUri, uploadId));
            string signatureForFinishMultipartUpload = SignatureHelper.CreateSignature(secretKey, SignatureHelper.CreateStringToSign(finishOrCancelMultipartUploadUri, "POST", string.Empty, "text/plain", expiry, null));

            var finishMultipartUploadUrl = new Uri(string.Format("{0}?uploadId={1}&AWSAccessKeyId={2}&Signature={3}&Expires={4}", requestUri.AbsoluteUri, HttpUtility.UrlEncode(uploadId), accessKey, HttpUtility.UrlEncode(signatureForFinishMultipartUpload), expiry));

            var payload = new StringBuilder();
            payload.Append("<?xml version=\"1.0\" encoding=\"utf-8\"?><CompleteMultipartUpload>");
            foreach (var item in partNumberETags)
            {
                payload.AppendFormat("<Part><PartNumber>{0}</PartNumber><ETag>{1}</ETag></Part>", item.Key, item.Value);
            }
            payload.Append("</CompleteMultipartUpload>");

            var requestPayload = Encoding.UTF8.GetBytes(payload.ToString());

            var request = (HttpWebRequest)WebRequest.Create(finishMultipartUploadUrl);
            request.Method = "POST";
            request.ContentType = "text/plain";
            request.ContentLength = requestPayload.Length;

            using (var stream = request.GetRequestStream())
            {
                stream.Write(requestPayload, 0, requestPayload.Length);
            }
            using (var resp = (HttpWebResponse)request.GetResponse())
            {
            }

            string signatureForGet = SignatureHelper.CreateSignature(secretKey, SignatureHelper.CreateStringToSign(new Uri(requestUri.AbsoluteUri), "GET", string.Empty, string.Empty, expiry, null));
            string signedGetUrl = string.Format("{0}?AWSAccessKeyId={1}&Signature={2}&Expires={3}", requestUri.AbsoluteUri, accessKey, HttpUtility.UrlEncode(signatureForGet), expiry);
            return signedGetUrl;
        }
    }
}