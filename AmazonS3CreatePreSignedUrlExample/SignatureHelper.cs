using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Web;

namespace AmazonS3CreatePreSignedUrlExample
{
    /// <summary>
    /// http://gauravmantri.com/2014/01/06/create-pre-signed-url-using-c-for-uploading-large-files-in-amazon-s3/#comment-21241
    /// 
    /// First thing that we need to do is write code to create authorization header.
    /// To learn more about how to create authorization header, please click here: http://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html.
    /// If you go through the documentation, you will realize that in order to create authorization header, there are a few things you would need to do:
    /// Create “CanonicalizedResource” Element String
    /// Create “CanonicalizedAmzHeaders” Element String
    /// Create “StringToSign String
    /// Create Signature
    /// </summary>
    public static class SignatureHelper
    {
        private static readonly string[] SubResourcesToConsider = { "acl", "lifecycle", "location", "logging", "notification", "partNumber", "policy", "requestPayment", "torrent", "uploadId", "uploads", "versionId", "versioning", "versions", "website", };
        private static readonly string[] OverrideResponseHeadersToConsider = { "response-content-type", "response-content-language", "response-expires", "response-cache-control", "response-content-disposition", "response-content-encoding" };

        /// <summary>
        /// Creates the string to sign (for regular authorization header).
        /// </summary>
        /// <param name="requestUri">The request URI.</param>
        /// <param name="httpVerb">The HTTP verb.</param>
        /// <param name="contentMD5">The content m d5.</param>
        /// <param name="contentType">Type of the content.</param>
        /// <param name="date">The date.</param>
        /// <param name="requestHeaders">The request headers.</param>
        /// <returns></returns>
        public static string CreateStringToSign(Uri requestUri, string httpVerb, string contentMD5, string contentType, DateTime date, NameValueCollection requestHeaders)
        {
            var canonicalizedResourceString = GetCanonicalizedResourceString(requestUri);
            var canonicalizedAmzHeadersString = CreateCanonicalizedAmzHeadersString(requestHeaders);
            var dateInStringFormat = date.ToString("R");

            if (requestHeaders != null && requestHeaders.AllKeys.Contains("x-amz-date"))
            {
                dateInStringFormat = string.Empty;
            }

            var stringToSign = string.Format("{0}\n{1}\n{2}\n{3}\n{4}{5}", httpVerb, contentMD5, contentType, dateInStringFormat, canonicalizedAmzHeadersString, canonicalizedResourceString);
            return stringToSign;
        }

        /// <summary>
        /// Creates the string to sign (for Pre Signed URL authorization header).
        /// </summary>
        /// <param name="requestUri">The request URI.</param>
        /// <param name="httpVerb">The HTTP verb.</param>
        /// <param name="contentMD5">The content m d5.</param>
        /// <param name="contentType">Type of the content.</param>
        /// <param name="secondsSince1stJan1970">The seconds since1st jan1970.</param>
        /// <param name="requestHeaders">The request headers.</param>
        /// <returns></returns>
        public static string CreateStringToSign(Uri requestUri, string httpVerb, string contentMD5, string contentType, long secondsSince1stJan1970, NameValueCollection requestHeaders)
        {
            var canonicalizedResourceString = GetCanonicalizedResourceString(requestUri);
            var canonicalizedAmzHeadersString = CreateCanonicalizedAmzHeadersString(requestHeaders);
            var stringToSign = string.Format("{0}\n{1}\n{2}\n{3}\n{4}{5}", httpVerb, contentMD5, contentType, secondsSince1stJan1970, canonicalizedAmzHeadersString, canonicalizedResourceString);
            return stringToSign;
        }

        public static string GetCanonicalizedResourceString(Uri requestUri)
        {
            string host = requestUri.DnsSafeHost;
            var hostElementsArray = host.Split('.');
            string bucketName = "";
            if (hostElementsArray.Length > 3)
            {
                var sb = new StringBuilder();
                for (int i = 0; i < hostElementsArray.Length - 3; i++)
                {
                    sb.AppendFormat("{0}.", hostElementsArray[i]);
                }
                bucketName = sb.ToString();
                if (bucketName.Length > 0)
                {
                    if (bucketName.EndsWith("."))
                    {
                        bucketName = bucketName.Substring(0, bucketName.Length - 1);
                    }
                    bucketName = string.Format("/{0}", bucketName);
                }
            }

            var subResourcesList = SubResourcesToConsider.ToList();
            var overrideResponseHeadersList = OverrideResponseHeadersToConsider.ToList();
            StringBuilder canonicalizedResourceStringBuilder = new StringBuilder();
            canonicalizedResourceStringBuilder.Append(bucketName);
            canonicalizedResourceStringBuilder.Append(requestUri.AbsolutePath);

            var queryVariables = HttpUtility.ParseQueryString(requestUri.Query);

            var queryVariablesToConsider = new SortedDictionary<string, string>();
            var overrideResponseHeaders = new SortedDictionary<string, string>();

            if (queryVariables.Count > 0)
            {
                var numQueryItems = queryVariables.Count;
                for (int i = 0; i < numQueryItems; i++)
                {
                    var key = queryVariables.GetKey(i);
                    var value = queryVariables[key];
                    if (subResourcesList.Contains(key))
                    {
                        if (queryVariablesToConsider.ContainsKey(key))
                        {
                            var val = queryVariablesToConsider[key];
                            queryVariablesToConsider[key] = string.Format("{0},{1}", value, val);
                        }
                        else
                        {
                            queryVariablesToConsider.Add(key, value);
                        }
                    }

                    if (overrideResponseHeadersList.Contains(key))
                    {
                        overrideResponseHeaders.Add(key, HttpUtility.UrlDecode(value));
                    }
                }
            }

            if (queryVariablesToConsider.Count > 0 || overrideResponseHeaders.Count > 0)
            {
                StringBuilder queryStringInCanonicalizedResourceString = new StringBuilder();
                queryStringInCanonicalizedResourceString.Append("?");
                for (int i = 0; i < queryVariablesToConsider.Count; i++)
                {
                    var key = queryVariablesToConsider.Keys.ElementAt(i);
                    var value = queryVariablesToConsider.Values.ElementAt(i);
                    if (!string.IsNullOrWhiteSpace(value))
                    {
                        queryStringInCanonicalizedResourceString.AppendFormat("{0}={1}&", key, value);
                    }
                    else
                    {
                        queryStringInCanonicalizedResourceString.AppendFormat("{0}&", key);
                    }
                }

                for (int i = 0; i < overrideResponseHeaders.Count; i++)
                {
                    var key = overrideResponseHeaders.Keys.ElementAt(i);
                    var value = overrideResponseHeaders.Values.ElementAt(i);
                    queryStringInCanonicalizedResourceString.AppendFormat("{0}={1}&", key, value);
                }

                var str = queryStringInCanonicalizedResourceString.ToString();
                if (str.EndsWith("&"))
                {
                    str = str.Substring(0, str.Length - 1);
                }

                canonicalizedResourceStringBuilder.Append(str);
            }

            return canonicalizedResourceStringBuilder.ToString();
        }

        /// <summary>
        /// To create CanonicalizedAmzHeaders Element String, you would need all the headers which will be included in the request.
        /// Based on the documentation, only the headers starting with “x-amz-“ will be considered in this function though.
        /// </summary>
        /// <param name="requestHeaders">The request headers.</param>
        /// <returns></returns>
        public static string CreateCanonicalizedAmzHeadersString(NameValueCollection requestHeaders)
        {
            string canonicalizedAmzHeadersString = string.Empty;

            if (requestHeaders != null && requestHeaders.Count > 0)
            {
                StringBuilder sb = new StringBuilder();
                SortedDictionary<string, string> sortedRequestHeaders = new SortedDictionary<string, string>();
                var requestHeadersCount = requestHeaders.Count;

                for (int i = 0; i < requestHeadersCount; i++)
                {
                    var key = requestHeaders.Keys.Get(i);
                    var value = requestHeaders[key].Trim();
                    key = key.ToLowerInvariant();
                    if (key.StartsWith("x-amz-", StringComparison.InvariantCultureIgnoreCase))
                    {
                        if (sortedRequestHeaders.ContainsKey(key))
                        {
                            var val = sortedRequestHeaders[key];
                            sortedRequestHeaders[key] = string.Format("{0},{1}", val, value);
                        }
                        else
                        {
                            sortedRequestHeaders.Add(key, value);
                        }
                    }
                }

                if (sortedRequestHeaders.Count > 0)
                {
                    foreach (var item in sortedRequestHeaders)
                    {
                        sb.AppendFormat("{0}:{1}\n", item.Key, item.Value);
                    }
                    canonicalizedAmzHeadersString = sb.ToString();
                }
            }
            return canonicalizedAmzHeadersString;
        }

        /// <summary>
        /// Creates the signature.
        /// </summary>
        /// <param name="secretKey">The secret key.</param>
        /// <param name="stringToSign">The string to sign.</param>
        /// <returns></returns>
        public static string CreateSignature(string secretKey, string stringToSign)
        {
            byte[] dataToSign = Encoding.UTF8.GetBytes(stringToSign);
            using (var hmacsha1 = new HMACSHA1(Encoding.UTF8.GetBytes(secretKey)))
            {
                return Convert.ToBase64String(hmacsha1.ComputeHash(dataToSign));
            }
        }
    }
}