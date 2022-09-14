using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using System.Web;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace GEditIngest
{
    public class Function
    {

        private HttpClient httpClient = null;

        string apiKey = Environment.GetEnvironmentVariable("API_KEY");
        string apiUser = Environment.GetEnvironmentVariable("API_USER");
        string env = Environment.GetEnvironmentVariable("GE_ENV");
        string account = Environment.GetEnvironmentVariable("ACCOUNT");

        IAmazonS3 S3Client { get; set; }

        /// <summary>
        /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
        /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
        /// region the Lambda function is executed in.
        /// </summary>
        public Function()
        {
            S3Client = new AmazonS3Client();
        }

        /// <summary>
        /// Constructs an instance with a preconfigured S3 client. This can be used for testing the outside of the Lambda environment.
        /// </summary>
        /// <param name="s3Client"></param>
        public Function(IAmazonS3 s3Client)
        {
            this.S3Client = s3Client;
        }

        /// <summary>
        /// This method is called for every Lambda invocation. This method takes in an S3 event object and can be used 
        /// to respond to S3 notifications.
        /// </summary>
        /// <param name="evnt"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task<string> FunctionHandler(S3Event evnt, ILambdaContext context)
        {
            var evntStr = JsonConvert.SerializeObject(evnt);
            context.Logger.LogDebug(evntStr);

            var targetBucket = Environment.GetEnvironmentVariable("BUCKET");
            var s3Event = evnt.Records?[0].S3;
            if (s3Event == null || s3Event.Bucket.Name != targetBucket)
            {
                context.Logger.LogLine($"Skipped. Bucket name mismatch (Expected: {targetBucket}, got: {s3Event.Bucket.Name})");
                return null;
            }
            AmazonDynamoDBConfig clientConfig = new AmazonDynamoDBConfig();
            clientConfig.RegionEndpoint = RegionEndpoint.USEast1;
            AmazonDynamoDBClient dynamo = new AmazonDynamoDBClient(clientConfig);
            Table dupeTbl = Table.LoadTable(dynamo, "IngestAntiDupe");
            var entry = await dupeTbl.GetItemAsync(s3Event.Object.ETag);
            if (entry != null)
            {
                context.Logger.LogLine($"Duplicate ETag: {s3Event.Object.ETag} (File key: \"{s3Event.Object.Key})\"");
                return null;
            }

            httpClient = new HttpClient();
            httpClient.Timeout = TimeSpan.FromMinutes(5);
            try
            {
                var strippedFileName = Path.GetFileNameWithoutExtension(s3Event.Object.Key);
                var response = await SearchAsset(strippedFileName);
                var responseJson = await response.ReadAsStringAsync();
                var jobj = JObject.Parse(responseJson);
                var assetCount = int.Parse(jobj["count"].ToString());
                if (assetCount < 1)
                {
                    context.Logger.LogLine($"Search returned 0 assets. Searched for: {strippedFileName}");
                    return null;
                }
                var assets = jobj["assets"].ToList();
                var transferResponse = await TransferAttachment(s3Event.Bucket.Name, s3Event.Object.Key, assets[0]);

                if (transferResponse.IsSuccessStatusCode)
                {
                    context.Logger.LogLine($"Finished transfer: {s3Event.Object.Key} from bucket {s3Event.Bucket.Name}.");
                    var eventEtag = new Document();
                    eventEtag["EventETag"] = s3Event.Object.ETag;
                    eventEtag["Expires"] = ((DateTimeOffset)DateTime.Now.AddMinutes(10)).ToUnixTimeSeconds();
                    await dupeTbl.PutItemAsync(eventEtag);
                    string a = eventEtag["EventETag"];
                    context.Logger.LogLine($"Logged ETag: {eventEtag["EventETag"].AsString()}. Expires: {eventEtag["Expires"].AsString()} UNIX Epoch.");
                }
                else
                {
                    context.Logger.LogLine($"Transfer failed: {s3Event.Object.Key} from bucket {s3Event.Bucket.Name}.");
                    context.Logger.LogLine($"Status code: {transferResponse.StatusCode}");
                    context.Logger.LogLine($"Content string: {await transferResponse.Content.ReadAsStringAsync()}");
                    throw new HttpRequestException();
                }
                return null;
            }
            catch (Exception e)
            {
                context.Logger.LogLine($"Error transferring {s3Event.Object.Key} from bucket {s3Event.Bucket.Name}.");
                context.Logger.LogLine(e.Message);
                context.Logger.LogLine(e.StackTrace);
                throw;
            }
        }
        public async Task<HttpContent> SearchAsset(string assetName)
        {
            //https://dev.globaledit.com/v1/accounts/:accountId/search/assets

            assetName = HttpUtility.UrlDecode(assetName);

            UriBuilder uriBuilder = new UriBuilder();
            uriBuilder.Scheme = "https";
            uriBuilder.Host = env;
            uriBuilder.Path = @$"/v1/accounts/{account}/search/assets";

            var content = new JsonObject();
            content.Add("query", $"search={assetName}&searchMode=all&$searchFields=name&$orderby=search.score()%20desc&$top=5001&$skip=0");
            content.Add("searchType", "workspace");

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                RequestUri = uriBuilder.Uri,
                Headers = {
                    { "x-globaledit-api-key", apiKey },
                    { "x-globaledit-userid", apiUser }
                },
                Content = new StringContent(content.ToString(), Encoding.UTF8, "application/json")
            };
            var responseContent = (await httpClient.SendAsync(request)).Content;
            return responseContent;
        }
        public async void GetMe()
        {
            UriBuilder uriBuilder = new UriBuilder();
            uriBuilder.Scheme = "https";
            uriBuilder.Host = env;
            uriBuilder.Path = @$"/v1/users/me";
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get,
                RequestUri = uriBuilder.Uri,
                Headers = {
                    { "x-globaledit-api-key", apiKey },
                    { "x-globaledit-userid", apiUser }
                },
            };
            var responseContent = (await httpClient.SendAsync(request)).Content;
            var rrrs = await responseContent.ReadAsStringAsync();
        }
        public async Task<HttpResponseMessage> TransferAttachment(string s3Bucket, string s3Key, JToken assetToken)
        {
            var sourceContent = await GetS3Content(s3Bucket, s3Key);

            var attCont = await InitiateAttachmentUpload(s3Key, sourceContent.Headers.ContentLength.ToString(), assetToken);
            var attObj = JObject.Parse(await attCont.ReadAsStringAsync());
            var putUrl = GenerateUploadUrl(attObj, sourceContent.Headers.ContentType.ToString());

            var stream = await sourceContent.ReadAsStreamAsync();
            var sc = new StreamContent(stream);
            sc.Headers.ContentLength = sourceContent.Headers.ContentLength;
            sc.Headers.ContentType = sourceContent.Headers.ContentType;

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Put,
                RequestUri = new Uri(putUrl),
                Content = sc
            };

            return await httpClient.SendAsync(request);
        }
        public async Task<HttpContent> GetS3Content(string s3bucket, string s3key)
        {
            s3key = HttpUtility.UrlDecode(s3key);
            var presignedRequest = new GetPreSignedUrlRequest();
            presignedRequest.BucketName = s3bucket;
            presignedRequest.Key = s3key;
            presignedRequest.Expires = DateTime.Now + TimeSpan.FromMinutes(5);
            var presignedUrl = S3Client.GetPreSignedURL(presignedRequest);

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get,
                RequestUri = new Uri(presignedUrl)
            };
            return (await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead)).Content;
        }
        public async Task<HttpContent> GetAssetSummary(JToken assetToken)
        {
            //https://dev.globaledit.com/v1/assets/:assetId/summary?scopeType=workspace&scopeId=1234

            var assetId = assetToken["id"].ToString();
            var workspaceId = assetToken["workspaceId"].ToString();

            UriBuilder uriBuilder = new UriBuilder();
            uriBuilder.Scheme = "https";
            uriBuilder.Host = env;
            uriBuilder.Path = @$"/v1/assets/{assetId}/summary";
            uriBuilder.Query = $"scopeType=workspace&scopeId={workspaceId}";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get,
                RequestUri = uriBuilder.Uri,
                Headers = {
                    { "x-globaledit-api-key", apiKey },
                    { "x-globaledit-userid", apiUser }
                },
            };
            return (await httpClient.SendAsync(request)).Content;
        }
        public async Task<HttpContent> InitiateAttachmentUpload(string s3Key, string s3Size, JToken assetToken)
        {
            //POST /v1/assets/{assetId}/attachments?scopeType=type&scopeId={id}

            var assetId = assetToken["id"].ToString();
            var workspaceId = assetToken["workspaceId"].ToString();
            var fileSize = long.Parse(s3Size);
            var fileName = s3Key;
            fileName = HttpUtility.UrlDecode(fileName);
            UriBuilder uriBuilder = new UriBuilder();
            uriBuilder.Scheme = "https";
            uriBuilder.Host = env;
            uriBuilder.Path = @$"/v1/assets/{assetId}/attachments";
            uriBuilder.Query = $"scopeType=workspace&scopeId={workspaceId}";

            var content = new JsonObject();
            var upload = new JsonObject();
            upload.Add("fileName", fileName);
            upload.Add("fileSize", fileSize);
            content.Add("upload", upload);

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                RequestUri = uriBuilder.Uri,
                Headers = {
                    { "x-globaledit-api-key", apiKey },
                    { "x-globaledit-userid", apiUser }
                },

                Content = new StringContent(content.ToString(), Encoding.UTF8, "application/json")
            };

            return (await httpClient.SendAsync(request)).Content;
        }
        public string GenerateUploadUrl(JToken uploadObj, string contentType)
        {
            var accessKey = uploadObj["token"]["accessKey"].ToString();
            var secretKey = uploadObj["token"]["secretKey"].ToString();
            var token = uploadObj["token"]["token"].ToString();

            var tmpBucket = uploadObj["s3Bucket"].ToString();
            var tmpKey = uploadObj["file"]["s3Key"].ToString();
            RegionEndpoint region = RegionEndpoint.GetBySystemName(uploadObj["token"]["region"].ToString());

            var putUrl = "";

            SessionAWSCredentials tempCredentials = new SessionAWSCredentials(accessKey, secretKey, token);
            using (var tmpS3Client = new AmazonS3Client(tempCredentials, region))
            {
                GetPreSignedUrlRequest tmpReq = new GetPreSignedUrlRequest();
                tmpReq.BucketName = tmpBucket;
                tmpReq.Key = tmpKey;
                tmpReq.Expires = DateTime.Now + TimeSpan.FromMinutes(60);
                tmpReq.Verb = HttpVerb.PUT;
                tmpReq.ContentType = contentType;
                putUrl = tmpS3Client.GetPreSignedURL(tmpReq);
            }

            return putUrl;
        }
        public async Task<HttpContent> InitiateVersionUpload(JObject summaryObj, string fileName, string fileSize)
        {
            //https://dev.globaledit.com/v1/assets/:assetId/versions?scopeType=workspace&scopeId=1234&folderId=513a3249-792a-eba5-4012-1cca92a02b84

            var assetId = summaryObj["id"];
            var folderId = summaryObj["ancestry"][0]["folderId"].ToString();
            var workspaceId = summaryObj["workspaceId"].ToString();
            var workspaceName = summaryObj["workspaceName"].ToString();

            var uriBuilder = new UriBuilder();
            uriBuilder.Scheme = "https";
            uriBuilder.Host = env;
            uriBuilder.Path = @$"/v1/assets/{assetId}/versions";
            uriBuilder.Query = $"scopeType=workspace&scopeId={workspaceId}&folderId={folderId}";

            var content = new JsonObject();
            content.Add("workspaceId", int.Parse(workspaceId));
            content.Add("workspaceName", workspaceName);
            var upload = new JsonObject();
            upload.Add("fileName", fileName);
            upload.Add("fileSize", fileSize);
            content.Add("upload", upload);

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                RequestUri = uriBuilder.Uri,
                Headers = {
                    { "x-globaledit-api-key", apiKey },
                    { "x-globaledit-userid", apiUser }
                },
                Content = new StringContent(content.ToString(), Encoding.UTF8, "application/json")
            };

            return (await httpClient.SendAsync(request)).Content;
        }
    }
}
