using CsvHelper;
using CsvHelper.Configuration;
using CsvHelper.Configuration.Attributes;
using System.Globalization;
using System.Text.Json;

/*
    * 0    TIMESTAMP	
    * 1    PreciseTimeStamp	
    * 2    Tenant	
    * 3    Role	
    * 4    DeployRing	
    * 5    Forest	
    * 6    DAG	
    * 7    RoleInstance	
    * 8    env_cloud_role	
    * 9    env_cloud_roleInstance	
    * 10   env_dt_spanId	
    * 11   env_dt_traceId	
    * 12   env_name	
    * 13   env_time	
    * 14   env_ver	
    * 15   kind	
    * 16   name	
    * 17   parentId	
    * 18   startTime	
    * 19   success	
    * 20   RowKey	
    * 21   __AuthType__	
    * 22   __AuthIdentity__	
    * 23   __SourceEvent__	
    * 24   __SourceMoniker__	
    * 25   http.host	
    * 26   httpMethod	
    * 27   httpStatusCode	
    * 28   httpUrl	
    * 29   net.peer.name	
    * 30   net.peer.port	
    * 31   rpc.grpc.status_code	
    * 32   rpc.method	
    * 33   rpc.service	
    * 34   rpc.system	
    * 35   ApplicationName	
    * 36   ApplicationVersion	
    * 37   CafeHopCount
    * 38   ClientRequestId	
    * 39   Clique	
    * 40   DatabaseAvailabilityGroup	
    * 41   Datacenter	
    * 42   FrontEndServerName	
    * 43   IsFromCafe	
    * 44   IsSynthetic	
    * 45   IsTestServer	
    * 46   Machine	
    * 47   OperationName	
    * 48   OsVersion	
    * 49   Region	
    * 50   RequestSizeBytes	
    * 51   RoutingSessionKeyMatchesAnchorMailbox	
    * 52   RuntimeFramework	RuntimeFrameworkDescription	
    * 53   ServerRequestId	
    * 54   UserAgent	
    * 55   ResponseContentLength	
    * 56   http.path	
    * 57   http.route	
    * 58   ResponseContentType	
    * 59   ClientApplicationId	
    * 60   ClientApplicationName	
    * 61   ConsolidatedClientAppName	
    * 62   IsConsumer	
    * 63   MsAppName	
    * 64   TenantId	
    * 65   processorName	
    * 66   processorType	
    * 67   network.protocol.version	
    * 68   SubScenario	
    * 69   http.request.header.ms_cv	
    * 70   http.request.header.scenariotag	
    * 71   http.response.header.request_id	
    * 72   http.response.header.x_beserver	
    * 73   http.response.header.x_diaginfo	
    * 74   clientRequestId	
    * 75   env_mscv_cV	
    * 76   requestId	
    * 77   http.target	
    * 78   http.user_agent	
    * 79   net.peer.ip	
    * 80   http.response.header.ms_cv	
    * 81   http.response.header.x_feserver	
    * 82   statusMessage	
    * 83   http.flavor	
    * 84   http.scheme	
    * 85   env_cloud_roleVer	
    * 86   cloud.deploymentUnit	
    * 87   cloud.environment	
    * 88   cloud.location	
    * 89   cloud.name	
    * 90   recipient.id	
    * 91   tenant.domain	
    * 92   tenant.id
    */
internal class RawSpan
{
    [Index(8)]
    public string ServiceName { get; set; }
    [Index(10)]
    public string SpanId { get; set; }
    [Index(11)]
    public string TraceId { get; set; }
    [Index(18)]
    public string StartTimeUtc { get; set; }
    [Index(13)]
    public string EndTimeUtc { get; set; }
    [Index(15)]
    public string Kind { get; set; }
    [Index(16)]
    public string DisplayName { get; set; }
    [Index(16)]
    public string OperationName { get; set; }
    [Index(17)]
    public string ParentId { get; set; }
    [Index(19)]
    public string Success { get; set; }
    [Index(12)]
    public string Source { get; set; }
    [Index(25)]
    public string HttpHost { get; set; }
    [Index(26)]
    public string HttpMethod { get; set; }
    [Index(27)]
    public string HttpStatusCode { get; set; }
    [Index(28)]
    public string HttpUrl { get; set; }
    [Index(69)]
    public string HttpRequestHeaderMsCv { get; set; }
    [Index(70)]
    public string HttpRequestHeaderScenariotag { get; set; }
    [Index(71)]
    public string HttpResponseHeaderRequestId { get; set; }
    [Index(72)]
    public string HttpResponseHeaderXBeserver { get; set; }
    [Index(73)]
    public string HttpResponseHeaderXDiaginfo { get; set; }
    [Index(77)]
    public string HttpTarget { get; set; }
    [Index(78)]
    public string HttpUserAgent { get; set; }
    [Index(83)]
    public string HttpFlavor { get; set; }
    [Index(84)]
    public string HttpScheme { get; set; }
    public RawSpan() { }
}

internal class Tags
{
    public string HttpHost { get; set; }
    public string HttpMethod { get; set; }
    public string HttpStatusCode { get; set; }
    public string HttpUrl { get; set; }
    public string HttpRequestHeaderMsCv { get; set; }
    public string HttpRequestHeaderScenariotag { get; set; }
    public string HttpResponseHeaderRequestId { get; set; }
    public string HttpResponseHeaderXBeserver { get; set; }
    public string HttpResponseHeaderXDiaginfo { get; set; }
    public string HttpTarget { get; set; }
    public string HttpUserAgent { get; set; }
    public string HttpFlavor { get; set; }
    public string HttpScheme { get; set; }
    public Tags(RawSpan rawSpan)
    {
        HttpHost = rawSpan.HttpHost;
        HttpMethod = rawSpan.HttpMethod;
        HttpStatusCode = rawSpan.HttpStatusCode;
        HttpUrl = rawSpan.HttpUrl;
        HttpRequestHeaderMsCv = rawSpan.HttpRequestHeaderMsCv;
        HttpRequestHeaderScenariotag = rawSpan.HttpRequestHeaderScenariotag;
        HttpResponseHeaderRequestId = rawSpan.HttpResponseHeaderRequestId;
        HttpResponseHeaderXBeserver = rawSpan.HttpResponseHeaderXBeserver;
        HttpResponseHeaderXDiaginfo = rawSpan.HttpResponseHeaderXDiaginfo;
        HttpTarget = rawSpan.HttpTarget;
        HttpUserAgent = rawSpan.HttpUserAgent;
        HttpFlavor = rawSpan.HttpFlavor;
        HttpScheme = rawSpan.HttpScheme;
    }
}
internal class Span
{
    public string ServiceName { get; set; }
    public string SpanId { get; set; }
    public string TraceId { get; set; }
    public string StartTimeUtc { get; set; }
    public string Duration { get; set; }
    public string Kind { get; set; }
    public string DisplayName { get; set; }
    public string OperationName { get; set; }
    public string ParentId { get; set; }
    public string Status { get; set; }
    public string Tags { get; set; }
    public string Events { get; set; }
    public string Links { get; set; }
    public string Source { get; set; }
    public Span(RawSpan rawSpan)
    {
        ServiceName = rawSpan.ServiceName;
        SpanId = rawSpan.SpanId;
        TraceId = rawSpan.TraceId;
        StartTimeUtc = rawSpan.StartTimeUtc;
        TimeSpan span = DateTime.Parse(rawSpan.EndTimeUtc).Subtract(DateTime.Parse(rawSpan.StartTimeUtc));
        Duration = Convert.ToInt32(span.TotalMilliseconds * 1000).ToString();
        Kind = rawSpan.Kind;
        DisplayName = rawSpan.DisplayName;
        OperationName = rawSpan.OperationName;
        ParentId = rawSpan.ParentId;
        Status = rawSpan.Success == "TRUE" ? "success" : "fail";
        Source = rawSpan.Source;
        Events = "";
        Links = "";
        Tags = JsonSerializer.Serialize(new Tags(rawSpan));
    }

    public override string ToString()
    {
        return $"{ServiceName}\t{SpanId}\t{TraceId}\t{StartTimeUtc}\t{Duration}\t{Kind}\t{DisplayName}\t{OperationName}\t{ParentId}\t{Status}\t{Tags}\t{Events}\t{Links}\t{Source}";
    }
}

internal class Converter
{
    readonly static private string InputPath = "input.csv";
    readonly static private string OutputPath = "output.tsv";
    readonly static private HashSet<string> parents = new();
    readonly static private List<Span> allSpans = new();
    readonly static private Stack<Span> filteredSpans = new();

    private static void GenerateOutputCSV()
    {
        using StreamWriter sw = new(OutputPath);
        sw.WriteLine("ServiceName\tSpanId\tTraceId\tStartTimeUtc\tDuration\tKind\tDisplayName\tOperationName\tParentId\tStatus\tTags\tEvents\tLinks\tSource");
        while (filteredSpans.Count > 0)
        {
            sw.WriteLine(filteredSpans.Pop());
        }
    }

    private static void ParseInputCSV()
    {
        var config = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            Delimiter = ",",
            HasHeaderRecord = true,
            HeaderValidated = null,
            MissingFieldFound = null
        };
        using var sr = new StreamReader(InputPath);
        using var csv = new CsvReader(sr, config);
        var records = csv.GetRecords<RawSpan>();
        foreach (var record in records)
        {
            Span span = new(record);
            allSpans.Add(span);
        }
    }

    private static void FilterAndDistinct()
    {
        int count = allSpans.Count();
        for (int i = count - 1; i >= 1; i--)
        {
            Span span = allSpans[i];
            if (span.ParentId == "" || parents.Contains(span.ParentId))
            {
                if (!parents.Contains(span.SpanId))
                {
                    parents.Add(span.SpanId); // distinct
                    filteredSpans.Push(span);
                }
            }
        }
    }

    static void Main(string[] args)
    {
        ParseInputCSV();
        FilterAndDistinct();
        GenerateOutputCSV();
    }
}
