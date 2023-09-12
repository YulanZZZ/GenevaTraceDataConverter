using CsvHelper;
using CsvHelper.Configuration;
using CsvHelper.Configuration.Attributes;
using System.Globalization;
using System.Text.Json;

internal class RawSpan
{
    [Name("env_cloud_role")]
    public string ServiceName { get; set; }
    [Name("env_dt_spanId")]
    public string SpanId { get; set; }
    [Name("env_dt_traceId")]
    public string TraceId { get; set; }
    [Name("startTime")]
    public string StartTimeUtc { get; set; }
    [Name("env_time")]
    public string EndTimeUtc { get; set; }
    [Name("kind")]
    public string Kind { get; set; }
    [Name("name")]
    public string DisplayName { get; set; }
    [Name("name")]
    public string OperationName { get; set; }
    [Name("parentId")]
    public string ParentId { get; set; }
    [Name("success")]
    public string Success { get; set; }
    [Name("env_name")]
    public string Source { get; set; }
    [Name("http.host")]
    public string HttpHost { get; set; }
    [Name("httpMethod")]
    public string HttpMethod { get; set; }
    [Name("httpStatusCode")]
    public string HttpStatusCode { get; set; }
    [Name("httpUrl")]
    public string HttpUrl { get; set; }
    [Name("http.request.header.ms_cv")]
    public string HttpRequestHeaderMsCv { get; set; }
    [Name("http.request.header.scenariotag")]
    public string HttpRequestHeaderScenariotag { get; set; }
    [Name("http.response.header.request_id")]
    public string HttpResponseHeaderRequestId { get; set; }
    [Name("http.response.header.x_beserver")]
    public string HttpResponseHeaderXBeserver { get; set; }
    [Name("http.response.header.x_diaginfo")]
    public string HttpResponseHeaderXDiaginfo { get; set; }
    [Name("http.target")]
    public string HttpTarget { get; set; }
    [Name("http.user_agent")]
    public string HttpUserAgent { get; set; }
    [Name("http.flavor")]
    public string HttpFlavor { get; set; }
    [Name("http.scheme")]
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
        Status = bool.Parse(rawSpan.Success) ? "success" : "fail";
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
