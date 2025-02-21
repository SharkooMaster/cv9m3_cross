
public class M_EncodedResult
{
    public ulong bucket_id { get; set; }
    public ulong row_id { get; set; }
    public Dictionary<int, int> error_encoding = new Dictionary<int, int>();
}
