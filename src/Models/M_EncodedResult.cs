
public class M_EncodedResult
{
    public ulong bucket_id { get; set; }
    public ulong row_id { get; set; }
    public List<(int key, int value)> error_encoding = new List<(int, int)>();
}
