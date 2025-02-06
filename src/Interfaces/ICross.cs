
namespace Cross.Interfaces.Cross;

public interface ICross
{
    public Task<byte[]> CompressFile(byte[] _file);
    public Task<byte[]> DecompressFile(byte[] _file);
}
