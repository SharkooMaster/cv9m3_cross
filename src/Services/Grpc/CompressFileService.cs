

using CrossService;
using Grpc.Core;

public class CompressFileService : FileService.FileServiceBase
{
    public override async Task<FileResponse> ProcessFile(FileRequest request, ServerCallContext context)
    {
        Console.WriteLine("got a request");
        FileResponse to_return = new FileResponse();
        return to_return;
    }
}
