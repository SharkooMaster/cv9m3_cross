

using Cross.Services.Cross;
using CrossService;
using Google.Protobuf;
using Grpc.Core;

public class CompressFileService : FileService.FileServiceBase
{
    public override async Task<FileResponse> ProcessFile(FileRequest request, ServerCallContext context)
    {
        Console.WriteLine("REQUEST RECIEVED");
        Cross.Services.Cross.CrossService crossService = new Cross.Services.Cross.CrossService();

        FileResponse to_return = new FileResponse();
        to_return.FileContent = ByteString.CopyFrom(await crossService.CompressFile(request.FileContent.ToByteArray()));
        return to_return;
    }
}
