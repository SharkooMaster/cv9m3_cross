namespace Cross.Interfaces.Agneta
{
    public interface IAgnetaClientService
    {
        // Task SendUsageStatistics();

        Task ConnectAsync();
        Task SendMessageAsync(string message);
        Task<string> RecieveMessageAsync();
        Task SendCloseAsync();
    }
}