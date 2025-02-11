
using System.Data;
using System.Text.Json;
using Cross.Utilities;
using Google.Protobuf;
using Cross.Interfaces.Agneta;
using Cross.Models;
using System.Net.WebSockets;
using System.Text;
using Newtonsoft.Json;

namespace Cross.Services.Agneta
{
    public class CloseSocketMessage
    {
        public string cmd {get;set;}
    }

    public class AgnetaClientService : IAgnetaClientService
    {
        private readonly HttpClient _client;
        private readonly string _url;
        private readonly Uri _uri;
        private ClientWebSocket _client_ws;

        public AgnetaClientService(string uri)
        {
            try
            {
                _client_ws = new ClientWebSocket();

                var handler = new HttpClientHandler
                {
                    ServerCertificateCustomValidationCallback = 
                        HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
                };

                _client = new HttpClient(handler);
                _url = "https://agneta-loadbalancer.default.svc.cluster.local:443";
                _uri = new Uri(uri);
            }
            catch (Exception ex)
            {
                // Log or handle the detailed exception information
                Console.WriteLine($"Failed to create AgnetaClientService: {ex}");

                // Optionally rethrow to allow higher-level handling
                throw;
            }
        }
        
        //public async Task SendUsageStatistics()
        //{
        //    Dictionary<string, string> formData = new Dictionary<string, string>();
        //    formData.Add("node_type", "agent");
        //    formData.Add("load_score", Misc.GetMemoryUsagePercentage().ToString());
        //    formData.Add("data", Misc.GetServiceInfo("agent", Globals.ETCD_ID));
        //    formData.Add("max_cnt", "5");

        //    var content = new FormUrlEncodedContent(formData);

        //    var request = new HttpRequestMessage(HttpMethod.Post, $"{_url}/lbs/add")
        //    {
        //        Content = content
        //    };

        //    var response = await _client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
        //    
        //    if(!response.IsSuccessStatusCode)
        //    {
        //        Console.WriteLine($"ERROR::AgnetaClientService: Failed to send usage statistics -> {Globals.ETCD_ID}");
        //    }
        //}

        public async Task ConnectAsync()
        {
            if (_client_ws.State != WebSocketState.Open)
            {
                try
                {
                    _client_ws.Options.RemoteCertificateValidationCallback = 
                        (sender, certificate, chain, sslPolicyErrors) => true;
        
                    await _client_ws.ConnectAsync(_uri, CancellationToken.None);
                }
                catch (Exception ex)
                {
                    // Log or handle the detailed exception information
                    Console.WriteLine($"Failed to connect: {ex}");
                    
                    // Optionally rethrow to allow higher-level handling
                    throw;
                }
            }
        }

        public async Task SendMessageAsync(string message)
        {
            if(_client_ws.State != WebSocketState.Open)
            {
                await ConnectAsync();
            }

            var buffer = Encoding.UTF8.GetBytes(message);
            await _client_ws.SendAsync(new ArraySegment<byte>(buffer), WebSocketMessageType.Text, true, CancellationToken.None);
            // Console.WriteLine($"Log sent to Agneta: {message}");
        }

        public async Task<string> RecieveMessageAsync()
        {
            var buffer = new byte[1024];
            var result = await _client_ws.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
            return Encoding.UTF8.GetString(buffer, 0, result.Count);
        }

        public async Task SendCloseAsync()
        {
            CloseSocketMessage csm = new CloseSocketMessage(){ cmd = "unsubscribe_logs" };
            await SendMessageAsync(JsonConvert.SerializeObject(csm));
        }
    }
}