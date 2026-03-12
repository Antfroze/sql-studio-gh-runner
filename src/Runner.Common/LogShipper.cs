using System;
using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace GitHub.Runner.Common
{
    public static class LogShipper
    {
        private static readonly string _url = Environment.GetEnvironmentVariable("LOG_WS_URL");
        private static readonly ConcurrentQueue<(string, string)> _queue = new();
        private static ClientWebSocket _ws;
        private static readonly SemaphoreSlim _lock = new(1, 1);
        private static bool _started = false;

        public static void Send(string context, string line)
        {
            if (string.IsNullOrEmpty(_url)) return;
            _queue.Enqueue((context, line));
            EnsureStarted();
        }

        private static void EnsureStarted()
        {
            if (_started) return;
            _started = true;
            Task.Run(ProcessQueue);
        }

        private static async Task ProcessQueue()
        {
            while (true)
            {
                try
                {
                    _ws = new ClientWebSocket();
                    await _ws.ConnectAsync(new Uri(_url), CancellationToken.None);

                    while (_ws.State == WebSocketState.Open)
                    {
                        while (_queue.TryDequeue(out var item))
                        {
                            var (context, line) = item;
                            var payload = $"{context}|{line}\n";
                            var bytes = Encoding.UTF8.GetBytes(payload);
                            await _ws.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None);
                        }
                        await Task.Delay(10);
                    }
                }
                catch
                {
                    await Task.Delay(2000); // reconnect
                }
            }
        }
    }
}
