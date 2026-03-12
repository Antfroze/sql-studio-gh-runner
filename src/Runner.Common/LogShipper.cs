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
        private static readonly ConcurrentQueue<(string context, string line)> _queue = new();
        private static ClientWebSocket _ws;
        private static readonly SemaphoreSlim _wsLock = new(1, 1);
        private static int _started = 0;

        public static void Send(string context, string line)
        {
            if (string.IsNullOrEmpty(_url)) return;

            _queue.Enqueue((context, line));
            EnsureStarted();
        }

        private static void EnsureStarted()
        {
            if (Interlocked.CompareExchange(ref _started, 1, 0) == 0)
            {
                Task.Run(ProcessQueue);
            }
        }

        private static async Task ProcessQueue()
        {
            while (true)
            {
                try
                {
                    if (_ws == null || _ws.State != WebSocketState.Open)
                    {
                        _ws?.Dispose();
                        _ws = new ClientWebSocket();
                        await _ws.ConnectAsync(new Uri(_url), CancellationToken.None);
                    }

                    while (_ws.State == WebSocketState.Open && _queue.TryDequeue(out var item))
                    {
                        try
                        {
                            var payload = $"{item.context}|{item.line}\n";
                            var bytes = Encoding.UTF8.GetBytes(payload);

                            await _wsLock.WaitAsync();
                            try
                            {
                                await _ws.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None);
                            }
                            finally
                            {
                                _wsLock.Release();
                            }
                        }
                        catch
                        {
                            _queue.Enqueue(item);
                            throw;
                        }
                    }

                    await Task.Delay(10);
                }
                catch
                {
                    _ws?.Dispose();
                    _ws = null;
                    await Task.Delay(2000);
                }
            }
        }
    }
}
