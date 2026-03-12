using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace GitHub.Runner.Common
{
    public readonly struct LogForwarderContext
    {
        public LogForwarderContext(string runId, long jobId)
        {
            RunId = runId;
            JobId = jobId;
        }

        public string RunId { get; }
        public long JobId { get; }
    }

    public static class LogForwarder
    {
        private static readonly string _url = Environment.GetEnvironmentVariable("LOG_WS_URL");
        private static readonly ConcurrentQueue<(LogForwarderContext context, string line)> _queue = new();
        private static ClientWebSocket _ws;
        private static readonly SemaphoreSlim _wsLock = new(1, 1);
        private static int _started = 0;

        public static void Send(LogForwarderContext context, string line)
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

        private const int BatchSize = 100;
        private static readonly TimeSpan BatchInterval = TimeSpan.FromMilliseconds(250);

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

                    var batch = new List<(LogForwarderContext context, string line)>();
                    while (batch.Count < BatchSize && _queue.TryDequeue(out var item))
                    {
                        batch.Add(item);
                    }

                    if (batch.Count > 0)
                    {
                        var payload = new StringBuilder();
                        foreach (var item in batch)
                        {
                            payload.Append(JsonConvert.SerializeObject(new { runId = item.context.RunId, jobId = item.context.JobId, line = item.line })).Append('\n');
                        }
                        try
                        {
                            var bytes = Encoding.UTF8.GetBytes(payload.ToString());
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
                            foreach (var item in batch)
                            {
                                _queue.Enqueue(item);
                            }
                            throw;
                        }
                    }

                    await Task.Delay(BatchInterval);
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
