using Confluent.Kafka;

namespace ProducerProcessorConsumer.Models;

public class KafkaConsumer : IDisposable
{
    private readonly IConsumer<string, string> _consumer;
    private readonly KafkaConfig _config;
    private List<ConsumeResult<string, string>>? _currentBatch;

    public KafkaConsumer(KafkaConfig config)
    {
        _config = config;
        
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = config.BootstrapServers,
            GroupId = config.ConsumerGroup,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false, // Manual commit for batch processing
        };

        if (config.UseSasl)
        {
            consumerConfig.SecurityProtocol = SecurityProtocol.SaslSsl;
            consumerConfig.SaslMechanism = SaslMechanism.Plain;
            consumerConfig.SaslUsername = config.SaslUsername;
            consumerConfig.SaslPassword = config.SaslPassword;
        }

        _consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        _consumer.Subscribe(config.InputTopic);
    }

    public async Task<List<Record>> ConsumeBatchAsync(CancellationToken cancellationToken = default)
    {
        var batch = new List<Record>();
        var consumeResults = new List<ConsumeResult<string, string>>();
        
        // Use shorter poll intervals for better responsiveness
        var pollTimeout = TimeSpan.FromMilliseconds(250); // 250ms per poll
        
        while (batch.Count < _config.MaxPollSize && !cancellationToken.IsCancellationRequested)
        {
            try
            {
                // Consume with timeout to get available messages
                var result = _consumer.Consume(pollTimeout);
                
                if (result != null)
                {
                    var record = new Record(result.Message.Key, result.Message.Value);
                    batch.Add(record);
                    consumeResults.Add(result);
                    
                    // Continue consuming until no more messages are available
                    while (batch.Count < _config.MaxPollSize)
                    {
                        var immediateResult = _consumer.Consume(TimeSpan.FromMilliseconds(10)); // Very short timeout
                        if (immediateResult != null)
                        {
                            var immediateRecord = new Record(immediateResult.Message.Key, immediateResult.Message.Value);
                            batch.Add(immediateRecord);
                            consumeResults.Add(immediateResult);
                        }
                        else
                        {
                            break; // No more messages available right now
                        }
                    }
                    
                    Console.WriteLine($"[Consumer] Consumed {batch.Count} records (poll exhausted or max size reached)");
                    break; // Exit after consuming all available messages or reaching max size
                }
                else
                {
                    // No messages available - return empty batch
                    break;
                }
            }
            catch (ConsumeException ex)
            {
                Console.WriteLine($"Error consuming message: {ex.Error.Reason}");
                if (batch.Count > 0)
                {
                    Console.WriteLine($"[Consumer] Error occurred - processing partial batch of {batch.Count} records");
                    break;
                }
                await Task.Delay(1000, cancellationToken);
            }
        }

        Console.WriteLine($"[Consumer] Returning batch of {batch.Count} records");
        _currentBatch = consumeResults;
        return batch;
    }

    public Task CommitBatchAsync(CancellationToken cancellationToken = default)
    {
        if (_currentBatch == null || _currentBatch.Count == 0)
            throw new InvalidOperationException("No batch to commit");

        try
        {
            // Commit the last offset + 1 for each partition
            var offsets = _currentBatch
                .GroupBy(r => new TopicPartition(r.Topic, r.Partition))
                .Select(g => new TopicPartitionOffset(g.Key, g.Max(r => r.Offset.Value) + 1))
                .ToList();

            _consumer.Commit(offsets);
            
            var maxOffset = _currentBatch.Max(r => r.Offset.Value);
            Console.WriteLine($"[Consumer] Committed batch: {_currentBatch.Count} messages, max offset: {maxOffset}");
            _currentBatch = null;
            
            return Task.CompletedTask;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Consumer] Commit failed: {ex.Message}");
            throw;
        }
    }

    public long GetLastCommittedOffset()
    {
        try
        {
            var assignment = _consumer.Assignment;
            if (assignment.Count > 0)
            {
                var committed = _consumer.Committed(assignment, TimeSpan.FromSeconds(10));
                return committed.Max(tp => tp.Offset.Value);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Consumer] Failed to get committed offset: {ex.Message}");
        }
        return -1;
    }

    public void Dispose()
    {
        _consumer?.Close();
        _consumer?.Dispose();
    }
}