using Confluent.Kafka;
using System.Text.Json;

namespace ProducerProcessorConsumer.Models;

public class KafkaProducer : IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly KafkaConfig _config;
    private readonly object _lock = new();
    private int _producedCount = 0;

    public KafkaProducer(KafkaConfig config)
    {
        _config = config;
        
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = config.BootstrapServers,
            Acks = Acks.All, // Wait for all replicas to acknowledge
            RetryBackoffMs = 1000,
            MessageSendMaxRetries = 3,
            EnableIdempotence = true, // Prevent duplicate messages
        };

        if (config.UseSasl)
        {
            producerConfig.SecurityProtocol = SecurityProtocol.SaslSsl;
            producerConfig.SaslMechanism = SaslMechanism.Plain;
            producerConfig.SaslUsername = config.SaslUsername;
            producerConfig.SaslPassword = config.SaslPassword;
        }

        _producer = new ProducerBuilder<string, string>(producerConfig).Build();
    }

    public async Task ProduceAsync(ProcessedRecord processedRecord, CancellationToken cancellationToken = default)
    {
        try
        {
            var message = new Message<string, string>
            {
                Key = processedRecord.Key,
                Value = JsonSerializer.Serialize(new
                {
                    Key = processedRecord.Key,
                    Value = processedRecord.Value,
                    ProcessedAt = processedRecord.ProcessedAt,
                    BucketId = processedRecord.BucketId
                })
            };

            var result = await _producer.ProduceAsync(_config.OutputTopic, message, cancellationToken);
            
            lock (_lock)
            {
                _producedCount++;
                Console.WriteLine($"[Bucket {processedRecord.BucketId}] Produced: Key={processedRecord.Key}, " +
                    $"Offset={result.Offset}, Partition={result.Partition}, ProcessedAt={processedRecord.ProcessedAt:HH:mm:ss.fff}");
            }
        }
        catch (ProduceException<string, string> ex)
        {
            Console.WriteLine($"[Producer] Failed to produce Key={processedRecord.Key}: {ex.Error.Reason}");
            throw new InvalidOperationException($"Failed to produce message: {ex.Error.Reason}", ex);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Producer] Unexpected error producing Key={processedRecord.Key}: {ex.Message}");
            throw;
        }
    }

    public int GetProducedCount()
    {
        lock (_lock)
        {
            return _producedCount;
        }
    }

    public void Flush()
    {
        try
        {
            _producer.Flush();
            Console.WriteLine("[Producer] Flushed all pending messages");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Producer] Flush failed: {ex.Message}");
        }
    }

    public void Dispose()
    {
        Flush();
        _producer?.Dispose();
    }
}