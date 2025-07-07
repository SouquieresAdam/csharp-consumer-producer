namespace ProducerProcessorConsumer.Models;

public class KafkaPipeline : IDisposable
{
    private readonly KafkaConsumer _consumer;
    private readonly Processor _processor;
    private readonly KafkaProducer _producer;
    private readonly ConfigurationService _configService;

    public KafkaPipeline(KafkaConsumer consumer, Processor processor, KafkaProducer producer, ConfigurationService configService)
    {
        _consumer = consumer;
        _processor = processor;
        _producer = producer;
        _configService = configService;
    }

    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        int batchNumber = 0;
        var kafkaConfig = _configService.GetKafkaConfig();
        
        Console.WriteLine($"Starting pipeline with max poll size: {kafkaConfig.MaxPollSize}");
        
        while (!cancellationToken.IsCancellationRequested)
        {
            batchNumber++;
            Console.WriteLine($"\n=== Batch {batchNumber} ===");
            
            try
            {
                var batch = await _consumer.ConsumeBatchAsync(cancellationToken);
                
                if (batch.Count == 0)
                {
                    Console.WriteLine("No messages available, waiting...");
                    await Task.Delay(1000, cancellationToken);
                    continue;
                }
                
                Console.WriteLine($"Consumed {batch.Count} records");
                
                var buckets = batch.GroupBy(r => Math.Abs(r.Key.GetHashCode()) % 10).ToList();
                Console.WriteLine($"Distributed into {buckets.Count} buckets");
                
                var bucketTasks = buckets.Select(bucket => 
                    ProcessBucketAsync(bucket.Key, bucket.ToList(), cancellationToken)
                ).ToList();
                
                var bucketResults = await Task.WhenAll(bucketTasks);
                
                var allSuccessful = bucketResults.All(r => r.IsSuccess);
                var totalSuccessful = bucketResults.Sum(r => r.SuccessfulRecords.Count);
                var totalFailed = bucketResults.Sum(r => r.FailedRecords.Count);
                
                if (allSuccessful)
                {
                    await _consumer.CommitBatchAsync(cancellationToken);
                    Console.WriteLine($"[Batch {batchNumber}] SUCCESS - All {batch.Count} records processed and committed");
                }
                else
                {
                    Console.WriteLine($"[Batch {batchNumber}] FAILURE - {totalSuccessful} succeeded, {totalFailed} failed. Offset NOT committed.");
                    
                    // Log detailed failure information
                    foreach (var result in bucketResults.Where(r => !r.IsSuccess))
                    {
                        foreach (var (record, exception) in result.FailedRecords)
                        {
                            Console.WriteLine($"[Batch {batchNumber}] Failed record Key={record.Key}: {exception.Message}");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Batch {batchNumber}] Critical error: {ex.Message}");
                // Wait before retrying to avoid tight error loops
                await Task.Delay(5000, cancellationToken);
            }
        }
    }

    private async Task<BatchResult> ProcessBucketAsync(int bucketId, List<Record> records, CancellationToken cancellationToken)
    {
        var produceTasks = new List<Task>();
        var successfulRecords = new List<Record>();
        var failedRecords = new List<(Record, Exception)>();

        foreach (var record in records)
        {
            try
            {
                var processedRecord = await _processor.ProcessAsync(record, cancellationToken);
                var produceTask = _producer.ProduceAsync(processedRecord, cancellationToken);
                produceTasks.Add(produceTask);
                successfulRecords.Add(record);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Bucket {bucketId}] Processing failed for Key={record.Key}: {ex.Message}");
                failedRecords.Add((record, ex));
            }
        }

        try
        {
            await Task.WhenAll(produceTasks);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Bucket {bucketId}] Produce operations failed: {ex.Message}");
            // Move successful records to failed since produce failed
            foreach (var record in successfulRecords)
            {
                failedRecords.Add((record, ex));
            }
            successfulRecords.Clear();
        }

        return failedRecords.Count > 0 
            ? BatchResult.Failure(successfulRecords, failedRecords)
            : BatchResult.Success(successfulRecords);
    }

    public void Dispose()
    {
        _consumer?.Dispose();
        _producer?.Dispose();
    }
}