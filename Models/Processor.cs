namespace ProducerProcessorConsumer.Models;

public class Processor
{
    public async Task<ProcessedRecord> ProcessAsync(Record record, CancellationToken cancellationToken = default)
    {
        await Task.Delay(10, cancellationToken);
        
        var bucketId = Math.Abs(record.Key.GetHashCode()) % 10;
        var processedValue = record.Value.ToUpperInvariant();
        
        return new ProcessedRecord(record.Key, processedValue, DateTime.UtcNow, bucketId);
    }
}