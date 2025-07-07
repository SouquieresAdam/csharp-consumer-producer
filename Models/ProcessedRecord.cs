namespace ProducerProcessorConsumer.Models;

public record ProcessedRecord(string Key, string Value, DateTime ProcessedAt, int BucketId);