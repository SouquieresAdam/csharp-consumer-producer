namespace ProducerProcessorConsumer.Models;

public class BatchResult
{
    public bool IsSuccess { get; init; }
    public List<Record> SuccessfulRecords { get; init; } = new();
    public List<(Record Record, Exception Exception)> FailedRecords { get; init; } = new();
    public int TotalRecords => SuccessfulRecords.Count + FailedRecords.Count;
    
    public static BatchResult Success(List<Record> successfulRecords)
    {
        return new BatchResult
        {
            IsSuccess = true,
            SuccessfulRecords = successfulRecords
        };
    }
    
    public static BatchResult Failure(List<Record> successfulRecords, List<(Record, Exception)> failedRecords)
    {
        return new BatchResult
        {
            IsSuccess = false,
            SuccessfulRecords = successfulRecords,
            FailedRecords = failedRecords
        };
    }
}