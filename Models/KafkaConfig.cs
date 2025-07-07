namespace ProducerProcessorConsumer.Models;

public class KafkaConfig
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string InputTopic { get; set; } = "input-topic";
    public string OutputTopic { get; set; } = "output-topic";
    public string ConsumerGroup { get; set; } = "producer-processor-consumer-group";
    public string? SaslUsername { get; set; }
    public string? SaslPassword { get; set; }
    public bool UseSasl => !string.IsNullOrEmpty(SaslUsername) && !string.IsNullOrEmpty(SaslPassword);
    public int MaxPollSize { get; set; } = 500;
}