using Microsoft.Extensions.Configuration;

namespace ProducerProcessorConsumer.Models;

public class ConfigurationService
{
    private readonly IConfiguration _configuration;
    
    public ConfigurationService()
    {           
        var environment = Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT") ?? "Production";
        
        _configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .AddJsonFile($"appsettings.{environment}.json", optional: true, reloadOnChange: true)
            .AddEnvironmentVariables()
            .Build();
            
        Console.WriteLine($"[Config] Environment: {environment}");
    }

    public KafkaConfig GetKafkaConfig()
    {
        var config = new KafkaConfig();
        
        // Get from configuration
        config.BootstrapServers = _configuration["Kafka:BootstrapServers"] ?? "localhost:9092";
        config.InputTopic = _configuration["Kafka:Topics:InputTopic"] ?? "input-topic";
        config.OutputTopic = _configuration["Kafka:Topics:OutputTopic"] ?? "output-topic";
        config.ConsumerGroup = _configuration["Kafka:ConsumerGroup"] ?? "producer-processor-consumer-group";
        config.SaslUsername = _configuration["Kafka:SaslUsername"];
        config.SaslPassword = _configuration["Kafka:SaslPassword"];
        config.MaxPollSize = int.TryParse(_configuration["Kafka:MaxPollSize"], out var maxPollSize) ? maxPollSize : 500;

        // Override with environment variables if present
        config.BootstrapServers = Environment.GetEnvironmentVariable("BOOTSTRAP_SERVERS") ?? config.BootstrapServers;
        config.SaslUsername = Environment.GetEnvironmentVariable("SASL_USERNAME") ?? config.SaslUsername;
        config.SaslPassword = Environment.GetEnvironmentVariable("SASL_PASSWORD") ?? config.SaslPassword;
        config.InputTopic = Environment.GetEnvironmentVariable("INPUT_TOPIC") ?? config.InputTopic;
        config.OutputTopic = Environment.GetEnvironmentVariable("OUTPUT_TOPIC") ?? config.OutputTopic;
        config.ConsumerGroup = Environment.GetEnvironmentVariable("CONSUMER_GROUP") ?? config.ConsumerGroup;
        config.MaxPollSize = int.TryParse(Environment.GetEnvironmentVariable("KAFKA_MAX_POLL_SIZE"), out var envMaxPollSize) ? envMaxPollSize : config.MaxPollSize;

        return config;
    }


    public void ValidateConfiguration()
    {
        var config = GetKafkaConfig();
        
        if (string.IsNullOrEmpty(config.BootstrapServers))
            throw new InvalidOperationException("Bootstrap servers must be configured");
            
        if (string.IsNullOrEmpty(config.InputTopic))
            throw new InvalidOperationException("Input topic must be configured");
            
        if (string.IsNullOrEmpty(config.OutputTopic))
            throw new InvalidOperationException("Output topic must be configured");

        Console.WriteLine("=== Kafka Configuration ===");
        Console.WriteLine($"Bootstrap Servers: {config.BootstrapServers}");
        Console.WriteLine($"Input Topic: {config.InputTopic}");
        Console.WriteLine($"Output Topic: {config.OutputTopic}");
        Console.WriteLine($"Consumer Group: {config.ConsumerGroup}");
        Console.WriteLine($"Max Poll Size: {config.MaxPollSize}");
        Console.WriteLine($"Using SASL: {config.UseSasl}");
        Console.WriteLine("============================");
    }
}