using ProducerProcessorConsumer.Models;

class KafkaProgram
{
    static async Task Main(string[] args)
    {
        Console.WriteLine("Real Kafka Producer-Processor-Consumer Pipeline");
        Console.WriteLine("===============================================");
        Console.WriteLine("Processing in batches with hash-based distribution");
        Console.WriteLine("Up to 10 concurrent processing tasks per batch");
        Console.WriteLine("Press Ctrl+C to stop gracefully");
        Console.WriteLine();

        var configService = new ConfigurationService();
        
        try
        {
            configService.ValidateConfiguration();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Configuration error: {ex.Message}");
            Console.WriteLine("Please check your appsettings.json or environment variables.");
            return;
        }

        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (sender, e) =>
        {
            e.Cancel = true;
            Console.WriteLine("\nShutdown requested...");
            cts.Cancel();
        };

        var kafkaConfig = configService.GetKafkaConfig();
        
        using var consumer = new KafkaConsumer(kafkaConfig);
        using var producer = new KafkaProducer(kafkaConfig);
        var processor = new Processor();
        
        using var pipeline = new KafkaPipeline(consumer, processor, producer, configService);

        try
        {
            await pipeline.RunAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Pipeline stopped gracefully.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Pipeline error: {ex.Message}");
        }

        Console.WriteLine($"Last committed offset: {consumer.GetLastCommittedOffset()}");
        Console.WriteLine($"Total records produced: {producer.GetProducedCount()}");
        Console.WriteLine("Application finished.");
    }
}