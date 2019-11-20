using System;
using System.Text;
using System.Threading;
using Confluent.Kafka;

namespace KafkaKittenCatcher
{
    class Program
    {
        public static void Main(string[] args)
        {
            ConsumerConfig config = new ConsumerConfig
            {
                GroupId = "test-consumer-group",
                BootstrapServers = "[CLOUDKARAFKA_BROKERS]",
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.ScramSha256,
                SaslUsername = "[CLOUDKARAFKA_USERNAME]",
                SaslPassword = "[CLOUDKARAFKA_PASSWORD]"
            };

            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                consumer.Subscribe("[PREFIX]-kitten-topic");

                CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                  {
                    e.Cancel = true; //prevent the process from terminating.
                    cancellationTokenSource.Cancel();

                  };
                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = consumer.Consume(cancellationTokenSource.Token);
                            Console.WriteLine($"Consumed message '{cr.Value}' at '{cr.TopicPartitionOffset}'.");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    //Ensure the consumer leaves the group cleanly and final offsets are committed.
                    consumer.Close();
                }

            
                
            }
        }
    }
}
