using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace KafkaKittenStore
{
    class Program
    {
        public static async Task Main(string[] args)
        {

            ProducerConfig config = new ProducerConfig
            {
                BootstrapServers = "[CLOUDKARAFKA_BROKERS]",
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.ScramSha256,
                SaslUsername = "[CLOUDKARAFKA_USERNAME]",
                SaslPassword = "[CLOUDKARAFKA_PASSWORD]"
            };

            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    string stringValue = "";
                    for (int i = 0; i < 5; ++i)
                    {

                        stringValue += "ฅ^•ﻌ•^ฅ";
                        var deliveryReport = await producer.ProduceAsync(topic: "[PREFIX]-kitten-topic", message: new Message<Null, string> { Value = stringValue });
                        Console.WriteLine($"Delivered {deliveryReport.Message.Value} to partition {deliveryReport.TopicPartitionOffset}");
                    }
                }
                catch (ProduceException<Null, string> exception)
                {

                    Console.WriteLine($"Delivery failed: {exception.Error.Reason}");
                }

            }


        }
    }
}
