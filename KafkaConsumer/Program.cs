using Confluent.Kafka;
using System;

namespace KafkaConsumer
{
    public class Program
    {
        public static void Main()
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "127.0.0.1:9092",
                GroupId = "order-group",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<string, string>(config).Build();
            consumer.Subscribe("orders");

            Console.WriteLine("Esperando mensajes...");

            while (true)
            {
                try
                {
                    var consumeResult = consumer.Consume();
                    Console.WriteLine($"Recibido: {consumeResult.Message.Value} (Key: {consumeResult.Message.Key})");
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Error en la recepción: {e.Error.Reason}");
                }
            }
        }
    }
}
