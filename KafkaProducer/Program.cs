using Confluent.Kafka;
using System;
using System.Threading.Tasks;

namespace KafkaProducer
{
    public class Program
    {
        public static async Task Main()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "127.0.0.1:9092"
            };

            var producer = new ProducerBuilder<string, string>(config).Build();

            for (int i = 1; i <= 5; i++)
            {
                Console.Write($"Ingrese el nombre del cliente para realizar el pedido {i}: ");
                string name = Console.ReadLine();

                var orderId = Guid.NewGuid().ToString();
                var message = new Message<string, string>
                {
                    Key = orderId,
                    Value = $"Pedido {i} de: {name} generado a las {DateTime.UtcNow}"
                };

                var deliveryResult = await producer.ProduceAsync("orders", message);
                Console.WriteLine($"Enviado: {message.Value} (Partición: {deliveryResult.Partition})");
            }
        }
    }
}
