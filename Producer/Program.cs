using Confluent.Kafka;
using System.Threading.Tasks;
using System.Text.Json;

public class Order
{
    public string OrderId { get; set; }
    public string Status { get; set; }
    public DateTime Timestamp { get; set; }
}

public class Producer
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("Iniciando el productor de pedidos...");
        System.Threading.Thread.Sleep(60000); // Espera a que Kafka esté listo

        var config = new ProducerConfig { BootstrapServers = "kafka:9092" };

        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            // Simular un pedido creado
            var orderId = "#0001";
            var createdOrder = new Order { OrderId = orderId, Status = "Creado", Timestamp = DateTime.UtcNow };
            await producer.ProduceAsync("order-status-changes", new Message<Null, string> { Value = JsonSerializer.Serialize(createdOrder) });
            Console.WriteLine($"Pedido {orderId} creado y publicado en Kafka.");

            // Simular un cambio de estado: Confirmado
            await Task.Delay(5000); // Espera 5 segundos
            var confirmedOrder = new Order { OrderId = orderId, Status = "Confirmado", Timestamp = DateTime.UtcNow };
            await producer.ProduceAsync("order-status-changes", new Message<Null, string> { Value = JsonSerializer.Serialize(confirmedOrder) });
            Console.WriteLine($"Pedido {orderId} confirmado y publicado en Kafka.");

            // Simular un cambio de estado: Enviado
            await Task.Delay(5000); // Espera 5 segundos
            var shippedOrder = new Order { OrderId = orderId, Status = "Enviado", Timestamp = DateTime.UtcNow };
            await producer.ProduceAsync("order-status-changes", new Message<Null, string> { Value = JsonSerializer.Serialize(shippedOrder) });
            Console.WriteLine($"Pedido {orderId} enviado y publicado en Kafka.");
        }
    }
}