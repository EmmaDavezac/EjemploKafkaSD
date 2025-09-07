using Confluent.Kafka;
using System.Text.Json;


public class Program
{
    public static void Main(string[] args)
    {

        Console.WriteLine("Iniciando el consumidor de AnalyticsService...");
        System.Threading.Thread.Sleep(60000); // Espera a que Kafka esté listo

        var config = new ConsumerConfig
        {
            GroupId = "consumers-group", 

            BootstrapServers = "kafka:9092",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
        {
            consumer.Subscribe("order-status-changes");
            Console.WriteLine("Esperando cambios de estado de pedidos...");

            while (true)
            {
                var consumeResult = consumer.Consume();
                // Usamos JsonDocument para analizar el mensaje sin una clase
                using (JsonDocument doc = JsonDocument.Parse(consumeResult.Message.Value))
                {
                   
                    JsonElement root = doc.RootElement;

                    // Extraemos los valores de las propiedades
                    string orderId = root.GetProperty("OrderId").GetString();
                    string status = root.GetProperty("Status").GetString();

                    Console.WriteLine($"Pedido {orderId}: estado {status}");

                 
                }
            }
        }
    }
}