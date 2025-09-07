using Confluent.Kafka;
using System.Text.Json;


public class Program
{
    public static void Main(string[] args)
    {

        Console.WriteLine("Iniciando el consumidor de NotificationService...");
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
                    // Obtenemos la raíz del documento JSON
                    JsonElement root = doc.RootElement;

                    // Extraemos los valores de las propiedades
                    string orderId = root.GetProperty("OrderId").GetString();
                    string status = root.GetProperty("Status").GetString();

                    Console.WriteLine($"Pedido {orderId}: estado {status}");

                    // Lógica para el análisis de datos
                    // Ya no necesitas el objeto 'order', puedes usar las variables orderId y status directamente.
                }
            }
        }
    }
}