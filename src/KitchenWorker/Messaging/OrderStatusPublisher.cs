using System.Text;
using System.Text.Json;
using RabbitMQ.Client;

namespace KitchenWorker.Messaging;

public class OrderStatusPublisher
{
    private readonly IConfiguration _configuration;

    public OrderStatusPublisher(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    public async Task PublishStatusAsync(
        Guid orderId,
        string status)
    {
        var factory = new ConnectionFactory
        {
            HostName = _configuration["RabbitMQ:Host"] ?? "localhost",
            Port = int.Parse(_configuration["RabbitMQ:Port"] ?? "5672"),
            UserName = _configuration["RabbitMQ:Username"]!,
            Password = _configuration["RabbitMQ:Password"]!
        };

        await using var connection =
            await factory.CreateConnectionAsync();

        await using var channel =
            await connection.CreateChannelAsync();

        await channel.QueueDeclareAsync(
            queue: "order-status",
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null);

        var message = JsonSerializer.Serialize(new
        {
            EventType = "OrderStatusChanged",
            OrderId = orderId.ToString(),
            Status = status,
            UpdatedAt = DateTime.UtcNow
        });

        var body = Encoding.UTF8.GetBytes(message);

        var properties = new BasicProperties
        {
            Persistent = true,
            ContentType = "application/json"
        };

        await channel.BasicPublishAsync(
            exchange: "",
            routingKey: "order-status",
            mandatory: true,
            basicProperties: properties,
            body: body);
    }
}
