using System.Text;
using System.Text.Json;
using RabbitMQ.Client;

namespace OrderService.Messaging;

public class RabbitMqPublisher
{
    private readonly IConfiguration _configuration;

    public RabbitMqPublisher(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    private ConnectionFactory CreateFactory()
    {
        return new ConnectionFactory
        {
            HostName = _configuration["RabbitMQ:Host"] ?? "localhost",
            Port = int.Parse(_configuration["RabbitMQ:Port"] ?? "5672"),
            UserName = _configuration["RabbitMQ:Username"]!,
            Password = _configuration["RabbitMQ:Password"]!
        };
    }

    public async Task PublishOrderCreatedAsync(object orderEvent)
    {
        await PublishAsync("orders", orderEvent);
    }

    public async Task PublishOrderStatusAsync(
        Guid orderId,
        string status)
    {
        await PublishAsync("order-status", new
        {
            EventType = "OrderStatusChanged",
            OrderId = orderId.ToString(),
            Status = status,
            UpdatedAt = DateTime.UtcNow
        });
    }

    private async Task PublishAsync(
        string queueName,
        object message)
    {
        var factory = CreateFactory();

        await using var connection =
            await factory.CreateConnectionAsync();

        await using var channel =
            await connection.CreateChannelAsync();

        await channel.QueueDeclareAsync(
            queue: queueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null);

        var body = Encoding.UTF8.GetBytes(
            JsonSerializer.Serialize(message));

        var properties = new BasicProperties
        {
            Persistent = true,
            ContentType = "application/json"
        };

        await channel.BasicPublishAsync(
            exchange: "",
            routingKey: queueName,
            mandatory: true,
            basicProperties: properties,
            body: body);
    }
}
