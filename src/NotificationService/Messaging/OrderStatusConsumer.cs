using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.SignalR;
using NotificationService.Hubs;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace NotificationService.Messaging;

public class OrderStatusConsumer : BackgroundService
{
    private readonly IConfiguration _configuration;
    private readonly IHubContext<NotificationHub> _hubContext;
    private readonly ILogger<OrderStatusConsumer> _logger;

    public OrderStatusConsumer(
        IConfiguration configuration,
        IHubContext<NotificationHub> hubContext,
        ILogger<OrderStatusConsumer> logger)
    {
        _configuration = configuration;
        _hubContext = hubContext;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        var factory = new ConnectionFactory
        {
            HostName = _configuration["RabbitMQ:Host"] ?? "localhost",
            Port = int.Parse(_configuration["RabbitMQ:Port"] ?? "5672"),
            UserName = _configuration["RabbitMQ:Username"]!,
            Password = _configuration["RabbitMQ:Password"]!
        };

        await using var connection =
            await factory.CreateConnectionAsync(stoppingToken);

        await using var channel =
            await connection.CreateChannelAsync(
                cancellationToken: stoppingToken);

        await channel.QueueDeclareAsync(
            queue: "order-status",
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            cancellationToken: stoppingToken);

        await channel.BasicQosAsync(
            prefetchSize: 0,
            prefetchCount: 1,
            global: false,
            cancellationToken: stoppingToken);

        var consumer = new AsyncEventingBasicConsumer(channel);

        consumer.ReceivedAsync += async (_, eventArgs) =>
        {
            try
            {
                var message = Encoding.UTF8.GetString(
                    eventArgs.Body.ToArray());

                using var json = JsonDocument.Parse(message);

                var orderId = json.RootElement
                    .GetProperty("OrderId").GetString();

                var status = json.RootElement
                    .GetProperty("Status").GetString();

                if (string.IsNullOrWhiteSpace(orderId) ||
                    string.IsNullOrWhiteSpace(status))
                {
                    throw new InvalidOperationException(
                        "Thong diep khong hop le.");
                }

                await _hubContext.Clients
                    .Group(orderId)
                    .SendAsync(
                        "OrderStatusChanged",
                        new { OrderId = orderId, Status = status },
                        stoppingToken);

                _logger.LogInformation(
                    "Da gui thong bao don {OrderId}: {Status}",
                    orderId, status);

                await channel.BasicAckAsync(
                    eventArgs.DeliveryTag, false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Loi xu ly thong bao");

                await channel.BasicNackAsync(
                    eventArgs.DeliveryTag, false, false);
            }
        };

        await channel.BasicConsumeAsync(
            queue: "order-status",
            autoAck: false,
            consumer: consumer,
            cancellationToken: stoppingToken);

        _logger.LogInformation(
            "NotificationService dang cho thong bao...");

        await Task.Delay(
            Timeout.Infinite, stoppingToken);
    }
}
