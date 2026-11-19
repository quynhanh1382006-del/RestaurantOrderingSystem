
using System.Text;
using System.Text.Json;
using Npgsql;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace KitchenWorker;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IConfiguration _configuration;

    public Worker(
        ILogger<Worker> logger,
        IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
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
            queue: "orders",
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
                var body = eventArgs.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);

                using var json = JsonDocument.Parse(message);

                var orderId = json.RootElement
                    .GetProperty("Id")
                    .GetGuid();

                var connectionString = _configuration
                    .GetConnectionString("DefaultConnection")
                    ?? throw new InvalidOperationException(
                        "Khong tim thay PostgreSQL connection string.");

                await using var dbConnection =
                    new NpgsqlConnection(connectionString);

                await dbConnection.OpenAsync(stoppingToken);

                await using var command = new NpgsqlCommand(
                    """
                    UPDATE "Orders"
                    SET "Status" = @status
                    WHERE "Id" = @id
                    """,
                    dbConnection);

                command.Parameters.AddWithValue("status", "Preparing");
                command.Parameters.AddWithValue("id", orderId);

                var rows = await command.ExecuteNonQueryAsync(
                    stoppingToken);

                if (rows == 0)
                {
                    throw new InvalidOperationException(
                        $"Khong tim thay don hang {orderId}.");
                }

                _logger.LogInformation(
                    "BEP DA NHAN DON HANG: {OrderId}", orderId);

                _logger.LogInformation(
                    "Don hang {OrderId} da chuyen sang Preparing.",
                    orderId);

                await channel.BasicAckAsync(
                    deliveryTag: eventArgs.DeliveryTag,
                    multiple: false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Loi xu ly don hang");

                await channel.BasicNackAsync(
                    deliveryTag: eventArgs.DeliveryTag,
                    multiple: false,
                    requeue: false);
            }
        };

        await channel.BasicConsumeAsync(
            queue: "orders",
            autoAck: false,
            consumer: consumer,
            cancellationToken: stoppingToken);

        _logger.LogInformation(
            "Kitchen Worker dang cho don hang...");

        await Task.Delay(Timeout.Infinite, stoppingToken);
    }
}
