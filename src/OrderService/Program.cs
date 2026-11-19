using OrderService.Messaging;
using Microsoft.EntityFrameworkCore;
using OrderService.Data;
using OrderService.Models;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSingleton<RabbitMqPublisher>();

var connectionString = builder.Configuration
    .GetConnectionString("DefaultConnection")
    ?? throw new InvalidOperationException(
        "KhÃ´ng tÃ¬m tháº¥y chuá»—i káº¿t ná»‘i PostgreSQL.");

builder.Services.AddDbContext<OrderDbContext>(options =>
    options.UseNpgsql(connectionString));

var app = builder.Build();

// Danh sÃ¡ch mÃ³n Äƒn
var menuItems = new List<MenuItemDto>
{
    new(1, "CÆ¡m gÃ ", 45000),
    new(2, "Phá»Ÿ bÃ²", 50000),
    new(3, "BÃºn cháº£", 40000),
    new(4, "TrÃ  Ä‘Ã o", 25000)
};

// GET: Danh sÃ¡ch mÃ³n Äƒn
app.MapGet("/menu", () =>
{
    return Results.Ok(menuItems);
});

// POST: Táº¡o Ä‘Æ¡n hÃ ng vÃ  lÆ°u PostgreSQL
app.MapPost("/orders", async Task<IResult>
    (CreateOrderRequest request, OrderDbContext db,
     RabbitMqPublisher publisher) =>
{
    if (request.Items == null || request.Items.Count == 0)
    {
        return Results.BadRequest(new
        {
            message = "ÄÆ¡n hÃ ng pháº£i cÃ³ Ã­t nháº¥t má»™t mÃ³n."
        });
    }

    var orderItems = new List<OrderItem>();

    foreach (var item in request.Items)
    {
        if (item.Quantity <= 0)
        {
            return Results.BadRequest(new
            {
                message = "Sá»‘ lÆ°á»£ng pháº£i lá»›n hÆ¡n 0."
            });
        }

        var menuItem = menuItems.FirstOrDefault(
            m => m.Id == item.MenuItemId);

        if (menuItem == null)
        {
            return Results.BadRequest(new
            {
                message = $"KhÃ´ng tÃ¬m tháº¥y mÃ³n cÃ³ ID {item.MenuItemId}."
            });
        }

        orderItems.Add(new OrderItem
        {
            MenuItemId = menuItem.Id,
            Name = menuItem.Name,
            Price = menuItem.Price,
            Quantity = item.Quantity
        });
    }

    var order = new Order
    {
        CreatedAt = DateTime.UtcNow,
        Status = "Pending",
        Items = orderItems
    };

    order.Total = order.Items.Sum(
        item => item.Price * item.Quantity);

    db.Orders.Add(order);
await db.SaveChangesAsync();

await publisher.PublishOrderCreatedAsync(new
{
    EventType = "OrderCreated",
    order.Id,
    order.CreatedAt,
    order.Status,
    order.Total,
    Items = order.Items.Select(item => new
    {
        item.MenuItemId,
        item.Name,
        item.Price,
        item.Quantity
    })
});

return Results.Created($"/orders/{order.Id}", new
    {
        order.Id,
        order.CreatedAt,
        order.Status,
        order.Total,
        Items = order.Items
    });
});

// GET: Danh sÃ¡ch Ä‘Æ¡n hÃ ng tá»« PostgreSQL
app.MapGet("/orders", async (OrderDbContext db) =>
{
    var orders = await db.Orders
        .Include(o => o.Items)
        .AsNoTracking()
        .OrderByDescending(o => o.CreatedAt)
        .ToListAsync();

    return Results.Ok(orders);
});

// GET: Chi tiáº¿t Ä‘Æ¡n hÃ ng
app.MapGet("/orders/{id:guid}", async Task<IResult>
    (Guid id, OrderDbContext db) =>
{
    var order = await db.Orders
        .Include(o => o.Items)
        .AsNoTracking()
        .FirstOrDefaultAsync(o => o.Id == id);

    if (order == null)
    {
        return Results.NotFound(new
        {
            message = "KhÃ´ng tÃ¬m tháº¥y Ä‘Æ¡n hÃ ng."
        });
    }

    return Results.Ok(order);
});

// PUT: Cap nhat trang thai don hang
app.MapPut("/orders/{id:guid}/status", async (
    Guid id,
    string status,
    OrderDbContext db,
    RabbitMqPublisher publisher) =>
{
    var order = await db.Orders.FindAsync(id);

    if (order is null)
        return Results.NotFound(new { message = "Khong tim thay don hang." });

    var validStatuses = new[]
    {
        "Pending", "Preparing", "Ready", "Cancelled"
    };

    if (!validStatuses.Contains(status))
        return Results.BadRequest(new { message = "Trang thai khong hop le." });

    order.Status = status;
    await db.SaveChangesAsync();
    await publisher.PublishOrderStatusAsync(id, status);

    return Results.Ok(new
    {
        message = "Cap nhat trang thai thanh cong.",
        order.Id,
        order.Status
    });
});
app.MapGet("/", () => Results.Ok(new
{
    message = "Restaurant Ordering System - OrderService",
    database = "PostgreSQL"
}));

app.Run();

public record MenuItemDto(int Id, string Name, decimal Price);

public record CreateOrderRequest(
    List<CreateOrderItemRequest>? Items);

public record CreateOrderItemRequest(
    int MenuItemId,
    int Quantity);




