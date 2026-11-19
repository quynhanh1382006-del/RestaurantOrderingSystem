using NotificationService.Hubs;
using NotificationService.Messaging;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSignalR();

builder.Services.AddHostedService<OrderStatusConsumer>();

var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles();

app.MapHub<NotificationHub>("/notificationHub");

app.Run();
