using SistemaOpiniones.ETL.Domain.Interfaces;

namespace SistemaOpiniones.ETL.Worker;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IServiceProvider _serviceProvider;
    private readonly TimeSpan _period = TimeSpan.FromHours(1);

    public Worker(ILogger<Worker> logger, IServiceProvider serviceProvider)
    {
        _logger = logger;
        _serviceProvider = serviceProvider;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(_period);

        do
        {
            _logger.LogInformation("Iniciando Worker en: {time}", DateTimeOffset.Now);

            using (var scope = _serviceProvider.CreateScope())
            {
                var orchestrator = scope.ServiceProvider.GetRequiredService<IEtlOrchestrator>();
                try
                {
                    await orchestrator.ExecuteEtlProcessAsync(stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogCritical(ex, "El proceso de orquestación ETL falló catastróficamente.");
                }
            }

            _logger.LogInformation("Worker finalizado, esperando el próximo ciclo de {Period}.", _period);
        }
        while (await timer.WaitForNextTickAsync(stoppingToken) && !stoppingToken.IsCancellationRequested);
    }
}