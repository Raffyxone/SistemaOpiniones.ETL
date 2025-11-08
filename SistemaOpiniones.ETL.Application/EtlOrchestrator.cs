using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SistemaOpiniones.ETL.Domain.Interfaces;

namespace SistemaOpiniones.ETL.Application;

public class EtlOrchestrator : IEtlOrchestrator
{
    private readonly ILogger<EtlOrchestrator> _logger;
    private readonly IExtractorFactory _extractorFactory;
    private readonly IStagingService _stagingService;
    private readonly EtlProcessOptions _options;

    public EtlOrchestrator(
        ILogger<EtlOrchestrator> logger,
        IExtractorFactory extractorFactory,
        IStagingService stagingService,
        IOptions<EtlProcessOptions> options)
    {
        _logger = logger;
        _extractorFactory = extractorFactory;
        _stagingService = stagingService;
        _options = options.Value;
    }

    public async Task ExecuteEtlProcessAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Iniciando ciclo de extracción ETL.");
        var stopwatch = Stopwatch.StartNew();

        foreach (var sourceName in _options.SourcesToRun)
        {
            if (cancellationToken.IsCancellationRequested) break;

            var extractorStopwatch = Stopwatch.StartNew();
            _logger.LogInformation("--- Ejecutando extractor: {FuenteNombre} ---", sourceName);

            try
            {
                var extractor = _extractorFactory.GetExtractor(sourceName);
                var extractedData = await extractor.ExtractAsync(cancellationToken);

                int count = extractedData.Count();
                _logger.LogInformation("Datos extraídos de {FuenteNombre}: {Count} registros.", sourceName, count);

                if (count > 0)
                {
                    await _stagingService.StageDataAsync(extractedData, cancellationToken);
                }

                extractorStopwatch.Stop();
                _logger.LogInformation("--- Extractor {FuenteNombre} finalizado en {Elapsed}ms ---", sourceName, extractorStopwatch.ElapsedMilliseconds);
            }
            catch (NotSupportedException nsex)
            {
                _logger.LogWarning(nsex.Message);
            }
            catch (Exception ex)
            {
                extractorStopwatch.Stop();
                _logger.LogError(ex, "Fallo el extractor {FuenteNombre} después de {Elapsed}ms.", sourceName, extractorStopwatch.ElapsedMilliseconds);
            }
        }

        stopwatch.Stop();
        _logger.LogInformation("Ciclo de extracción ETL finalizado en {Elapsed}ms.", stopwatch.ElapsedMilliseconds);
    }
}