using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SistemaOpiniones.ETL.Domain.Interfaces;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SistemaOpiniones.ETL.Application
{
    public class EtlOrchestrator : IEtlOrchestrator
    {
        private readonly ILogger<EtlOrchestrator> _logger;
        private readonly IExtractorFactory _extractorFactory;
        private readonly IStagingService _stagingService;
        private readonly IDwhLoaderService _dwhLoaderService;
        private readonly EtlProcessOptions _options;

        public EtlOrchestrator(
            ILogger<EtlOrchestrator> logger,
            IExtractorFactory extractorFactory,
            IStagingService stagingService,
            IDwhLoaderService dwhLoaderService,
            IOptions<EtlProcessOptions> options)
        {
            _logger = logger;
            _extractorFactory = extractorFactory;
            _stagingService = stagingService;
            _dwhLoaderService = dwhLoaderService;
            _options = options.Value;
        }

        public async Task ExecuteEtlProcessAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Iniciando ciclo completo ETL.");
            var totalStopwatch = Stopwatch.StartNew();

            foreach (var sourceName in _options.SourcesToRun)
            {
                if (cancellationToken.IsCancellationRequested) break;
                var stopwatch = Stopwatch.StartNew();
                _logger.LogInformation("--- Iniciando extracción: {FuenteNombre} ---", sourceName);

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

                    stopwatch.Stop();
                    _logger.LogInformation("--- Extractor {FuenteNombre} finalizado en {Elapsed}ms ---", sourceName, stopwatch.ElapsedMilliseconds);
                }
                catch (NotSupportedException nsex)
                {
                    _logger.LogWarning(nsex.Message);
                }
                catch (Exception ex)
                {
                    stopwatch.Stop();
                    _logger.LogError(ex, "Fallo el extractor {FuenteNombre}.", sourceName);
                }
            }

            if (!cancellationToken.IsCancellationRequested)
            {
                _logger.LogInformation("--- INICIO FASE 2: Carga Dimensiones DWH ---");
                try
                {
                    var dimStopwatch = Stopwatch.StartNew();
                    await _dwhLoaderService.LoadDimensionsAsync(cancellationToken);
                    dimStopwatch.Stop();
                    _logger.LogInformation("Carga de Dimensiones completada en {Elapsed}ms.", dimStopwatch.ElapsedMilliseconds);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error crítico durante la carga de dimensiones.");
                }
            }

            totalStopwatch.Stop();
            _logger.LogInformation("Proceso ETL finalizado en {Elapsed}ms.", totalStopwatch.ElapsedMilliseconds);
        }
    }
}