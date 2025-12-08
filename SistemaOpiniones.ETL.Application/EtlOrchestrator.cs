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
        private readonly IFactLoaderService _factLoaderService;
        private readonly EtlProcessOptions _options;

        public EtlOrchestrator(
            ILogger<EtlOrchestrator> logger,
            IExtractorFactory extractorFactory,
            IStagingService stagingService,
            IDwhLoaderService dwhLoaderService,
            IFactLoaderService factLoaderService,
            IOptions<EtlProcessOptions> options)
        {
            _logger = logger;
            _extractorFactory = extractorFactory;
            _stagingService = stagingService;
            _dwhLoaderService = dwhLoaderService;
            _factLoaderService = factLoaderService;
            _options = options.Value;
        }

        public async Task ExecuteEtlProcessAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Iniciando ciclo completo ETL.");
            var totalStopwatch = Stopwatch.StartNew();

            foreach (var sourceName in _options.SourcesToRun)
            {
                if (cancellationToken.IsCancellationRequested) break;
                try
                {
                    _logger.LogInformation("--- Iniciando extracción: {FuenteNombre} ---", sourceName);
                    var extractor = _extractorFactory.GetExtractor(sourceName);
                    var extractedData = await extractor.ExtractAsync(cancellationToken);

                    if (extractedData.Any())
                    {
                        await _stagingService.StageDataAsync(extractedData, cancellationToken);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Fallo el extractor {FuenteNombre}.", sourceName);
                }
            }

            if (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    _logger.LogInformation("--- FASE 2: Carga Dimensiones DWH ---");
                    await _dwhLoaderService.LoadDimensionsAsync(cancellationToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error crítico durante la carga de dimensiones.");
                }

                try
                {
                    _logger.LogInformation("--- FASE 3: Carga Fact Table ---");
                    await _factLoaderService.LoadFactOpinionsAsync(cancellationToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error crítico durante la carga de Facts.");
                }
            }

            totalStopwatch.Stop();
            _logger.LogInformation("Proceso ETL finalizado en {Elapsed}ms.", totalStopwatch.ElapsedMilliseconds);
        }
    }
}