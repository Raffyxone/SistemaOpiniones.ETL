using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using SistemaOpiniones.ETL.Domain.Entities.Dwh;
using SistemaOpiniones.ETL.Domain.Interfaces;
using SistemaOpiniones.ETL.Infrastructure.Persistence;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SistemaOpiniones.ETL.Infrastructure.Services
{
    public class FactLoaderService : IFactLoaderService
    {
        private readonly ILogger<FactLoaderService> _logger;
        private readonly IDbContextFactory<StagingDbContext> _stagingFactory;
        private readonly IDbContextFactory<DwhDbContext> _dwhFactory;

        public FactLoaderService(
            ILogger<FactLoaderService> logger,
            IDbContextFactory<StagingDbContext> stagingFactory,
            IDbContextFactory<DwhDbContext> dwhFactory)
        {
            _logger = logger;
            _stagingFactory = stagingFactory;
            _dwhFactory = dwhFactory;
        }

        public async Task LoadFactOpinionsAsync(CancellationToken cancellationToken)
        {
            using var staging = await _stagingFactory.CreateDbContextAsync(cancellationToken);
            using var dwh = await _dwhFactory.CreateDbContextAsync(cancellationToken);

            _logger.LogInformation(">>> Iniciando Carga de Fact Table (FactOpinions)...");

            _logger.LogInformation("Limpiando datos existentes en FactOpinions...");
            await dwh.FactOpinions.ExecuteDeleteAsync(cancellationToken);

            var clientesMap = await dwh.DimCustomers.ToDictionaryAsync(x => x.CustomerID ?? "", x => x.CustomerKey, cancellationToken);
            var productosMap = await dwh.DimProducts.ToDictionaryAsync(x => x.ProductID ?? "", x => x.ProductKey, cancellationToken);
            var fuentesMap = await dwh.DimSources.ToDictionaryAsync(x => x.SourceName, x => x.SourceKey, cancellationToken);
            var sentimientosMap = await dwh.DimSentiments.ToDictionaryAsync(x => x.SentimentName, x => x.SentimentKey, cancellationToken);

            var stagingData = await staging.OpinionesStaging.ToListAsync(cancellationToken);
            var factsToInsert = new List<FactOpinion>();

            foreach (var row in stagingData)
            {
                int customerKey = clientesMap.ContainsKey(row.ClienteIdExterno ?? "") ? clientesMap[row.ClienteIdExterno!] : -1;
                int productKey = productosMap.ContainsKey(row.ProductoIdExterno ?? "") ? productosMap[row.ProductoIdExterno!] : -1;
                int sourceKey = fuentesMap.ContainsKey(row.FuenteNombre) ? fuentesMap[row.FuenteNombre] : -1;
                int sentimentKey = sentimientosMap.ContainsKey(row.SentimientoDetectado ?? "") ? sentimientosMap[row.SentimientoDetectado!] : -1;

                int dateKey = (row.FechaOpinion.Year * 10000) + (row.FechaOpinion.Month * 100) + row.FechaOpinion.Day;

                if (customerKey != -1 && productKey != -1 && sourceKey != -1)
                {
                    factsToInsert.Add(new FactOpinion
                    {
                        SourceOpinionID = row.OpinionFuenteId,
                        DateKey = dateKey,
                        CustomerKey = customerKey,
                        ProductKey = productKey,
                        SourceKey = sourceKey,
                        SentimentKey = sentimentKey,
                        SatisfactionScore = (byte)row.Calificacion
                    });
                }
            }

            if (factsToInsert.Any())
            {
                await dwh.FactOpinions.AddRangeAsync(factsToInsert, cancellationToken);
                await dwh.SaveChangesAsync(cancellationToken);
                _logger.LogInformation($"Carga de Facts completada. {factsToInsert.Count} registros insertados.");
            }
            else
            {
                _logger.LogWarning("No se encontraron registros válidos para insertar en la Fact Table.");
            }
        }
    }
}