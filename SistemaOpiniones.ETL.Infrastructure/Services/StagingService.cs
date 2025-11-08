using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using SistemaOpiniones.ETL.Domain.Entities;
using SistemaOpiniones.ETL.Domain.Interfaces;
using SistemaOpiniones.ETL.Infrastructure.Persistence;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SistemaOpiniones.ETL.Infrastructure.Services
{
    public class StagingService : IStagingService
    {
        private readonly ILogger<StagingService> _logger;
        private readonly IDbContextFactory<StagingDbContext> _dbContextFactory;

        public StagingService(ILogger<StagingService> logger, IDbContextFactory<StagingDbContext> dbContextFactory)
        {
            _logger = logger;
            _dbContextFactory = dbContextFactory;
        }

        public async Task StageDataAsync(IEnumerable<OpinionFuente> datos, CancellationToken cancellationToken)
        {
            if (datos == null || !datos.Any())
            {
                _logger.LogInformation("No hay datos para guardar en staging.");
                return;
            }

            var datosList = datos.ToList();
            _logger.LogInformation("Guardando {Count} registros en la tabla de staging...", datosList.Count);

            await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);
            foreach (var newRecord in datosList)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var fuente = newRecord.FuenteNombre ?? string.Empty;
                var id = newRecord.OpinionFuenteId ?? string.Empty;

                var existing = await dbContext.OpinionesStaging
                    .FirstOrDefaultAsync(o => o.FuenteNombre == fuente && o.OpinionFuenteId == id, cancellationToken);

                if (existing != null)
                {
                    existing.FechaOpinion = newRecord.FechaOpinion;
                    existing.ClienteIdExterno = newRecord.ClienteIdExterno;
                    existing.ProductoIdExterno = newRecord.ProductoIdExterno;
                    existing.Calificacion = newRecord.Calificacion;
                    existing.Comentario = newRecord.Comentario;
                    existing.SentimientoDetectado = newRecord.SentimientoDetectado;
                    dbContext.OpinionesStaging.Update(existing);
                }
                else
                {
                    await dbContext.OpinionesStaging.AddAsync(newRecord, cancellationToken);
                }
            }

            await dbContext.SaveChangesAsync(cancellationToken);
            _logger.LogInformation("Datos guardados en staging exitosamente.");
        }
    }
}
