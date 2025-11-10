using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using SistemaOpiniones.ETL.Domain.Entities;
using SistemaOpiniones.ETL.Domain.Interfaces;
using SistemaOpiniones.ETL.Infrastructure.Persistence;
using System.Data;
using Microsoft.Data.SqlClient;

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

            var dataTable = new DataTable();
            dataTable.Columns.Add("OpinionFuenteId", typeof(string));
            dataTable.Columns.Add("FuenteNombre", typeof(string));
            dataTable.Columns.Add("FechaOpinion", typeof(DateTime));
            dataTable.Columns.Add("ClienteIdExterno", typeof(string));
            dataTable.Columns.Add("ProductoIdExterno", typeof(string));
            dataTable.Columns.Add("Calificacion", typeof(int));
            dataTable.Columns.Add("Comentario", typeof(string));
            dataTable.Columns.Add("SentimientoDetectado", typeof(string));

            foreach (var newRecord in datosList)
            {
                dataTable.Rows.Add(
                    newRecord.OpinionFuenteId,
                    newRecord.FuenteNombre,
                    newRecord.FechaOpinion,
                    (object?)newRecord.ClienteIdExterno ?? DBNull.Value,
                    (object?)newRecord.ProductoIdExterno ?? DBNull.Value,
                    newRecord.Calificacion,
                    (object?)newRecord.Comentario ?? DBNull.Value,
                    (object?)newRecord.SentimientoDetectado ?? DBNull.Value
                );
            }

            var sqlParameter = new SqlParameter("@tvpData", SqlDbType.Structured)
            {
                TypeName = "dbo.OpinionFuenteType",
                Value = dataTable
            };

            var mergeSql = @"
                MERGE INTO OpinionesStaging AS Target
                USING @tvpData AS Source
                ON Target.FuenteNombre = Source.FuenteNombre AND Target.OpinionFuenteId = Source.OpinionFuenteId
                WHEN MATCHED THEN
                    UPDATE SET
                        Target.FechaOpinion = Source.FechaOpinion,
                        Target.ClienteIdExterno = Source.ClienteIdExterno,
                        Target.ProductoIdExterno = Source.ProductoIdExterno,
                        Target.Calificacion = Source.Calificacion,
                        Target.Comentario = Source.Comentario,
                        Target.SentimientoDetectado = Source.SentimientoDetectado
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (OpinionFuenteId, FuenteNombre, FechaOpinion, ClienteIdExterno, ProductoIdExterno, Calificacion, Comentario, SentimientoDetectado)
                    VALUES (Source.OpinionFuenteId, Source.FuenteNombre, Source.FechaOpinion, Source.ClienteIdExterno, Source.ProductoIdExterno, Source.Calificacion, Source.Comentario, Source.SentimientoDetectado);
            ";

            try
            {
                await dbContext.Database.ExecuteSqlRawAsync(mergeSql, new[] { sqlParameter }, cancellationToken);
                _logger.LogInformation("Datos guardados en staging exitosamente (operación MERGE).");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error durante la operación MERGE masiva en staging.");
                throw;
            }
        }
    }
}