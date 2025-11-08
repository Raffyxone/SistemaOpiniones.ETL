using Microsoft.Extensions.Logging;
using SistemaOpiniones.ETL.Domain.Entities;
using SistemaOpiniones.ETL.Domain.Interfaces;
using SistemaOpiniones.ETL.Domain.Interfaces.Repositories;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SistemaOpiniones.ETL.Infrastructure.Extractors
{
    public class DatabaseExtractor : IExtractor
    {
        private readonly ILogger<DatabaseExtractor> _logger;
        private readonly IOpinionRepository _opinionRepository;

        public string FuenteNombre => "BaseDatosReseñas";

        public DatabaseExtractor(ILogger<DatabaseExtractor> logger, IOpinionRepository opinionRepository)
        {
            _logger = logger;
            _opinionRepository = opinionRepository;
        }

        public async Task<IEnumerable<OpinionFuente>> ExtractAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Iniciando extracción de Base de Datos OLTP (Reseñas).");
            var opiniones = new List<OpinionFuente>();

            try
            {
                var fechaDesde = DateTime.UtcNow.AddDays(-1);
                var reseñas = await _opinionRepository.GetNewOpinionsAsync(fechaDesde, cancellationToken);

                foreach (var reseña in reseñas)
                {
                    opiniones.Add(new OpinionFuente
                    {
                        OpinionFuenteId = reseña.FuenteOpinionID ?? reseña.OpinionID.ToString(),
                        FuenteNombre = FuenteNombre,
                        FechaOpinion = reseña.Fecha,
                        ClienteIdExterno = reseña.ClienteID?.ToString(),
                        ProductoIdExterno = reseña.ProductoID?.ToString(),
                        Calificacion = reseña.PuntajeSatisfaccion ?? 0,
                        Comentario = reseña.Comentario,
                        SentimientoDetectado = reseña.Clasificacion
                    });
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al extraer datos de la Base de Datos.");
                throw;
            }

            _logger.LogInformation("Extracción de Base de Datos OLTP completada. {Count} registros leídos.", opiniones.Count);
            return opiniones;
        }
    }
}
