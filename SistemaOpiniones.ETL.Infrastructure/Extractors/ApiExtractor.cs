using Microsoft.Extensions.Logging;
using SistemaOpiniones.ETL.Domain.Entities;
using SistemaOpiniones.ETL.Domain.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http.Json;
using System.Text;
using System.Threading.Tasks;

namespace SistemaOpiniones.ETL.Infrastructure.Extractors
{
    public class ApiExtractor : IExtractor
    {
        private readonly ILogger<ApiExtractor> _logger;
        private readonly HttpClient _httpClient;

        public string FuenteNombre => "ComentariosApi";

        public ApiExtractor(ILogger<ApiExtractor> logger, IHttpClientFactory httpClientFactory)
        {
            _logger = logger;
            _httpClient = httpClientFactory.CreateClient(FuenteNombre);
        }

        public async Task<IEnumerable<OpinionFuente>> ExtractAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Iniciando extracción de API REST (Comentarios de prueba).");
            var opiniones = new List<OpinionFuente>();

            try
            {
                var response = await _httpClient.GetFromJsonAsync<List<JsonPlaceholderComment>>("comments?_limit=5", cancellationToken);

                if (response != null)
                {
                    foreach (var review in response)
                    {
                        opiniones.Add(new OpinionFuente
                        {
                            OpinionFuenteId = review.Id.ToString(),
                            FuenteNombre = FuenteNombre,
                            FechaOpinion = DateTime.UtcNow,
                            ClienteIdExterno = review.Email,
                            ProductoIdExterno = review.PostId.ToString(),
                            Calificacion = (review.Id % 5) + 1,
                            Comentario = review.Body,
                            SentimientoDetectado = null!
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al extraer datos de la API REST.");
                throw;
            }

            _logger.LogInformation("Extracción de API REST completada. {Count} registros leídos.", opiniones.Count);
            return opiniones;
        }
    }
}