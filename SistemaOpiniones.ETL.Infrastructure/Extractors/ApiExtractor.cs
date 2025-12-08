using Microsoft.Extensions.Logging;
using SistemaOpiniones.ETL.Domain.Entities;
using SistemaOpiniones.ETL.Domain.Interfaces;
using System.Net.Http.Json;

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
                var response = await _httpClient.GetFromJsonAsync<List<JsonPlaceholderComment>>("comments?_limit=500", cancellationToken);

                if (response != null)
                {
                    foreach (var review in response)
                    {
                        try
                        {
                            var score = (review.Id % 5) + 1;

                            string sentimiento = score >= 4 ? "Positivo" : (score == 3 ? "Neutro" : "Negativo");

                            opiniones.Add(new OpinionFuente
                            {
                                OpinionFuenteId = review.Id.ToString(),
                                FuenteNombre = FuenteNombre,
                                FechaOpinion = DateTime.UtcNow,
                                ClienteIdExterno = review.Email,
                                ProductoIdExterno = review.PostId.ToString(),
                                Calificacion = score,
                                Comentario = review.Body,
                                SentimientoDetectado = sentimiento
                            });
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex, "Error al mapear registro de API con id: {ApiId}. Registro omitido.", review.Id);
                        }
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