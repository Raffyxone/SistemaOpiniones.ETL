using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using CsvHelper;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SistemaOpiniones.ETL.Domain.Entities;
using SistemaOpiniones.ETL.Domain.Interfaces;

namespace SistemaOpiniones.ETL.Infrastructure.Extractors
{
    public class CsvExtractor : IExtractor
    {
        private readonly ILogger<CsvExtractor> _logger;
        private readonly string _filePath;
        private readonly IHostEnvironment _env;

        public string FuenteNombre => "EncuestaInterna";

        public CsvExtractor(ILogger<CsvExtractor> logger, IOptions<FileSourcesOptions> options, IHostEnvironment env)
        {
            _logger = logger;
            _filePath = options.Value.EncuestasCsvPath;
            _env = env;
        }

        public async Task<IEnumerable<OpinionFuente>> ExtractAsync(CancellationToken cancellationToken)
        {
            var opiniones = new List<OpinionFuente>();
            string absolutePath = Path.Combine(_env.ContentRootPath, _filePath);
            if (!File.Exists(absolutePath))
            {
                string alt = Path.Combine(_env.ContentRootPath, "DataInput", Path.GetFileName(_filePath));
                if (File.Exists(alt)) absolutePath = alt;
            }
            _logger.LogInformation("Iniciando extracción de CSV desde: {FilePath}", absolutePath);
            using var reader = new StreamReader(absolutePath);
            using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
            await foreach (var record in csv.GetRecordsAsync<EncuestaCsv>(cancellationToken))
            {
                opiniones.Add(new OpinionFuente
                {
                    OpinionFuenteId = record.EncuestaId.ToString(),
                    FuenteNombre = FuenteNombre,
                    FechaOpinion = record.Fecha,
                    ClienteIdExterno = record.ClienteId,
                    ProductoIdExterno = record.ProductoId,
                    Calificacion = record.Calificacion,
                    Comentario = record.Comentario,
                    SentimientoDetectado = record.Sentimiento
                });
            }
            _logger.LogInformation("Extracción de CSV completada. {Count} registros leídos.", opiniones.Count);
            return opiniones;
        }
    }
}