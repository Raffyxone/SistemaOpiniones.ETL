using System.Globalization;
using System.IO;
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

            await csv.ReadAsync();
            csv.ReadHeader();
            int rowNumber = 1;

            while (await csv.ReadAsync())
            {
                cancellationToken.ThrowIfCancellationRequested();
                rowNumber++;
                try
                {
                    var record = csv.GetRecord<EncuestaCsv>();
                    if (record != null)
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
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Error al leer la fila {RowNumber} del CSV: {RawRecord}. Fila omitida.", rowNumber, csv.Context.Parser.RawRecord);
                }
            }

            _logger.LogInformation("Extracción de CSV completada. {Count} registros leídos.", opiniones.Count);
            return opiniones;
        }
    }
}