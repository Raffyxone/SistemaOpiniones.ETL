using SistemaOpiniones.ETL.Domain.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SistemaOpiniones.ETL.Infrastructure.Factories
{
    public class ExtractorFactory : IExtractorFactory
    {
        private readonly IReadOnlyDictionary<string, IExtractor> _extractors;

        public ExtractorFactory(IEnumerable<IExtractor> extractors)
        {
            _extractors = extractors.ToDictionary(e => e.FuenteNombre, StringComparer.OrdinalIgnoreCase);
        }

        public IExtractor GetExtractor(string sourceName)
        {
            if (_extractors.TryGetValue(sourceName, out var extractor))
            {
                return extractor;
            }

            throw new NotSupportedException($"El extractor para la fuente '{sourceName}' no está registrado.");
        }
    }
}
