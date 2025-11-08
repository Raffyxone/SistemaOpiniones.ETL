using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SistemaOpiniones.ETL.Infrastructure.Extractors
{
    public class FileSourcesOptions
    {
        public const string SectionName = "FileSources";
        public string EncuestasCsvPath { get; set; }
    }
}
