using System;

namespace SistemaOpiniones.ETL.Domain.Entities.Dwh
{
    public class DimSource
    {
        public int SourceKey { get; set; }
        public int? SourceID { get; set; }
        public string? SourceType { get; set; }
        public string SourceName { get; set; } = null!;
    }
}