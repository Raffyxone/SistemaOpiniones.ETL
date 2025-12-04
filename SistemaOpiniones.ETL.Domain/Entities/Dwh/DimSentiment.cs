using System;

namespace SistemaOpiniones.ETL.Domain.Entities.Dwh
{
    public class DimSentiment
    {
        public int SentimentKey { get; set; }
        public string? SentimentCode { get; set; }
        public string SentimentName { get; set; } = null!;
    }
}