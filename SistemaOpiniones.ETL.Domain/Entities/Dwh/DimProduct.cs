using System;

namespace SistemaOpiniones.ETL.Domain.Entities.Dwh
{
    public class DimProduct
    {
        public int ProductKey { get; set; }
        public string? ProductID { get; set; }
        public string ProductName { get; set; } = null!;
        public string? Brand { get; set; }
        public decimal? Price { get; set; }
        public string? CategoryName { get; set; }
        public DateTime? DateFrom { get; set; }
        public DateTime? DateTo { get; set; }
        public bool IsCurrentRecord { get; set; }
    }
}