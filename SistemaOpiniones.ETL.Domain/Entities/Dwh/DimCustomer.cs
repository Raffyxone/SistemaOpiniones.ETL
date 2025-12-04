using System;

namespace SistemaOpiniones.ETL.Domain.Entities.Dwh
{
    public class DimCustomer
    {
        public int CustomerKey { get; set; }
        public string? CustomerID { get; set; }
        public string? FullName { get; set; }
        public string? Email { get; set; }
        public string? Country { get; set; }
        public string? City { get; set; }
        public int? Age { get; set; }
        public string? Segment { get; set; }
        public DateTime? DateFrom { get; set; }
        public DateTime? DateTo { get; set; }
        public bool IsCurrentRecord { get; set; }
    }
}