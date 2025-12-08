using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SistemaOpiniones.ETL.Domain.Entities.Dwh
{
    [Table("FactOpinions", Schema = "Fact")]
    public class FactOpinion
    {
        [Key]
        public int OpinionKey { get; set; }
        public string? SourceOpinionID { get; set; }
        public int DateKey { get; set; }
        public int ProductKey { get; set; }
        public int CustomerKey { get; set; }
        public int SourceKey { get; set; }
        public int SentimentKey { get; set; }
        public byte SatisfactionScore { get; set; }
    }
}
