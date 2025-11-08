using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SistemaOpiniones.ETL.Domain.Entities
{
    public class OpinionFuente
    {
        public string OpinionFuenteId { get; set; } = null!;
        public string FuenteNombre { get; set; } = null!;
        public DateTime FechaOpinion { get; set; }
        public string? ClienteIdExterno { get; set; }
        public string? ProductoIdExterno { get; set; }
        public int Calificacion { get; set; }
        public string? Comentario { get; set; }
        public string? SentimientoDetectado { get; set; }
    }
}
