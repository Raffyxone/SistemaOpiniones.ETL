using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SistemaOpiniones.ETL.Domain.Entities
{
    public class EncuestaCsv
    {
        public int EncuestaId { get; set; }
        public string ClienteId { get; set; }
        public string ProductoId { get; set; }
        public DateTime Fecha { get; set; }
        public int Calificacion { get; set; }
        public string Comentario { get; set; }
        public string Sentimiento { get; set; }
    }
}
