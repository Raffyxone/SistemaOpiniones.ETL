using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SistemaOpiniones.ETL.Domain.Entities
{
    public class OpinionFuenteOltp
    {
        public int OpinionID { get; set; }
        public string FuenteOpinionID { get; set; }
        public int? ClienteID { get; set; }
        public int? ProductoID { get; set; }
        public int FuenteID { get; set; }
        public DateTime Fecha { get; set; }
        public string Comentario { get; set; }
        public string Clasificacion { get; set; }
        public byte? PuntajeSatisfaccion { get; set; }
    }
}
