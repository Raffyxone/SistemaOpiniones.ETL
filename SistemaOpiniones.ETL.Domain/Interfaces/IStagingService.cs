using SistemaOpiniones.ETL.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SistemaOpiniones.ETL.Domain.Interfaces
{
    public interface IStagingService
    {
        Task StageDataAsync(IEnumerable<OpinionFuente> datos, CancellationToken cancellationToken);
    }
}
