using SistemaOpiniones.ETL.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SistemaOpiniones.ETL.Domain.Interfaces.Repositories
{
    public interface IOpinionRepository
    {
        Task<IEnumerable<OpinionFuenteOltp>> GetNewOpinionsAsync(DateTime sinceDate, CancellationToken cancellationToken);
    }
}
