using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SistemaOpiniones.ETL.Domain.Interfaces
{
    public interface IEtlOrchestrator
    {
        Task ExecuteEtlProcessAsync(CancellationToken cancellationToken);
    }
}
