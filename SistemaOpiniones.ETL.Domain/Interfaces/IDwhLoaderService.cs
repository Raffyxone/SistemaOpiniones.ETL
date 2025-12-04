using System.Threading;
using System.Threading.Tasks;

namespace SistemaOpiniones.ETL.Domain.Interfaces
{
    public interface IDwhLoaderService
    {
        Task LoadDimensionsAsync(CancellationToken cancellationToken);
    }
}