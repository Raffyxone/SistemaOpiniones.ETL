using Microsoft.EntityFrameworkCore;
using SistemaOpiniones.ETL.Domain.Entities;
using SistemaOpiniones.ETL.Domain.Interfaces.Repositories;
using SistemaOpiniones.ETL.Infrastructure.Persistence;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SistemaOpiniones.ETL.Infrastructure.Repositories
{
    public class OpinionRepository : IOpinionRepository
    {
        private readonly IDbContextFactory<FuenteDatosDbContext> _dbContextFactory;

        public OpinionRepository(IDbContextFactory<FuenteDatosDbContext> dbContextFactory)
        {
            _dbContextFactory = dbContextFactory;
        }

        public async Task<IEnumerable<OpinionFuenteOltp>> GetNewOpinionsAsync(DateTime sinceDate, CancellationToken cancellationToken)
        {
            await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

            var reseñas = await dbContext.Opiniones
                .AsNoTracking()
                .Where(r => r.Fecha > sinceDate)
                .ToListAsync(cancellationToken);

            return reseñas;
        }
    }
}
