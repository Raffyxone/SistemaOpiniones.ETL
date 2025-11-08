using Microsoft.EntityFrameworkCore;
using SistemaOpiniones.ETL.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SistemaOpiniones.ETL.Infrastructure.Persistence
{
    public class FuenteDatosDbContext : DbContext
    {
        public FuenteDatosDbContext(DbContextOptions<FuenteDatosDbContext> options)
            : base(options)
        {
        }

        public DbSet<OpinionFuenteOltp> Opiniones { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<OpinionFuenteOltp>(e =>
            {
                e.ToTable("Opiniones");
                e.HasKey(o => o.OpinionID);
            });
        }
    }
}
