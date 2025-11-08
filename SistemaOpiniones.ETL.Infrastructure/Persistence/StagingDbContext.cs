using Microsoft.EntityFrameworkCore;
using SistemaOpiniones.ETL.Domain.Entities;

namespace SistemaOpiniones.ETL.Infrastructure.Persistence
{
    public class StagingDbContext : DbContext
    {
        public StagingDbContext(DbContextOptions<StagingDbContext> options)
            : base(options)
        {
        }

        public DbSet<OpinionFuente> OpinionesStaging { get; set; } = null!;

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<OpinionFuente>().ToTable("OpinionesStaging");
            modelBuilder.Entity<OpinionFuente>().HasKey(o => new { o.FuenteNombre, o.OpinionFuenteId });
            modelBuilder.Entity<OpinionFuente>().Property(o => o.OpinionFuenteId).HasMaxLength(255).IsRequired();
            modelBuilder.Entity<OpinionFuente>().Property(o => o.FuenteNombre).HasMaxLength(100).IsRequired();
            modelBuilder.Entity<OpinionFuente>().Property(o => o.ClienteIdExterno).IsRequired(false).HasMaxLength(255);
            modelBuilder.Entity<OpinionFuente>().Property(o => o.ProductoIdExterno).IsRequired(false).HasMaxLength(255);
            modelBuilder.Entity<OpinionFuente>().Property(o => o.Comentario).IsRequired(false);
            modelBuilder.Entity<OpinionFuente>().Property(o => o.SentimientoDetectado).IsRequired(false).HasMaxLength(50);
        }
    }
}
