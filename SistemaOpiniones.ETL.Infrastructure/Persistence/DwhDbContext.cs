using Microsoft.EntityFrameworkCore;
using SistemaOpiniones.ETL.Domain.Entities.Dwh;

namespace SistemaOpiniones.ETL.Infrastructure.Persistence
{
    public class DwhDbContext : DbContext
    {
        public DwhDbContext(DbContextOptions<DwhDbContext> options) : base(options) { }

        public DbSet<DimCustomer> DimCustomers { get; set; }
        public DbSet<DimProduct> DimProducts { get; set; }
        public DbSet<DimSource> DimSources { get; set; }
        public DbSet<DimSentiment> DimSentiments { get; set; }
        public DbSet<FactOpinion> FactOpinions { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<DimCustomer>().ToTable("DimCustomer", "Dimension").HasKey(e => e.CustomerKey);
            modelBuilder.Entity<DimProduct>().ToTable("DimProduct", "Dimension").HasKey(e => e.ProductKey);
            modelBuilder.Entity<DimSource>().ToTable("DimSource", "Dimension").HasKey(e => e.SourceKey);
            modelBuilder.Entity<DimSentiment>().ToTable("DimSentiment", "Dimension").HasKey(e => e.SentimentKey);

            modelBuilder.Entity<DimProduct>().Property(p => p.Price).HasColumnType("decimal(18,2)");

            modelBuilder.Entity<FactOpinion>().ToTable("FactOpinions", "Fact").HasKey(e => e.OpinionKey);
        }
    }
}