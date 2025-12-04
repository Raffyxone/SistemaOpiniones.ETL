using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using SistemaOpiniones.ETL.Domain.Interfaces;
using SistemaOpiniones.ETL.Infrastructure.Persistence;
using System;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SistemaOpiniones.ETL.Infrastructure.Services
{
    public class DwhLoaderService : IDwhLoaderService
    {
        private readonly ILogger<DwhLoaderService> _logger;
        private readonly IDbContextFactory<StagingDbContext> _stagingFactory;
        private readonly IDbContextFactory<DwhDbContext> _dwhFactory;

        public DwhLoaderService(
            ILogger<DwhLoaderService> logger,
            IDbContextFactory<StagingDbContext> stagingFactory,
            IDbContextFactory<DwhDbContext> dwhFactory)
        {
            _logger = logger;
            _stagingFactory = stagingFactory;
            _dwhFactory = dwhFactory;
        }

        public async Task LoadDimensionsAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation(">>> Iniciando carga de Dimensiones al DWH...");

            await LoadDimSource(cancellationToken);
            await LoadDimSentiment(cancellationToken);
            await LoadDimCustomer(cancellationToken);
            await LoadDimProduct(cancellationToken);

            _logger.LogInformation(">>> Carga de Dimensiones finalizada.");
        }

        private async Task LoadDimSource(CancellationToken cancellationToken)
        {
            using var staging = await _stagingFactory.CreateDbContextAsync(cancellationToken);
            using var dwh = await _dwhFactory.CreateDbContextAsync(cancellationToken);

            var sources = await staging.OpinionesStaging
                .Select(s => new { s.FuenteNombre })
                .Distinct()
                .ToListAsync(cancellationToken);

            var table = new DataTable();
            table.Columns.Add("SourceName", typeof(string));
            table.Columns.Add("SourceType", typeof(string));

            foreach (var item in sources)
            {
                string type = item.FuenteNombre.Contains("Csv", StringComparison.OrdinalIgnoreCase) ? "Archivo" : "Sistema";
                table.Rows.Add(item.FuenteNombre, type);
            }

            var param = new SqlParameter("@tvp", SqlDbType.Structured) { TypeName = "[Dimension].[DimSourceType]", Value = table };

            var sql = @"
                MERGE INTO [Dimension].[DimSource] AS Target
                USING @tvp AS Source
                ON Target.SourceName = Source.SourceName
                WHEN NOT MATCHED THEN
                    INSERT (SourceName, SourceType) VALUES (Source.SourceName, Source.SourceType);";

            await dwh.Database.ExecuteSqlRawAsync(sql, new[] { param }, cancellationToken);
            _logger.LogInformation($"Dimension Fuentes procesada. {sources.Count} registros.");
        }

        private async Task LoadDimSentiment(CancellationToken cancellationToken)
        {
            using var staging = await _stagingFactory.CreateDbContextAsync(cancellationToken);
            using var dwh = await _dwhFactory.CreateDbContextAsync(cancellationToken);

            var sentiments = await staging.OpinionesStaging
                .Where(s => s.SentimientoDetectado != null)
                .Select(s => s.SentimientoDetectado)
                .Distinct()
                .ToListAsync(cancellationToken);

            var table = new DataTable();
            table.Columns.Add("SentimentName", typeof(string));
            table.Columns.Add("SentimentCode", typeof(string));

            foreach (var s in sentiments)
            {
                var code = s!.Length >= 3 ? s.Substring(0, 3).ToUpper() : s.ToUpper();
                table.Rows.Add(s, code);
            }

            var param = new SqlParameter("@tvp", SqlDbType.Structured) { TypeName = "[Dimension].[DimSentimentType]", Value = table };

            var sql = @"
                MERGE INTO [Dimension].[DimSentiment] AS Target
                USING @tvp AS Source
                ON Target.SentimentName = Source.SentimentName
                WHEN NOT MATCHED THEN
                    INSERT (SentimentName, SentimentCode) VALUES (Source.SentimentName, Source.SentimentCode);";

            await dwh.Database.ExecuteSqlRawAsync(sql, new[] { param }, cancellationToken);
            _logger.LogInformation($"Dimension Sentimientos procesada. {sentiments.Count} registros.");
        }

        private async Task LoadDimCustomer(CancellationToken cancellationToken)
        {
            using var staging = await _stagingFactory.CreateDbContextAsync(cancellationToken);
            using var dwh = await _dwhFactory.CreateDbContextAsync(cancellationToken);

            var customers = await staging.OpinionesStaging
                .Where(c => c.ClienteIdExterno != null)
                .Select(c => new
                {
                    ID = c.ClienteIdExterno,
                    Name = "Cliente " + c.ClienteIdExterno
                })
                .Distinct()
                .ToListAsync(cancellationToken);

            var table = new DataTable();
            table.Columns.Add("CustomerID", typeof(string));
            table.Columns.Add("FullName", typeof(string));
            table.Columns.Add("Email", typeof(string));

            foreach (var c in customers)
            {
                object emailValue = DBNull.Value;
                if (c.ID != null && c.ID.Contains("@"))
                {
                    emailValue = c.ID;
                }

                table.Rows.Add(c.ID, c.Name, emailValue);
            }

            var param = new SqlParameter("@tvp", SqlDbType.Structured) { TypeName = "[Dimension].[DimCustomerType]", Value = table };

            var sql = @"
                MERGE INTO [Dimension].[DimCustomer] AS Target
                USING @tvp AS Source
                ON Target.CustomerID = Source.CustomerID
                WHEN MATCHED THEN
                    UPDATE SET 
                        Target.FullName = Source.FullName,
                        Target.Email = Source.Email  -- Actualizamos Email
                WHEN NOT MATCHED THEN
                    INSERT (CustomerID, FullName, Email, IsCurrentRecord, DateFrom)
                    VALUES (Source.CustomerID, Source.FullName, Source.Email, 1, GETDATE());";

            await dwh.Database.ExecuteSqlRawAsync(sql, new[] { param }, cancellationToken);
            _logger.LogInformation($"Dimension Clientes procesada. {customers.Count} registros.");
        }

        private async Task LoadDimProduct(CancellationToken cancellationToken)
        {
            using var staging = await _stagingFactory.CreateDbContextAsync(cancellationToken);
            using var dwh = await _dwhFactory.CreateDbContextAsync(cancellationToken);

            var products = await staging.OpinionesStaging
                .Where(p => p.ProductoIdExterno != null)
                .Select(p => new { ID = p.ProductoIdExterno })
                .Distinct()
                .ToListAsync(cancellationToken);

            var table = new DataTable();
            table.Columns.Add("ProductID", typeof(string));
            table.Columns.Add("ProductName", typeof(string));
            table.Columns.Add("Brand", typeof(string));   
            table.Columns.Add("Price", typeof(decimal));
            table.Columns.Add("CategoryName", typeof(string));

            foreach (var p in products)
            {
                table.Rows.Add(p.ID, $"Producto {p.ID}", DBNull.Value, DBNull.Value, "General");
            }

            var param = new SqlParameter("@tvp", SqlDbType.Structured) { TypeName = "[Dimension].[DimProductType]", Value = table };

            var sql = @"
                MERGE INTO [Dimension].[DimProduct] AS Target
                USING @tvp AS Source
                ON Target.ProductID = Source.ProductID
                WHEN MATCHED THEN
                    UPDATE SET Target.ProductName = Source.ProductName
                WHEN NOT MATCHED THEN
                    INSERT (ProductID, ProductName, Brand, Price, CategoryName, IsCurrentRecord, DateFrom)
                    VALUES (Source.ProductID, Source.ProductName, Source.Brand, Source.Price, Source.CategoryName, 1, GETDATE());";

            await dwh.Database.ExecuteSqlRawAsync(sql, new[] { param }, cancellationToken);
            _logger.LogInformation($"Dimension Productos procesada. {products.Count} registros.");
        }
    }
}