using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;
using SistemaOpiniones.ETL.Application;
using SistemaOpiniones.ETL.Domain.Interfaces;
using SistemaOpiniones.ETL.Domain.Interfaces.Repositories;
using SistemaOpiniones.ETL.Infrastructure.Extractors;
using SistemaOpiniones.ETL.Infrastructure.Factories;
using SistemaOpiniones.ETL.Infrastructure.Persistence;
using SistemaOpiniones.ETL.Infrastructure.Repositories;
using SistemaOpiniones.ETL.Infrastructure.Services;
using SistemaOpiniones.ETL.Worker;
using System;

public class Program
{
    public static void Main(string[] args)
    {
        IHost host = CreateHostBuilder(args).Build();
        host.Run();
    }

    public static IHostBuilder CreateHostBuilder(string[] args) =>
        Host.CreateDefaultBuilder(args)
            .UseSerilog((context, services, configuration) => configuration.ReadFrom.Configuration(context.Configuration))
            .ConfigureServices((hostContext, services) =>
            {
                IConfiguration configuration = hostContext.Configuration;

                services.AddOptions<EtlProcessOptions>().Bind(configuration.GetSection(EtlProcessOptions.SectionName));
                services.AddOptions<FileSourcesOptions>().Bind(configuration.GetSection(FileSourcesOptions.SectionName));

                services.AddDbContextFactory<FuenteDatosDbContext>(options =>
                    options.UseSqlServer(configuration.GetConnectionString("FuenteReseñasConnection")));

                services.AddDbContextFactory<StagingDbContext>(options =>
                    options.UseSqlServer(configuration.GetConnectionString("StagingConnection")));

                services.AddDbContextFactory<DwhDbContext>(options =>
                    options.UseSqlServer(configuration.GetConnectionString("DwhConnection")));

                services.AddHttpClient("ComentariosApi", (serviceProvider, client) =>
                {
                    var apiOptions = configuration.GetSection("ApiSources:ComentariosApi");
                    var baseUrl = apiOptions["BaseUrl"];
                    if (!string.IsNullOrEmpty(baseUrl))
                    {
                        client.BaseAddress = new Uri(baseUrl);
                    }

                    if (!string.IsNullOrEmpty(apiOptions["ApiKey"]))
                    {
                        client.DefaultRequestHeaders.Add("X-Api-Key", apiOptions["ApiKey"]);
                    }
                });

                services.AddScoped<IEtlOrchestrator, EtlOrchestrator>();
                services.AddScoped<IExtractor, CsvExtractor>();
                services.AddScoped<IExtractor, DatabaseExtractor>();
                services.AddScoped<IExtractor, ApiExtractor>();
                services.AddScoped<IOpinionRepository, OpinionRepository>();
                services.AddScoped<IExtractorFactory, ExtractorFactory>();
                services.AddScoped<IStagingService, StagingService>();

                services.AddScoped<IDwhLoaderService, DwhLoaderService>();
                services.AddScoped<IFactLoaderService, FactLoaderService>();

                services.AddHostedService<Worker>();
            });
}