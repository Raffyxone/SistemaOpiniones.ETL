# SistemaOpiniones.ETL

Proceso ETL que consolida opiniones de clientes desde tres fuentes heterogéneas y las carga en un Data Warehouse en esquema estrella para su análisis.

## Qué hace

Un `Worker Service` (`BackgroundService`) ejecuta cada hora un ciclo ETL completo:

1. **Extracción** — lee las fuentes configuradas en `EtlProcess:SourcesToRun` a través de un `IExtractorFactory` que selecciona el extractor correspondiente:
   - `CsvExtractor` — encuestas internas desde un archivo CSV (`CsvHelper`).
   - `DatabaseExtractor` — reseñas desde una base de datos SQL Server operacional (OLTP).
   - `ApiExtractor` — comentarios desde una API REST externa.
2. **Staging** — los datos extraídos, sin importar su origen, se normalizan y persisten en una base de *staging* intermedia.
3. **Carga al Data Warehouse** — desde staging se cargan primero las dimensiones (`DimCustomer`, `DimProduct`, `DimSource`, `DimSentiment`, `DimDate`) y después la tabla de hechos (`FactOpinion`), separadas en los esquemas `Dimension` y `Fact` de SQL Server.

`DimCustomer` implementa una dimensión de cambio lento (**SCD Tipo 2**): cada cambio en los datos de un cliente conserva el historial mediante las columnas `DateFrom`, `DateTo` e `IsCurrentRecord`, en vez de sobrescribir el registro.

## Arquitectura

Clean Architecture en cuatro proyectos:

| Proyecto | Responsabilidad |
|---|---|
| `SistemaOpiniones.ETL.Domain` | Entidades (fuente y DW) e interfaces del proceso (`IEtlOrchestrator`, `IExtractor`, `IStagingService`, `IDwhLoaderService`, `IFactLoaderService`). |
| `SistemaOpiniones.ETL.Application` | `EtlOrchestrator`, que coordina las tres fases del ciclo con manejo de errores por fuente (un extractor que falla no detiene a los demás). |
| `SistemaOpiniones.ETL.Infrastructure` | Extractores concretos, factory, repositorios y acceso a datos (EF Core + `Z.EntityFramework.Extensions` para carga masiva). |
| `SistemaOpiniones.ETL.Worker` | Punto de entrada: hospeda el `BackgroundService` y la configuración. |

## Tecnologías

C# · .NET · Entity Framework Core (SQL Server) · CsvHelper · Serilog (consola y archivo, rotación diaria) · SQL Server (OLTP, Staging y Data Warehouse en esquema estrella).

## Configuración

Las cadenas de conexión (fuente OLTP, staging y DW), la URL de la API externa y la ruta del CSV se definen en `appsettings.json` / `appsettings.Development.json`. `SourcesToRun` controla qué fuentes se ejecutan en cada ciclo sin recompilar.

## Scripts SQL

En `SQL_Scripts/` están el modelo de la base operacional, el de staging y el modelo dimensional del Data Warehouse (`DWModeladoOpiniones.sql`).
