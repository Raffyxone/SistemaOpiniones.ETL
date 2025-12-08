USE [Sistema_Opiniones_DW]
GO
/****** Object:  Schema [Dimension]    Script Date: 24/10/2025 4:56:55 p. m. ******/
CREATE SCHEMA [Dimension]
GO
/****** Object:  Schema [Fact]    Script Date: 24/10/2025 4:56:55 p. m. ******/
CREATE SCHEMA [Fact]
GO
/****** Object:  Table [Dimension].[DimCustomer]    Script Date: 24/10/2025 4:56:55 p. m. ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [Dimension].[DimCustomer](
	[CustomerKey] [int] IDENTITY(1,1) NOT NULL,
	[CustomerID] [varchar](255) NULL,
	[FullName] [varchar](200) NULL,
	[Email] [varchar](150) NULL,    
	[Country] [varchar](90) NULL,
	[City] [varchar](90) NULL,
	[Age] [int] NULL,
	[Segment] [varchar](50) NULL,
	[DateFrom] [datetime2](7) NULL DEFAULT GETDATE(),
	[DateTo] [datetime2](7) NULL,
	[IsCurrentRecord] [bit] NOT NULL DEFAULT 1,
 CONSTRAINT [PK_DimCustomer] PRIMARY KEY CLUSTERED 
(
	[CustomerKey] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [Dimension].[DimDate]    Script Date: 24/10/2025 4:56:55 p. m. ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [Dimension].[DimDate](
	[DateKey] [int] NOT NULL,
	[Date] [date] NOT NULL,
	[Year] [int] NOT NULL,
	[Quarter] [int] NOT NULL,
	[Month] [int] NOT NULL,
	[MonthName] [varchar](20) NOT NULL,
	[Day] [int] NOT NULL,
	[DayOfWeekName] [varchar](15) NOT NULL,
	[IsWeekName] [bit] NOT NULL,
 CONSTRAINT [PK_DimDate] PRIMARY KEY CLUSTERED 
(
	[DateKey] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [Dimension].[DimProduct]    Script Date: 24/10/2025 4:56:55 p. m. ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [Dimension].[DimProduct](
	[ProductKey] [int] IDENTITY(1,1) NOT NULL,
	[ProductID] [varchar](255) NULL
	[ProductName] [varchar](200) NOT NULL,
	[Brand] [varchar](100) NULL,
	[Price] [decimal](18, 2) NULL, 
	[CategoryName] [varchar](80) NOT NULL,
	[DateFrom] [datetime2](7) NULL DEFAULT GETDATE(),
	[DateTo] [datetime2](7) NULL,
	[IsCurrentRecord] [bit] NOT NULL DEFAULT 1,
 CONSTRAINT [PK_DimProduct] PRIMARY KEY CLUSTERED 
(
	[ProductKey] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [Dimension].[DimSentiment]    Script Date: 24/10/2025 4:56:55 p. m. ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [Dimension].[DimSentiment](
	[SentimentKey] [int] IDENTITY(1,1) NOT NULL,
	[SentimentCode] [varchar](10) NOT NULL,
	[SentimentName] [varchar](50) NOT NULL,
 CONSTRAINT [PK_DimSentiment] PRIMARY KEY CLUSTERED 
(
	[SentimentKey] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [Dimension].[DimSource]    Script Date: 24/10/2025 4:56:55 p. m. ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [Dimension].[DimSource](
	[SourceKey] [int] IDENTITY(1,1) NOT NULL,
	[SourceID] [int] NULL,
	[SourceType] [varchar](50) NOT NULL,
	[SourceName] [varchar](150) NOT NULL,
 CONSTRAINT [PK_DimSource] PRIMARY KEY CLUSTERED 
(
	[SourceKey] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [Fact].[FactOpinions]    Script Date: 24/10/2025 4:56:55 p. m. ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [Fact].[FactOpinions](
	[OpinionKey] [int] IDENTITY(1,1) NOT NULL,
	[SourceOpinionID] [varchar](255) NULL,
	[DateKey] [int] NOT NULL,
	[ProductKey] [int] NOT NULL,
	[CustomerKey] [int] NOT NULL,
	[SourceKey] [int] NOT NULL,
	[SentimentKey] [int] NOT NULL,
	[SatisfactionScore] [tinyint] NOT NULL,
 CONSTRAINT [PK_FactOpinions] PRIMARY KEY CLUSTERED 
(
	[OpinionKey] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
ALTER TABLE [Fact].[FactOpinions]  WITH CHECK ADD  CONSTRAINT [FK_FactOpinions_DimCustomer] FOREIGN KEY([CustomerKey])
REFERENCES [Dimension].[DimCustomer] ([CustomerKey])
GO
ALTER TABLE [Fact].[FactOpinions] CHECK CONSTRAINT [FK_FactOpinions_DimCustomer]
GO
ALTER TABLE [Fact].[FactOpinions]  WITH CHECK ADD  CONSTRAINT [FK_FactOpinions_DimDate] FOREIGN KEY([DateKey])
REFERENCES [Dimension].[DimDate] ([DateKey])
GO
ALTER TABLE [Fact].[FactOpinions] CHECK CONSTRAINT [FK_FactOpinions_DimDate]
GO
ALTER TABLE [Fact].[FactOpinions]  WITH CHECK ADD  CONSTRAINT [FK_FactOpinions_DimProduct] FOREIGN KEY([ProductKey])
REFERENCES [Dimension].[DimProduct] ([ProductKey])
GO
ALTER TABLE [Fact].[FactOpinions] CHECK CONSTRAINT [FK_FactOpinions_DimProduct]
GO
ALTER TABLE [Fact].[FactOpinions]  WITH CHECK ADD  CONSTRAINT [FK_FactOpinions_DimSentiment] FOREIGN KEY([SentimentKey])
REFERENCES [Dimension].[DimSentiment] ([SentimentKey])
GO
ALTER TABLE [Fact].[FactOpinions] CHECK CONSTRAINT [FK_FactOpinions_DimSentiment]
GO
ALTER TABLE [Fact].[FactOpinions]  WITH CHECK ADD  CONSTRAINT [FK_FactOpinions_DimSource] FOREIGN KEY([SourceKey])
REFERENCES [Dimension].[DimSource] ([SourceKey])
GO
ALTER TABLE [Fact].[FactOpinions] CHECK CONSTRAINT [FK_FactOpinions_DimSource]
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Contiene la información demográfica de nuestros clientes. Guarda un historial para saber cómo era un cliente en el momento en que dio su opinión.' , @level0type=N'SCHEMA',@level0name=N'Dimension', @level1type=N'TABLE',@level1name=N'DimCustomer'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Es nuestro calendario maestro. Nos permite analizar las opiniones por día, mes, año, trimestre o incluso si fue un fin de semana.' , @level0type=N'SCHEMA',@level0name=N'Dimension', @level1type=N'TABLE',@level1name=N'DimDate'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Define la clasificación de una opinión. Nos permite agrupar rápidamente todos los comentarios en Positivos, Negativos o Neutros.' , @level0type=N'SCHEMA',@level0name=N'Dimension', @level1type=N'TABLE',@level1name=N'DimSentiment'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Aquí catalogamos de dónde vino cada opinión. Nos dice si fue una encuesta interna, un comentario en la web o una mención en redes sociales.' , @level0type=N'SCHEMA',@level0name=N'Dimension', @level1type=N'TABLE',@level1name=N'DimSource'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Esta es la tabla más importante. Cada fila es una opinión única, conectada a todas las dimensiones para darnos el contexto completo del evento' , @level0type=N'SCHEMA',@level0name=N'Fact', @level1type=N'TABLE',@level1name=N'FactOpinions'
GO

/*Tipos de Tabla para Carga Masiva (TVP) - REQUERIDO PARA EL CÓDIGO C#*/
IF TYPE_ID(N'[Dimension].[DimCustomerType]') IS NULL
BEGIN
    CREATE TYPE [Dimension].[DimCustomerType] AS TABLE (
        CustomerID VARCHAR(255),
        FullName VARCHAR(200),
        Email VARCHAR(150)
    );
END
GO
IF TYPE_ID(N'[Dimension].[DimProductType]') IS NULL
BEGIN
    CREATE TYPE [Dimension].[DimProductType] AS TABLE (
        ProductID VARCHAR(255),
        ProductName VARCHAR(200),
        Brand VARCHAR(100),
        Price DECIMAL(18, 2),
        CategoryName VARCHAR(80)
    );
END
GO
IF TYPE_ID(N'[Dimension].[DimSourceType]') IS NULL
BEGIN
    CREATE TYPE [Dimension].[DimSourceType] AS TABLE (
        SourceName VARCHAR(150),
        SourceType VARCHAR(50)
    );
END
GO
IF TYPE_ID(N'[Dimension].[DimSentimentType]') IS NULL
BEGIN
    CREATE TYPE [Dimension].[DimSentimentType] AS TABLE (
        SentimentName VARCHAR(50),
        SentimentCode VARCHAR(10)
    );
END
GO

DELETE FROM [Fact].[FactOpinions];
GO

DELETE FROM [Dimension].[DimCustomer];
DELETE FROM [Dimension].[DimProduct];
DELETE FROM [Dimension].[DimSource];
DELETE FROM [Dimension].[DimSentiment];
GO

DBCC CHECKIDENT ('[Fact].[FactOpinions]', RESEED, 0);

DBCC CHECKIDENT ('[Dimension].[DimCustomer]', RESEED, 0);
DBCC CHECKIDENT ('[Dimension].[DimProduct]', RESEED, 0);
DBCC CHECKIDENT ('[Dimension].[DimSource]', RESEED, 0);
DBCC CHECKIDENT ('[Dimension].[DimSentiment]', RESEED, 0);
GO

--1. Verificar Clientes
SELECT CustomerKey, CustomerID, FullName, Email, IsCurrentRecord 
FROM [Dimension].[DimCustomer]

--2. Verificar Productos
SELECT ProductKey, ProductID, ProductName, Price, CategoryName 
FROM [Dimension].[DimProduct]

--3. Verificar Fuentes
SELECT * FROM [Dimension].[DimSource]

--4. Verificar Sentimientos
SELECT * FROM [Dimension].[DimSentiment]

/*pa llenar con fechas mi dimdate y así mi facttable ir sin problemas*/
SET NOCOUNT ON

DECLARE @StartDate DATE = '2024-01-01'
DECLARE @EndDate   DATE = '2030-12-31'

SET DATEFIRST 7

WHILE @StartDate <= @EndDate
BEGIN
    IF NOT EXISTS (SELECT 1 FROM Dimension.DimDate WHERE Date = @StartDate)
    BEGIN
        INSERT INTO Dimension.DimDate (
            DateKey, Date, Year, Quarter, Month, MonthName, Day, DayOfWeekName, IsWeekName
        )
        VALUES (
            CAST(FORMAT(@StartDate, 'yyyyMMdd') AS INT),
            @StartDate,
            YEAR(@StartDate),
            DATEPART(QUARTER, @StartDate),
            MONTH(@StartDate),
            DATENAME(MONTH, @StartDate),
            DAY(@StartDate),
            DATENAME(WEEKDAY, @StartDate),
            CASE WHEN DATEPART(WEEKDAY, @StartDate) IN (1, 7) THEN 1 ELSE 0 END
        )
    END

    SET @StartDate = DATEADD(DAY, 1, @StartDate)
END
PRINT 'Proceso de DimDate finalizado (se ignoraron duplicados).'
GO

SELECT 
    dp.ProductName,
    COUNT(*) as CantidadOpiniones,
    AVG(fo.SatisfactionScore) as PromedioSatisfaccion
FROM Fact.FactOpinions fo
JOIN Dimension.DimProduct dp ON fo.ProductKey = dp.ProductKey
GROUP BY dp.ProductName
ORDER BY CantidadOpiniones DESC

/*para ver la cantidad de ingresos por fuente*/
SELECT 
    ds.SourceName AS Fuente,
    COUNT(*) AS Cantidad_Registros
FROM Fact.FactOpinions fo
JOIN Dimension.DimSource ds ON fo.SourceKey = ds.SourceKey
GROUP BY ds.SourceName
ORDER BY Cantidad_Registros DESC