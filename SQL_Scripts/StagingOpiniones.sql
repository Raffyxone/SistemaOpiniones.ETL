CREATE DATABASE [StagingDB] 
GO 
USE [StagingDB]
GO 
DELETE FROM OpinionesStaging;

CREATE TABLE [dbo].[OpinionesStaging]( 
[OpinionFuenteId] [varchar](255) NOT NULL, 
[FuenteNombre] [varchar](100) NOT NULL,
[FechaOpinion] [datetime2](7) NOT NULL, 
[ClienteIdExterno] [varchar](255) NULL,
[ProductoIdExterno] [varchar](255) NULL, 
[Calificacion] [int] NOT NULL,
[Comentario] [nvarchar](max) NULL,
[SentimientoDetectado] 
[varchar](50) NULL,
[FechaCargaStaging] [datetime2](7) NOT NULL DEFAULT (getdate()), 
CONSTRAINT [PK_OpinionesStaging] PRIMARY KEY CLUSTERED ( [FuenteNombre] ASC, 
[OpinionFuenteId] ASC ) ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY] GO

select FuenteNombre, count(*) as Registros
from StagingDB.dbo.OpinionesStaging
group by FuenteNombre

IF TYPE_ID(N'OpinionFuenteType') IS NULL
BEGIN
    CREATE TYPE dbo.OpinionFuenteType AS TABLE (
        OpinionFuenteId NVARCHAR(255) NOT NULL,
        FuenteNombre NVARCHAR(100) NOT NULL,
        FechaOpinion DATETIME2 NOT NULL,
        ClienteIdExterno NVARCHAR(255) NULL,
        ProductoIdExterno NVARCHAR(255) NULL,
        Calificacion INT NOT NULL,
        Comentario NVARCHAR(MAX) NULL,
        SentimientoDetectado NVARCHAR(50) NULL,
        PRIMARY KEY (FuenteNombre, OpinionFuenteId)
    );
END

TRUNCATE TABLE OpinionesStaging
GO