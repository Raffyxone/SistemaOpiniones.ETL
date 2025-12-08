create database sistema_opiniones_clientes

use sistema_opiniones_clientes

select FuenteNombre, count(*) as Registros
from StagingDB.dbo.OpinionesStaging
group by FuenteNombre


--tablas maestras
create table Categorias(
CategoriaID int primary key identity(1,1),
Nombre varchar(100) not null unique
)
create table Clientes(
ClienteID int primary key identity(1,1),
Nombre varchar(100) not null,
Email varchar(150) not null unique
)
create table Productos(
ProductoID int primary key identity(1,1),
Nombre varchar(100) not null unique,
CategoriaID int not null,
foreign key (CategoriaID) references Categorias (CategoriaID)
)
create table Fuentes(
FuenteID int primary key identity(1,1),
TipoFuente varchar(50) not null,
NombreFuente varchar(100) not null unique
)

create table Opiniones(
OpinionID int primary key identity(1,1),
FuenteOpinionID varchar(255) null, 
ClienteID int null,
ProductoID int null,
FuenteID int not null,
Fecha datetime not null,
Comentario nvarchar(max) null,
Clasificacion varchar(20) null,
PuntajeSatisfaccion tinyint null,
foreign key (ClienteID) references Clientes (ClienteID),
foreign key (ProductoID) references Productos (ProductoID),
foreign key (FuenteID) references Fuentes (FuenteID)
)

create table Cargas_ETL_Log(
CargaID int primary key identity(1,1),
FechaCarga datetime default getdate(),
RegistrosProcesados int not null,
RegistrosInsertados int not null,
ErroresEncontrados int not null,
Observaciones varchar(1000) null
)

/** 
 Script SQL para generar datos aleatorios de pruebas:
  - inserta categorías, productos y clientes ficticios.
  - genera 10,000 reseñas web con puntajes, clasificaciones y fechas aleatorias.
  - ideal para pruebas de carga, BI, o poblar un entorno de desarrollo.
*/

set nocount on

-- 1. Asegurar que existe la Fuente Correcta según el SRS ("Reseñas Web")
if not exists (select 1 from Fuentes where NombreFuente = 'ResenasWeb')
begin
    insert into Fuentes (TipoFuente, NombreFuente) 
    values ('Sitio Web', 'ResenasWeb');
end

-- Obtener el ID de la fuente correcta
declare @FuenteWebID int = (select FuenteID from Fuentes where NombreFuente = 'ResenasWeb');

-- 2. Generar Categorías Variadas (Contexto E-commerce)
insert into Categorias (Nombre) values 
('Hogar'), ('Juguetes'), ('Deportes'), ('Moda'), ('Computación');

-- 3. Generar 50 Productos Aleatorios
declare @i int = 1;
while @i <= 50
begin
    insert into Productos (Nombre, CategoriaID)
    values (
        'Producto Web ' + cast(@i as varchar), 
        floor(rand()*(6-1)+1) 
    );
    set @i = @i + 1;
end

-- 4. Generar 100 Clientes Aleatorios (Usuarios registrados en la web)
set @i = 1;
while @i <= 100
begin
    insert into Clientes (Nombre, Email)
    values (
        'Usuario Web ' + cast(@i as varchar), 
        'cliente' + cast(@i as varchar) + '@ecommerce.com'
    );
    set @i = @i + 1;
end

-- 5. Generar 10,000 Reseñas Web (La carga pesada)
set @i = 1;
declare @TotalOpiniones int = 10000;

while @i <= @TotalOpiniones
begin
    -- Variables para aleatoriedad
    declare @RandCliente int = (select top 1 ClienteID from Clientes order by newid());
    declare @RandProducto int = (select top 1 ProductoID from Productos order by newid());
    
    declare @RandClasificacion varchar(20);
    declare @RandPuntaje int;
    declare @RandFecha datetime = dateadd(day, -floor(rand()*365), getdate()); -- Último año

    -- Lógica de consistencia: Puntaje vs Clasificación
    set @RandPuntaje = floor(rand()*(6-1)+1); -- 1 a 5
    
    if @RandPuntaje >= 4 set @RandClasificacion = 'Positivo';
    else if @RandPuntaje = 3 set @RandClasificacion = 'Neutro';
    else set @RandClasificacion = 'Negativo';

    -- Insertar usando la FUENTE WEB
    insert into Opiniones (
        FuenteOpinionID, ClienteID, ProductoID, FuenteID, Fecha, 
        Comentario, Clasificacion, PuntajeSatisfaccion
    )
    values (
        'WEB_REV_' + cast(@i as varchar),
        @RandCliente,
        @RandProducto,
        @FuenteWebID,
        @RandFecha,
        'Reseña de compra verificada en el sitio web #' + cast(@i as varchar),
        @RandClasificacion,
        @RandPuntaje
    );

    set @i = @i + 1;
end

print 'Carga masiva finalizada. Se generaron 10,000 Reseñas Web.'
go

select * from Clientes
select * from Productos
select * from Categorias
select * from Fuentes
select * from Opiniones
select * from Cargas_ETL_Log

/*
delete from Opiniones;
delete from Cargas_ETL_Log;
delete from Productos;
delete from Clientes;
delete from Fuentes;
delete from Categorias;


dbcc checkident ('Opiniones', reseed, 0);
dbcc checkident ('Cargas_ETL_Log', reseed, 0);
dbcc checkident ('Productos', reseed, 0);
dbcc checkident ('Clientes', reseed, 0);
dbcc checkident ('Fuentes', reseed, 0);
dbcc checkident ('Categorias', reseed, 0);
*/