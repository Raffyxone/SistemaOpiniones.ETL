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

select * from Clientes
select * from Productos
select * from Categorias
select * from Fuentes
select * from Opiniones
select * from Cargas_ETL_Log

use sistema_opiniones_clientes

INSERT INTO Categorias (Nombre)
VALUES ('Electrónica');

INSERT INTO Clientes (Nombre, Email)
VALUES 
('Rafael Torres', 'rafael@correo.com'),
('Ana Pérez', 'ana@correo.com');

INSERT INTO Productos (Nombre, CategoriaID)
VALUES ('Smartphone X', 1);

INSERT INTO Fuentes (TipoFuente, NombreFuente)
VALUES ('Encuesta Interna', 'EncuestaInterna');

--Insertar opiniones para prueba
INSERT INTO Opiniones
(FuenteOpinionID, ClienteID, ProductoID, FuenteID, Fecha, Comentario, Clasificacion, PuntajeSatisfaccion)
VALUES
('OLTP001', 1, 1, 1, GETDATE(), N'El producto es excelente', 'Positivo', 5),
('OLTP002', 2, 1, 1, GETDATE(), N'No me gustó la calidad', 'Negativo', 2);



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

