-- Databricks notebook source
PARTE 1
1) Crear base de datos datos_padron

CREATE DATABASES datos_padron 

2)Crear tabla padron_txt

CREATE TABLE padron_txt (COD_DISTRITO INT,DESC_DISTRITO STRING,
COD_DIST_BARRIO INT, DESC_BARRIO STRING,
COD_BARRIO INT,COD_DIST_SECCION INT,COD_SECCION INT,COD_EDAD_INT INT, EspanolesHombres INT,EspanolesMujeres INT,ExtranjerosHombres INT,ExtranjerosMujeres INT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = '\073',
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
TBLPROPERTIES("skip.header.line.count"="1");


2)Cargar tabla con datos 

LOAD DATA INPATH '/user/cloudera/Rango_Edades_Seccion_202204.csv' INTO TABLE padron_txt

LOAD LOCAL DATA INPATH '/home/cloudera/Desktop/Rango_Edades_Seccion_202204.csv' INTO TABLE pdron_txt4

3) Crear tabla padron_text2 con trim

CREATE TABLE padron_txt_2 AS
SELECT TRIM (COD_DISTRITO) AS COD_DISTRITO, TRIM(DESC_DISTRITO) AS DESC_DISTRITO,
TRIM(COD_DIST_BARRIO) AS COD_DIST_BARRIO, TRIM(DESC_BARRIO) AS DESC_BARRIO, TRIM(COD_BARRIO)
AS COD_BARRIO, TRIM(COD_DIST_SECCION) AS COD_DIST_SECCION, TRIM(COD_SECCION) AS COD_SECCION, 
TRIM(COD_EDAD_INT) AS COD_EDAD_INT, TRIM(EspanolesHombres) AS EspanolesHombres, TRIM(EspanolesMujeres) AS EspanolesMujeres, 
TRIM(ExtranjerosHombres) AS ExtranjerosHombres, TRIM(ExtranjerosMujeres) AS ExtranjerosMujeres
FROM padron_txt;


4)Investigar y entender la diferencia de incluir la palabra LOCAL en el comando LOADDATA

La sentencia LOAD DATA  lee filas desde un fichero de texto a una tabla a gran velocidad. Si se especifica la palabra LOCAL, se interpreta con respecto al cliente final de la conexión. Cuando se especifica LOCAL, el fichero es leído por el programa del cliente en el ordenador cliente y se envía al servidor. Si no se especifica LOCAL, el fichero debe estar en el ordenador servidor y es leído directamente por el servidor. 

5) Tabla con 0, no nulos

CREATE TABLE patron_txt2 AS
SELECT COD_DISTRITO,DESC_DISTRITO, COD_DIST_BARRIO,DESC_BARRIO, COD_BARRIO, COD_DIST_SECCION, COD_SECCION,COD_EDAD_INT,
CASE
when LENGTH(EspanolesHombres) > 0 then EspanolesHombres  ELSE 0 END AS EspanolesHombres,
CASE
when LENGTH(EspanolesMujeres) > 0 then EspanolesMujeres  ELSE 0 END AS EspanolesMujeres,
CASE
when LENGTH(ExtranjerosHombres) > 0 then ExtranjerosHombres  ELSE 0 END AS ExtranjerosHombres,
CASE
when LENGTH(ExtranjerosMujeres) > 0 then ExtranjerosMujeres  ELSE 0 END AS ExtranjerosMujeres
from padron_txt2



6)Expresión regular


a)\"(\d+)\"(\;)\"(\w+)(\s++)\"(\;)\"(\d+)\"(\;)\"(\w+)(\s++)\"(\;)\"(\d+)\"(\;)\"(\d+)\"(\;)\"(\d+)\"(\;)\"(\d+)\"(\;)\"(\d+)\"(\;)\"(\d+)\"(\;)\"(\d+)\"(\;)\"

"1";"CENTRO              ";"101";"PALACIO            ";"1";"1001";"1";"0";"3";"1";"1";"0" 

b)(\d+)(\;)(\w+)(\s++)(\;)(\d+)(\;)(\w+)(\;)(\d+)(\;)(\d+)(\;)(\d+)(\;)(\d+)(\;)(\d+)(\;)(\d+)(\;)(\d+)(\;)(\d+)(\;)

1;CENTRO         ;101;PALACIO;1;1001;1;0;3;1;1;0;

c)/[^\"]/gm

"1";"CENTRO              ";"101";"PALACIO            ";"1";"1001";"1";"0";"3";"1";"1";""  Coge todo lo q no sea comillas

d)/[A-Z0-9\;\s++]/gm

"1";"CENTRO              ";"101";"PALACIO            ";"1";"1001";"1";"0";"3";"1";"1";""




CREATE TABLE padron_txt4 (COD_DISTRITO INT,DESC_DISTRITO STRING,
COD_DIST_BARRIO INT, DESC_BARRIO STRING,
COD_BARRIO INT,COD_DIST_SECCION INT,COD_SECCION INT,COD_EDAD_INT INT, EspanolesHombres INT,EspanolesMujeres INT,ExtranjerosHombres INT,ExtranjerosMujeres INT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe' WITH SERDEPROPERTIES ("input.regex"='"(.*)"\073"([A-Za-z]*) *"\073"(.*)"\073"([A-Za-z]*) *"\073"(.*)"\073"(.*?)"\073"(.*?)"\073"(.*?)"\073"(.*?)"(.*?)"\073"(.*?)"\073"(.*?)"') 
STORED AS TEXTFILE TBLPROPERTIES ("skip.header.line.count"="1");



-- COMMAND ----------

PARTE 2

1) CTAS es una operación paralela que crea una nueva tabla basada en la salida de una instrucción SELECT. CTAS es la forma más sencilla y rápida de crear e insertar datos en una tabla con un solo comando.
CTAS puede especificar tanto la distribución de los datos de la tabla como el tipo de estructura de la misma. Uno de los usos más comunes de CTAS es crear la copia de una tabla para cambiar el DDL.

CREATE TABLE FactInternetSales_new
WITH
(
    CLUSTERED COLUMNSTORE INDEX,
    DISTRIBUTION = HASH(ProductKey),
    PARTITION
    (
        OrderDateKey RANGE RIGHT FOR VALUES
        (
        20000101,20010101,20020101,20030101,20040101,20050101,20060101,20070101,20080101,20090101,
        20100101,20110101,20120101,20130101,20140101,20150101,20160101,20170101,20180101,20190101,
        20200101,20210101,20220101,20230101,20240101,20250101,20260101,20270101,20280101,20290101
        )
    )
)
AS SELECT * FROM FactInternetSales;

2)PARQUET
Puede usar una consulta CTAS para crear una nueva tabla en formato Parquet a partir de una tabla de origen en un formato de almacenamiento diferente.

Utilice la formatpropiedad para especificar ORC, AVRO, JSONo TEXTFILEcomo formato de almacenamiento para la nueva tabla

CREATE TABLE padron_parquet 
    row format delimited 
    STORED AS Parquet 
AS select * from padron_txt2



3) PARQUET 2

CREATE TABLE padron_parquet_2 
    row format delimited 
    STORED AS Parquet 
AS select * from padron_txt3

2


5) FORMATO PARQUET VENTAJAS

Parquet es el formato de almacenamiento en columnas principal en el ecosistema Hadoop. Fue desarrollado por primera vez por Twitter y Cloudera en cooperación. En mayo de 2015, se graduó de la incubadora Apache y se convirtió en un proyecto Apache de alto nivel. Parquet es un formato de almacenamiento columnar que admite estructuras anidadas. Muy adecuado para escenarios OLAP, almacenamiento de columnas y escaneo.


Ventajas ppales:

El almacenamiento de columnas facilita el uso de una codificación y compresión eficientes para cada columna, lo que reduce el espacio en disco. 

Utilice la inserción de mapas y la inserción de predicados para leer solo las columnas requeridas y omitir las columnas que no cumplan con las condiciones, lo que puede reducir el escaneo de datos innecesario, traer mejoras de rendimiento y volverse más obvias cuando hay más campos de tabla.


6) SIZE TXT VS PARQIUET

PARQUET fichero padron_parquet-> sin espacios : 212.2 KB. tabla -> totalSize 	1.35 MB

PARQUET fichero padron_parquet2 -> con espacios : 1.4KB tabla -> totalSize 	212.17 KB

TXT con espacios : fichero -> 31.0MB tabla ->totalSize 	31.05 MB
 (padron_txt_3)
TXT sin espacios : fichero -> 8.05MB tbala->totalSize 	8.05 MB
padron_txt2


-- COMMAND ----------

PARTE 3 
1) IMPALA 

Apache Impala es una herramienta escalable de procesamiento MPP (Massively Parallel Processing). Tiene licencia open source. Fue desarrollada inicialmente por Cloudera y más tarde incluida en la Apache Software Foundation. Está incluida en las distribuciones de Cloudera.

Es muy popular para realizar consultas SQL interactivas con muy baja latencia. Además, soporta múltiples formatos como Parquet, ORC, JSON o Avro y tecnologías de almacenamiento como HDFS, Kudu, Hive, HBase, Amazon S3 o ADLS.

Impala usa los mismos metadatos, la misma sintaxis SQL y el mismo driver que Hive. Además, también se puede usar desde la interfaz de Hue, por lo que se integra perfectamente con el ecosistema de Hadoop.

Impala destaca cuando necesitamos una tecnología que nos proporcione una baja latencia en consultas exploratorias y de descubrimiento de datos. Así, podemos conseguir respuestas en tiempos menores de un segundo.

Admite seguridad Hadoop (autenticación Kerberos) .
Utiliza metadatos, controlador ODBC y sintaxis SQL de Apache Hive .
Es compatible con múltiples códecs de compresión:
(a) Snappy (Recomendado por su equilibrio efectivo entre la relación de compresión y la velocidad de descompresión),
(b) Gzip (recomendado cuando se alcanza el nivel más alto de compresión),

(c) Deflate (no compatible con archivos de texto), Bzip2, LZO (solo para archivos de texto);



2) IMPALA vs HIVE

* La velocidad de procesamiento de consultas en Hive es lenta, pero Impala es 6-69 veces más rápido que Hive .

* En Hive, la latencia es alta, pero en Impala, la latencia es baja .

* Hive admite el almacenamiento de archivos RC y ORC, pero el almacenamiento de Impala admite Hadoop y Apache HBase .

* Hive genera una expresión de consulta en tiempo de compilación, pero en la generación de código Impala para '' bucles grandes '' ocurre durante el tiempo de ejecución .

* Hive no admite el procesamiento en paralelo, pero Impala admite el procesamiento en paralelo.

*Hive admite MapReduce pero Impala no es compatible con MapReduce .

*En Hive, no hay una característica de seguridad, pero Impala admite la autenticación Kerberos .

* En una actualización de cualquier proyecto donde la compatibilidad y la velocidad son importantes, Hive es una opción ideal, pero para un nuevo proyecto, Impala es la opción ideal .

* Hive es tolerante a fallas, pero Impala no admite tolerancia a fallas .

* Hive admite tipos complejos, pero Impala no admite tipos complejos .

* Hive es Hadoop MapReduce basado en lotes, pero Impala es una base de datos MPP .

* Hive no es compatible con la informática interactiva, pero Impala es compatible con la informática interactiva .

* La consulta Hive tiene un problema de "arranque en frío", pero en el proceso de Impala daemon se inicia en el momento del arranque .

*El administrador de recursos de Hive es YARN (Yet Another Resource Negotiator) pero en Impala el administrador de recursos es nativo * YARN .

* Las distribuciones de Hive son todas de distribución de Hadoop, Hortonworks (Tez, LLAP) pero en la distribución de Impala están Cloudera MapR (* Amazon EMR) .

* El público de la colmena son ingenieros de datos, pero en el público de Impala son analistas de datos / científicos de datos.

* El rendimiento de la colmena es alto, pero en Impala el rendimiento es bajo .


3)INVALIDATE METADATA

La INVALIDATE METADATAdeclaración marca los metadatos de una o todas las tablas como obsoletas. La próxima vez que el servicio de Impala realice una consulta en una tabla cuyos metadatos estén invalidados, Impala recargará los metadatos asociados antes de continuar con la consulta. Como esta es una operación muy costosa en comparación con la actualización incremental de metadatos realizada por la REFRESHdeclaración, cuando sea posible, prefiera REFRESHen lugar de INVALIDATE METADATA.

INVALIDATE METADATAes necesario cuando se realizan los siguientes cambios fuera de Impala, en Hive y en otro cliente de Hive, como SparkSQL:
Los metadatos de las tablas existentes cambian.
Se agregan nuevas tablas e Impala usará las tablas.
Los privilegios de nivel SERVERo DATABASEse cambian desde fuera de Impala.
Bloquee los cambios de metadatos, pero los archivos siguen siendo los mismos (reequilibrio de HDFS).
Los frascos UDF cambian.
Algunas tablas ya no se consultan y desea eliminar sus metadatos del catálogo y las memorias caché del coordinador para reducir los requisitos de memoria.
No INVALIDATE METADATAes necesario cuando los cambios los realiza impalad.

Una vez emitido, el INVALIDATE METADATAestado de cuenta no se puede cancelar.

Sintaxis:

INVALIDATE METADATA [[db_name.]table_name]
Si no se especifica ninguna tabla, los metadatos almacenados en caché para todas las tablas se vacían y sincronizan con Hive Metastore (HMS). Si las tablas se eliminaron del HMS, se eliminarán del catálogo y, si se agregaron nuevas tablas, aparecerán en el catálogo.

Si especifica un nombre de tabla, solo los metadatos de esa tabla se vacían y sincronizan con el HMS.

Notas de uso:

Para devolver resultados de consulta precisos, Impala debe mantener los metadatos actualizados para las bases de datos y las tablas consultadas. Por lo tanto, si alguna otra entidad modifica la información utilizada por Impala en el metastore, la información almacenada en caché por Impala debe actualizarse a través INVALIDATE METADATAde o REFRESH.

INVALIDATE METADATAes una operación asíncrona que simplemente descarta los metadatos cargados de las cachés del catálogo y del coordinador. Después de esa operación, el catálogo y todos los coordinadores de Impala solo saben de la existencia de bases de datos y tablas y nada más. La carga de metadatos para las tablas se desencadena por cualquier consulta posterior.


Ejemplos:

Este ejemplo ilustra la creación de una nueva base de datos y una nueva tabla en Hive, y luego realiza una INVALIDATE METADATAdeclaración en Impala utilizando el nombre de tabla completo, después de lo cual Impala puede ver tanto la nueva tabla como la nueva base de datos.

Antes de INVALIDATE METADATAque se emitiera la declaración, Impala daría un error de no encontrado si intentara hacer referencia a esos nombres de bases de datos o tablas.

$ hive
hive> CREATE DATABASE new_db_from_hive;
hive> CREATE TABLE new_db_from_hive.new_table_from_hive (x INT);
hive> quit;

$ impala-shell
> REFRESH new_db_from_hive.new_table_from_hive;
ERROR: AnalysisException: Database does not exist: new_db_from_hive

> INVALIDATE METADATA new_db_from_hive.new_table_from_hive;

> SHOW DATABASES LIKE 'new*';
+--------------------+
| new_db_from_hive   |
+--------------------+

> SHOW TABLES IN new_db_from_hive;
+---------------------+
| new_table_from_hive |
+---------------------+

 
 De forma predeterminada, el INVALIDATE METADATAcomando verifica los permisos HDFS de los archivos y directorios de datos subyacentes, almacenando en caché esta información para que una declaración pueda cancelarse inmediatamente si, por ejemplo, el impalausuario no tiene permiso para escribir en el directorio de datos de la tabla.
 
 

4) INVALIDE METADATA A LA BBD

INVALIDATE METADATA datos_padron.padron_txt4

Se borran todas las columnas de la tabla. 

5)STC TOTAL

SELECT desc_distrito,desc_barrio , sum(EspanolesHombres) AS EspanolesHombres,sum(EspanolesMujeres) AS EspanolesMujeres,sum(ExtranjerosHombres)
AS ExtranjerosHombres,sum(ExtranjerosMujeres) AS ExtranjerosMujeres
FROM patron_txt5 GROUP BY desc_distrito,desc_barrio


SELECT desc_distrito,desc_barrio , sum(cast(EspanolesHombres AS INT))AS EspanolesHombres,sum(CAST(EspanolesMujeres AS INT))AS EspanolesMujeres,
sum(CAST(ExtranjerosHombres AS INT)) AS ExtranjerosHombres,
sum(CAST(ExtranjerosMujeres AS INT))AS ExtranjerosHombres
FROM padron_parquet2 GROUP BY desc_distrito,desc_barrio



6)HIVE
padron_txt 3 : Sin espacios y 0 en valores vacios.  descr barrio y desc distrito null se argupan

padron_parquet2: igual que al anterior pero arda bastante mas 


7) Misma consulta en Impala

Va mucho más rapido en Impala que en Hive 


8) Rendimiento HIve e IMpala

 patron_txt3

H ->tarda 14.10s 

I-> ha tardado 0.48 s 


padron_parquet2

H-> 24.22 seg

I-> 1.29 seg






-- COMMAND ----------

PARTE 4

1) tabla particionada

CREATE TABLE padron_particionado (
COD_DISTRITO string,
COD_DIST_BARRIO string,
COD_BARRIO string,
COD_DIST_SECCION string,
COD_SECCION string,
EspanolesHombres string,
EspanolesMujeres string,
ExtranjerosHombres string,
ExtranjerosMujeres string)
PARTITIONED BY (desc_distrito string,
desc_barrio string) STORED AS PARQUET;


2) insertar datos 

SET hive.exec.dynamic.partition=true;
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=1000;
SET hive.exec.dynamic.partition.mode=nostrict;
SET hive.exec.max.created.files = 10000;

SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=1000;

insert overwrite table padron_particionado
 PARTITION(desc_distrito, desc_barrio)
    SELECT 
    COD_DISTRITO,
    COD_DIST_BARRIO, 
    COD_BARRIO,
    COD_DIST_SECCION,
    COD_SECCION,
    EspanolesHombres,
    EspanolesMujeres,
    ExtranjerosHombres,
    ExtranjerosMujeres,
    desc_distrito, desc_barrio
    FROM padron_parquet2;



3) INVALIDatE METADATA


invalide metadata datos_padron.padron_particionado

4) stc total
SELECT desc_distrito,desc_barrio , sum(cast(EspanolesHombres AS INT))AS EspanolesHombres,sum(CAST(EspanolesMujeres AS INT))AS EspanolesMujeres,
   sum(CAST(ExtranjerosHombres AS INT)) AS ExtranjerosHombres,
  sum(CAST(ExtranjerosMujeres AS INT))AS ExtranjerosHombres
  FROM padron_parquet2 WHERE desc_distrito = "CENTRO" OR desc_distrito="LATINA" OR desc_distrito="CHAMARTIN" OR desc_distrito="TETUAN" OR desc_distrito="VICALVARO" OR desc_distrito="BARAJAS" GROUP BY desc_distrito,desc_barrio;
  
  
  Time taken: 18.072 seconds, Fetched: 34 row(s)

5) STC

SELECT desc_distrito,desc_barrio , sum(cast(EspanolesHombres AS INT))AS EspanolesHombres,sum(CAST(EspanolesMujeres AS INT))AS EspanolesMujeres,
   sum(CAST(ExtranjerosHombres AS INT)) AS ExtranjerosHombres,
  sum(CAST(ExtranjerosMujeres AS INT))AS ExtranjerosHombres
  FROM padron_parquet2 WHERE desc_distrito = "CENTRO" OR desc_distrito="LATINA" OR desc_distrito="CHAMARTIN" OR desc_distrito="TETUAN" OR desc_distrito="VICALVARO" OR desc_distrito="BARAJAS" GROUP BY desc_distrito,desc_barrio;
  
 Time taken: 26.792 seconds 34 rows


SELECT desc_distrito,desc_barrio , sum(cast(EspanolesHombres AS INT))AS EspanolesHombres,sum(CAST(EspanolesMujeres AS INT))AS EspanolesMujeres,
   sum(CAST(ExtranjerosHombres AS INT)) AS ExtranjerosHombres,
  sum(CAST(ExtranjerosMujeres AS INT))AS ExtranjerosHombres
  FROM padron_particionado WHERE desc_distrito = "CENTRO" OR desc_distrito="LATINA" OR desc_distrito="CHAMARTIN" OR desc_distrito="TETUAN" OR desc_distrito="VICALVARO" OR desc_distrito="BARAJAS" GROUP BY desc_distrito,desc_barrio;
  
  Time taken: 17.857 seconds, Fetched: 15 row(s)

6)STC IMPALA

SELECT desc_distrito,desc_barrio , sum(cast(EspanolesHombres AS INT))AS EspanolesHombres,sum(CAST(EspanolesMujeres AS INT))AS EspanolesMujeres,
   sum(CAST(ExtranjerosHombres AS INT)) AS ExtranjerosHombres,
  sum(CAST(ExtranjerosMujeres AS INT))AS ExtranjerosHombres
  FROM padron_parquet WHERE desc_distrito = "CENTRO" OR desc_distrito="LATINA" OR desc_distrito="CHAMARTIN" OR desc_distrito="TETUAN" OR desc_distrito="VICALVARO" OR desc_distrito="BARAJAS" GROUP BY desc_distrito,desc_barrio;
  
  34 rows , 
4.78s
  
  SELECT desc_distrito,desc_barrio , sum(cast(EspanolesHombres AS INT))AS EspanolesHombres,sum(CAST(EspanolesMujeres AS INT))AS EspanolesMujeres,
   sum(CAST(ExtranjerosHombres AS INT)) AS ExtranjerosHombres,
  sum(CAST(ExtranjerosMujeres AS INT))AS ExtranjerosHombres
  FROM padron_particionado 
  WHERE desc_distrito = "CENTRO" OR desc_distrito="LATINA" 
  OR desc_distrito="CHAMARTIN" 
  OR desc_distrito="TETUAN" OR desc_distrito="VICALVARO" 
  OR desc_distrito="BARAJAS" 
  GROUP BY desc_distrito,desc_barrio;
  
  2,14 seg
  
  7)stc avg,max,min,sum, count
  
  SELECT desc_distrito,
desc_barrio,
avg(CAST(EspanolesHombres AS INT)) AS media,
max(CAST(EspanolesHombres AS INT)) AS maximo,
min(CAST(EspanolesHombres AS INT)) AS minimo,
count(CAST(EspanolesHombres AS INT)) AS cantidad
FROM padron_particionado
WHERE desc_distrito IN ('CENTRO',
'LATINA',
'CHAMARTIN',
'TETUAN',
'VICALVARO',
'BARAJAS')
GROUP BY desc_distrito,
desc_barrio;


hive-> 18,34 seg 
IMpala-> 14 rows ,4048 seg

 SELECT desc_distrito,
desc_barrio,
avg(CAST(EspanolesHombres AS INT)) AS media,
max(CAST(EspanolesHombres AS INT)) AS maximo,
min(CAST(EspanolesHombres AS INT)) AS minimo,
count(CAST(EspanolesHombres AS INT)) AS cantidad
FROM patron_txt2
WHERE desc_distrito IN ('CENTRO',
'LATINA',
'CHAMARTIN',
'TETUAN',
'VICALVARO',
'BARAJAS')
GROUP BY desc_distrito,
desc_barrio;
  
  
  -----
  
   SELECT desc_distrito,
desc_barrio,
avg(CAST(EspanolesHombres AS INT)) AS media,
max(CAST(EspanolesHombres AS INT)) AS maximo,
min(CAST(EspanolesHombres AS INT)) AS minimo,
count(CAST(EspanolesHombres AS INT)) AS cantidad
FROM padron_parquet2
WHERE desc_distrito IN ('CENTRO',
'LATINA',
'CHAMARTIN',
'TETUAN',
'VICALVARO',
'BARAJAS')
GROUP BY desc_distrito,
desc_barrio;


H->24.8 34 rows
I->2.61 s 34 rows





-- COMMAND ----------

PARTE 5

1) y 2) Crear ficheros. 
datos1, datos2.

3) Crear directorio:

hdfs dfs -mkdir /test


4) Pasar ficheros al directorio nuevo:


hdfs dfs -put /home/cloudera/Desktop/datos1 /test

5) Crear  bbdd y tabla.

create table numeros_tbl ( numero1 int, numero2 int, numero3 int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

6)Cargar los datos. 

load data inpath '/test/datos1' into table numeros_tbl;

Al cargar los datos, el archivo con los datos ya no está en la carpeta test.  Se ha movido a /user/hive/warehouse/numeros/numeros_tbl,

Al borrar la tabla, el archivo también desparece.

7)Volver a mover fichero datos a hdfs:
hdfs dfs -put /home/cloudera/Desktop/datos1 /test

8)crear tabla de nuevo y cargar los datos en ella:

create external  table numeros_tbl ( numero1 int, numero2 int, numero3 int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
load data inpath '/test/datos1' into table numeros_tbl;
*Al cargar los datos el fichero vuelve a moverse a la ruta /user/hive/warehouse/numeros y esta vez al borrar la tabla, no se borra el archivo.

9)eliminar fichero: 
- borrar fichero datos1 del directorio donde esté : hdfs dfs -rm /user/hive/warehouse/numeros.db/numeros_tbl/datos1
-cargar en test: hdfs dfs -put /home/cloudera/Desktop/datos1 /test
- crear tabla: CREATE EXTERNAL TABLE numeros_tbl (col1 INT, col2 INT, col3 INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION "/test";
-SELECT * FROM numeros_tbl; la tabla está cargada.

10)
-Insertar datos2: hdfs dfs -put /home/cloudera/Desktop/datos2 /test
Al meter ese fichero datos2 en el directorio del fichero datos1, a donde se dirige la ruta de location al crear la tabla, con más datos, la tabla muestra los datos de los dos ficheros. 

11) Conclusiones: 

Al crear la tabla externa y ponerle como location una ruta de un fichero, todos los archivos que añadimos a ese fichero, se cargan en la base de datos y así la podemos mantener actualizada. 

