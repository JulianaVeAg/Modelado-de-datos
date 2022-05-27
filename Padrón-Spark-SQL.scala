// Databricks notebook source
//PARTE 7. SQL
//Crear Base de datos "datos_padron"

val db= spark.sql("create database datos_padron")

// COMMAND ----------

//Crear la tabla de datos padron_txt con todos los campos del fichero CSV y cargar los datos mediante el comando LOAD DATA LOCAL INPATH. La tabla tendrá formato texto y tendrá como delimitador de campo el caracter ';' y los campos que en el documento original están encerrados en comillas dobles '"' no deben estar envueltos en estos caracteres en la tabla de Hive (es importante indicar esto utilizando el serde de OpenCSV, si no la importación de las variables que hemos indicado como numéricas fracasará ya que al estar envueltos en comillas los toma como strings) y se deberá omitir la cabecera del fichero de datos al crear la tabla.


spark.sql("use datos_padron")

spark.sql(""" CREATE TABLE IF NOT EXISTS padron_txt
USING CSV OPTIONS (
header="true",
delimiter=";",
inferSchema="true",
path="/FileStore/tables/padron/Rango_Edades_Seccion_202204.csv")""")



// COMMAND ----------

spark.sql("select * from padron_txt").show()

// COMMAND ----------

//Quitar espacios innecesarios
spark.sql("""create table padron_txtSE as
select cod_distrito as cod_distrito, trim(desc_distrito) as desc_distrito, cod_dist_barrio as cod_dist_barrio, trim(desc_barrio) as desc_barrio, cod_barrio as cod_barrio, cod_dist_seccion as cod_dist_seccion, cod_seccion as cod_seccion, cod_edad_int as cod_edad_int, espanoleshombres as espanoleshombres, espanolesmujeres as espanolesmujeres, extranjeroshombres as extranjeroshombres, extranjerosmujeres as extranjerosmujeres from padron_txt""")

// COMMAND ----------

//Comprobamos que se hayan realizado los cambios
spark.sql("select * from padron_txtSE").show()

// COMMAND ----------

//Tenemos que quitar los valores nulos de la tabla y sustiuirlos por un 0..

//Primero comprobamos la longitud de los valores de las ultimas columnas
spark.sql("""select length(espanoleshombres), length(espanolesmujeres), length(extranjeroshombres), length(extranjerosmujeres) from padron_txtSE limit 20;""").show()

//

// COMMAND ----------

spark.sql("drop table  if exists padron_txt1")

// COMMAND ----------

spark.sql("""CREATE TABLE padron_txt1 AS
SELECT COD_DISTRITO,DESC_DISTRITO, COD_DIST_BARRIO,DESC_BARRIO, COD_BARRIO, COD_DIST_SECCION, COD_SECCION,COD_EDAD_INT,
CASE
when LENGTH(EspanolesHombres) > 0 then EspanolesHombres  ELSE 0 END AS EspanolesHombres,
CASE
when LENGTH(EspanolesMujeres) > 0 then EspanolesMujeres  ELSE 0 END AS EspanolesMujeres,
CASE
when LENGTH(ExtranjerosHombres) > 0 then ExtranjerosHombres  ELSE 0 END AS ExtranjerosHombres,
CASE
when LENGTH(ExtranjerosMujeres) > 0 then ExtranjerosMujeres  ELSE 0 END AS ExtranjerosMujeres
from padron_txtSE""")

// COMMAND ----------

//Comprobamos que se hayan realizado los cambios
spark.sql("select * from padron_txt1").show()

// COMMAND ----------

//Puede usar una consulta CTAS para crear una nueva tabla en formato Parquet a partir de una tabla de origen en un formato de almacenamiento diferente.

spark.sql("CREATE TABLE padron_parquet_2 STORED AS Parquet AS select * from padron_txt1")
