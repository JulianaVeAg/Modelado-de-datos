// Databricks notebook source
RUTA CSV:  /FileStore/tables/padron/Rango_Edades_Seccion_202204.csv

// COMMAND ----------

import org.apache.spark.sql.types._

val ruta_csv = "/FileStore/tables/padron/Rango_Edades_Seccion_202204.csv"


val schema = StructType(Array(StructField("COD_DISTRITO", IntegerType, false),
StructField("DESC_DISTRITO", StringType, false),
StructField("COD_DIST_BARRIO", IntegerType, false),
StructField("DESC_BARRIO", StringType, false),
StructField("COD_BARRIO", IntegerType, false),
StructField("COD_DIST_SECCION", IntegerType, false),
StructField("COD_SECCION", IntegerType, false),
StructField("COD_EDAD_INT", IntegerType, false),
StructField("EspanolesHombres", IntegerType, false),
StructField("EspanolesMujeres", IntegerType, false),
StructField("ExtranjerosHombres", IntegerType, false),
StructField("ExtranjerosMujeres", IntegerType, false)))


val padron_df = spark.read.format("csv").schema(schema).option("delimiter",";")
.option("header","true").option("emptyValue",0)
.csv(ruta_csv)


// COMMAND ----------

padron_df.select("*").show(10)

// COMMAND ----------

import org.apache.spark.sql.functions._


val padron_df2= padron_df.withColumn("DESC_DISTRITO",trim(col("DESC_DISTRITO")))
                         .withColumn("DESC_BARRIO",trim(col("DESC_BARRIO")))

padron_df2.show()

// COMMAND ----------

padron_df2.select("*").show(10)

// COMMAND ----------

//Enumera los barrios que sean diferentes
padron_df.select("desc_barrio").distinct.show()

// COMMAND ----------

//Crea una vista temporal de nombre "padrón" y a través de ella cuenta el número de barrios
//diferentes que hay.


padron_df.createOrReplaceTempView("padron")

val barriosDif = spark.sql("SELECT COUNT ( DISTINCT desc_barrio ) AS NbarriosDif FROM padron")
barriosDif.show()

// COMMAND ----------

//Crea una nueva columna que muestre la longitud de los campos de la columna  DESC_DISTRITO y que se llame "longitud"

import org.apache.spark.sql.functions.{col,length}

val longitud = padron_df2.withColumn("longitud",length(col("desc_distrito"))).show()


// COMMAND ----------

//Crea una nueva columna que muestre el valor 5 para cada uno de los registros de la tabla.
import org.apache.spark.sql.functions._
val numer5 = padron_df.withColumn("valor5",lit("5")).show()

// COMMAND ----------

//Borra esta columna.
padron_df.drop("valor5").show()

// COMMAND ----------

//Particiona el DataFrame por las variables DESC_DISTRITO y DESC_BARRIO.
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col

val padronDF_partic = padron_df2.repartition(col("DESC_DISTRITO"),col("DESC_BARRIO"))

padronDF_partic.rdd.getNumPartitions

//val padronDF_partic= Window.partitionBy("DESC_DISTRITO","DESC_BARRIO")



// COMMAND ----------

//Almacénalo en caché. Consulta en el puerto 4040 (UI de Spark) de tu usuario local el estado de los rdds almacenados.




padronDF_partic.cache()


/*Métodos cache()y persist():  Spark proporciona un mecanismo de optimización para almacenar el cálculo intermedio de un Spark DataFrame para que puedan reutilizarse en acciones posteriores. Cuando persiste un conjunto de datos, cada nodo almacena sus datos particionados en la memoria y los reutiliza en otras acciones en ese conjunto de datos. Y los datos persistentes de Spark en los nodos son tolerantes a fallas, lo que significa que si se pierde alguna partición de un conjunto de datos, se volverá a calcular automáticamente usando las transformaciones originales que lo crearon.

El método Spark DataFrame o Dataset cache()lo guarda de manera predeterminada en el nivel de almacenamiento ` MEMORY_AND_DISK` porque volver a calcular la representación en columnas en memoria de la tabla subyacente es costoso.



*/


// COMMAND ----------

//Lanza una consulta contra el DF resultante en la que muestre el número total de "espanoleshombres", "espanolesmujeres", extranjeroshombres" y "extranjerosmujeres" para cada barrio de cada distrito.
//Las columnas distrito y barrio deben ser las primeras en aparecer en el show. Los resultados deben estar ordenados en orden de más a menos según la columna "extranjerosmujeres" y desempatarán por la //columna  "extranjeroshombres"

import org.apache.spark.sql.functions._

padronDF_partic.groupBy(col("desc_distrito"),col("desc_barrio"))
       .agg(sum(col("EspanolesHombres")).alias("CantEspanolesH"),sum(col("EspanolesMujeres")).alias("CantEspanolesM"),sum(col("ExtranjerosHombres")).alias("CantExtranjerosH"),sum(col("ExtranjerosMujeres")).alias("CantExtranjerosM"))
.orderBy(col("desc_distrito"),col("CantExtranjerosM").desc,col("CantExtranjerosH").desc)
.show()

// COMMAND ----------

//Elimina el registro en caché.

padronDF_partic.unpersist()


// COMMAND ----------

//Crea un nuevo DataFrame a partir del original que muestre únicamente una columna con DESC_BARRIO, otra con DESC_DISTRITO y otra con el número total de "espanoleshombres"  residentes en cada distrito de //cada barrio. Únelo (con un join) con el DataFrame original a través de las columnas en común.

val DF2 = padron_df.groupBy(col("desc_distrito"),col("desc_barrio")) 
                   .agg(sum(col("espanolesHombres")).alias("TotalespanolesHombres"))
                   .orderBy(col("desc_distrito"))

DF2.show()




// COMMAND ----------

val join1 = padron_df.join(DF2,padron_df("desc_distrito") === DF2("desc_distrito") && padron_df("desc_barrio")=== DF2("desc_barrio"),"leftouter")
                      .select(padron_df("desc_distrito"),padron_df("desc_barrio"),DF2("TotalespanolesHombres")).distinct()

join1.show()

// COMMAND ----------

//Repite la función anterior utilizando funciones de ventana. (over(Window.partitionBy.....)).




 val padron_w = padron_df.withColumn("TotalEspanolesH", sum(col("espanoleshombres")).over(Window.partitionBy("DESC_DISTRITO", "DESC_BARRIO")))

padron_w.select(col("desc_distrito"),col("desc_barrio"),col("TotalEspanolesH")).distinct.show()


// COMMAND ----------

//Mediante una función Pivot muestra una tabla (que va a ser una tabla de contingencia) que contenga los valores totales ()la suma de valores) de espanolesmujeres para cada distrito y  en cada rango de edad (COD_EDAD_INT). Los distritos incluidos deben ser únicamente  CENTRO, BARAJAS y RETIRO y deben figurar como columnas.

val distritos = Seq("CENTRO", "BARAJAS", "RETIRO")

//val dfPivot= padron_df.groupBy("COD_EDAD_INT").pivot("desc_distrito",distritos).sum("EspanolesMujeres")

val pivotDF = padron_df2.groupBy("cod_edad_int")
                        .pivot("desc_distrito",distritos)
                        .agg(sum("EspanolesMujeres"))
pivotDF.show()


//dfPivot.show()

// COMMAND ----------

padron_df.select(sum("EspanolesMujeres"))..show()

// COMMAND ----------

val distritos2 = Seq("CENTRO", "BARAJAS", "RETIRO")

val padron_pivot = padron_df2
.groupBy("cod_edad_int")
.pivot("desc_distrito",distritos2)
.sum("EspanolesMujeres")
.orderBy(col("Cod_edad_int").asc)





padron_pivot.show(10)

// COMMAND ----------

//Utilizando este nuevo DF, crea 3 columnas nuevas que hagan referencia a qué porcentaje  de la suma de "espanolesmujeres" en los tres distritos para cada rango de edad representa  cada uno de los tres distritos. Debe estar redondeada a 2 decimales. Puedes imponerte la  condición extra de no apoyarte en ninguna columna auxiliar creada para el caso.


val padron_porcentaje  = padron_pivot.withColumn("%barajas",round(col("barajas")/(col("barajas")+col("centro")+col("retiro"))*100,2))
                                      .withColumn("%centro",round(col("centro")/(col("barajas")+col("CENTRO")+col("RETIRO"))*100,2))
                                      .withColumn("%retiro",round(col("retiro")/(col("BARAJAS")+col("CENTRO")+col("RETIRO"))*100,2))

display(padron_porcentaje)

// COMMAND ----------

//Guarda el archivo csv original particionado por distrito y por barrio (en ese orden) en un  directorio local. Consulta el directorio para ver la estructura de los ficheros y comprueba  que es la esperada.

padron_df.write.format("csv") .option("header","true").partitionBy("desc_distrito","desc_barrio") .save("dbfs:/FileStore/datos/")

// COMMAND ----------

//Haz el mismo guardado pero en formato parquet. Compara el peso del archivo con el resultado anterior.

padron_df.write.format("parquet")
.partitionBy("desc_distrito","desc_barrio")
.save("dbfs:/FileStore/datos/padron")
