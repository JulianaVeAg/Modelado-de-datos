# Databricks notebook source
#Cargar archivo
ruta = "/FileStore/tables/access_log_Jul95"

base_df = spark.read.text(ruta)

base_df.show(10, truncate= False)

# COMMAND ----------

#hostnames 199.72.81.55 / unicomp6.unicomp.net
host=  '^(\S+)\s'

#timestamps 01/Jul/1995:00:00:01 -0400
ts_pattern =  '(\d{2}\/\w{3}\/\d{4}\:\d{2}:\d{2}:\d{2})'

#HTTP ‘GET’, ‘/history/apollo/’, ‘HTTP/1.0’
method_uri_protocol_pattern =  '\"(\S+)\s(\S+)\s*(\S*)\s*(\S*)\"'


#HTTP status codes 200
status_pattern =  '\s(\d{3})\s'


#HTTP response content size 6245
content_size_pattern =  '\s(\d+)$'


# COMMAND ----------

from pyspark.sql.functions import regexp_extract

#Putting it all together
logs_df=base_df.select(regexp_extract('value', host, 1).alias('host'),
                         regexp_extract('value', ts_pattern, 1).alias('timestamp'),
                         regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
                         regexp_extract('value', method_uri_protocol_pattern, 2).alias('resource'),
                         regexp_extract('value', method_uri_protocol_pattern, 3).alias('protocol'),
                         regexp_extract('value', status_pattern, 1).cast('integer').alias('status'),
                         regexp_extract('value', content_size_pattern, 1).cast('integer').alias('content_size'))


logs_df.show()

# COMMAND ----------

from pyspark.sql.functions import *


#Convertir el mes de la fecha, en número
logs_df1 = logs_df.withColumn("timestamp", to_timestamp(logs_df.timestamp, "dd/MMM/yyyy:HH:mm:ss"))

logs_df1.show()

# COMMAND ----------

# ¿Cuáles son los distintos protocolos web utilizados? Agrúpalos.

logs_df1.select("protocol").distinct().show()

# COMMAND ----------

#¿Cuáles son los códigos de estado más comunes en la web? Agrúpalos y ordénalos  para ver cuál es el más común.

logs_df1.groupBy("status").count().show()

# COMMAND ----------

#¿Y los métodos de petición (verbos) más utilizados?

logs_df1.groupBy("method").count().show()


# COMMAND ----------

#¿Qué recurso tuvo la mayor transferencia de bytes de la página web?

logs_df1.groupBy("resource","content_size").sum("content_size").show()

# COMMAND ----------

#Además, queremos saber que recurso de nuestra web es el que más tráfico recibe. Es  decir, el recurso con más registros en nuestro log

logs_df1.groupBy("resource").count().orderBy(col("count").desc()).show()

# COMMAND ----------

#¿Qué días la web recibió más tráfico?

logs_df1.groupBy(dayofmonth("timestamp")).count().orderBy(col("count").desc()).show()

# COMMAND ----------

# ¿Cuáles son los hosts los más frecuentes?

logs_df1.groupBy("host").count().orderBy(col("count").desc()).show()

# COMMAND ----------

# ¿A qué horas se produce el mayor número de tráfico en la web?


logs_df1.groupBy(hour("timestamp")).count().orderBy(col("count").desc()).show()

# COMMAND ----------

# ¿Cuál es el número de errores 404 que ha habido cada día?

logs_df1.groupBy(dayofmonth(col("timestamp")),col("status")).count().show()

# COMMAND ----------

logs_df1.groupBy(dayofmonth(col("timestamp")),col("status")).count().where(col("status")==404).show()
