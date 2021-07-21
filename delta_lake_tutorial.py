# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Qué es un Data Lake
# MAGIC Es un repositorio donde es posible almacenar tanto datos estructurados como datos no estructurados. Un data lake es el lugar hacia el que se ingestan datos de diversas fuentes y suele estar dividido en varias “capas” cuyos nombres varían. Pero algo como:
# MAGIC - Landing – donde se almacena el dato “crudo”.
# MAGIC - Staging – donde se almacena el dato ya más limpio, normalmente en formato columnar.
# MAGIC - Analytics – donde se almacena el dato transformado, agregado, etc.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ficheros Parquet
# MAGIC ### Ventajas
# MAGIC - Formato columnar eficiente frente a archivos con formato row como csv.
# MAGIC - Menor tamaño y por tanto menor coste de almacenamiento.
# MAGIC - Mayor rapidez y por tanto menor coste de computación.
# MAGIC 
# MAGIC ### Desventajas
# MAGIC - No soporta transacciones ACID.
# MAGIC - No soporta Time Travel (versionado de datos).
# MAGIC - No soporta updates.
# MAGIC - No tiene un historial de los cambios realizados.
# MAGIC 
# MAGIC ### Formato de compresión Snappy
# MAGIC Es un formato de compresión cuyo objetivo no es el de comprimir al máximo, sino el de aportar muy altas velocidades con una compresión razonable.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Delta Table
# MAGIC ### 3.1 Crear Delta Table (SQL)
# MAGIC #### Importamos algunas librerías, borramos el warehouse y eliminamos las tablas si existen

# COMMAND ----------

from delta.tables import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.fs.rm("/user/hive/warehouse", True)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tabla1;
# MAGIC DROP TABLE IF EXISTS tabla2;
# MAGIC DROP TABLE IF EXISTS tabla3;
# MAGIC DROP TABLE IF EXISTS tabla1_parquet;
# MAGIC DROP TABLE IF EXISTS vuelos;
# MAGIC DROP TABLE IF EXISTS vuelos_copia;
# MAGIC DROP TABLE IF EXISTS example;
# MAGIC DROP TABLE IF EXISTS music;
# MAGIC DROP TABLE IF EXISTS music_new;
# MAGIC DROP TABLE IF EXISTS muebles1;
# MAGIC DROP TABLE IF EXISTS muebles2;
# MAGIC DROP TABLE IF EXISTS perro;
# MAGIC DROP TABLE IF EXISTS perro3;
# MAGIC DROP TABLE IF EXISTS perro_shallow;
# MAGIC DROP TABLE IF EXISTS profesores;
# MAGIC DROP TABLE IF EXISTS profesores_sc;
# MAGIC DROP TABLE IF EXISTS profesores_dc;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creamos una tabla delta con dos columnas, nombre y edad, e insertamos 3 registros diferentes

# COMMAND ----------

spark.sql("CREATE OR REPLACE TABLE tabla1(nombre STRING, edad STRING) USING DELTA")
spark.sql('INSERT INTO tabla1 VALUES("Pepe", "dieciséis"), ("Juan", "veinticinco"), ("Antonia", "ochenta y cinco"), ("Margarita", "veintiuno")')
spark.sql("SELECT * FROM tabla1").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creamos otra tabla delta con dos columnas, nombre y edad, e insertamos 3 registros. Dos de ellos diferentes a los de la tabla1. En el caso del tercer registro, tenemos que actualizar la edad de Antonia, pues era un error

# COMMAND ----------

spark.sql("CREATE OR REPLACE TABLE tabla2(nombre STRING, edad STRING) USING DELTA")
spark.sql('INSERT INTO tabla2 VALUES("Juanita", "quince"), ("Alfredo", "treinta y dos"), ("Antonia", "setenta y cinco")')
spark.sql("SELECT * FROM tabla2").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Ver forma de almacenamiento y delta_log
# MAGIC #### Observa los archivos que se han creado. ¿Cuántos hay? ¿Qué hay en el directorio _delta_log?

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/user/hive/warehouse/tabla1

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/user/hive/warehouse/tabla1/_delta_log

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Convertir tabla Parquet a Delta con SQL
# MAGIC #### Vamos a crear una TempView llamada vuelos_temp a partir de un csv

# COMMAND ----------

dfVuelos = spark.read.format("csv").option("header", True).load("dbfs:/FileStore/tables/flights.csv")
dfVuelos.createOrReplaceTempView("vuelos_temp")

# COMMAND ----------

# MAGIC %md
# MAGIC #### A continuación, vamos a guardar la tabla en formato parquet a partir de vuelos_temp

# COMMAND ----------

spark.sql("CREATE OR REPLACE TABLE vuelos STORED AS PARQUET AS SELECT * FROM vuelos_temp")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Finalmente, convertimos la tabla a delta

# COMMAND ----------

spark.sql("CONVERT TO DELTA vuelos")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Guardar los datos
# MAGIC #### SQL – INSERT INTO SELECT…
# MAGIC ##### De forma similar a CREATE TABLE AS SELECT, podemos usar INSERT INTO SELECT, aunque debemos crear una tabla con el esquema apropiado antes

# COMMAND ----------

spark.sql("CREATE OR REPLACE TABLE vuelos_copia(Year STRING, Month STRING, DayofMonth STRING, DayOfWeek STRING, DepTime STRING, CRSDepTime STRING, ArrTime STRING, CRSArrTime STRING,UniqueCarrier STRING,FlightNum STRING,TailNum STRING,ActualElapsedTime STRING,CRSElapsedTime STRING,AirTime STRING,ArrDelay STRING,DepDelay STRING,Origin STRING,Dest STRING,Distance STRING,TaxiIn STRING,TaxiOut STRING,Cancelled STRING,CancellationCode STRING,Diverted STRING,CarrierDelay STRING, WeatherDelay STRING,NASDelay STRING,SecurityDelay STRING,LateAircraftDelay STRING)")

spark.sql("INSERT INTO vuelos_copia SELECT * FROM vuelos_temp")

spark.sql("SELECT * FROM vuelos_copia").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Usando Spark DataFrame/Dataset API – saveAsTable
# MAGIC ##### Otra forma de guardar una tabla es usando el método saveAsTable(). Guardamos una tabla a partir del dfVuelos

# COMMAND ----------

dfVuelos.write.format("delta").mode("overwrite").saveAsTable("vuelos")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Usando Spark DataFrame/Dataset API – save
# MAGIC ##### Del mismo modo, podemos usar el método save(). Guardamos una tabla a partir de dfVuelos, en este caso con el modo ‘overwrite’

# COMMAND ----------

dfVuelos.write.format("delta").mode("overwrite").save("/user/hive/warehouse/vuelos")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Comparemos la forma de guardar una tabla en formato delta con guardar una tabla en formato parquet

# COMMAND ----------

#dfVuelos.write.format("parquet").mode("overwrite").save("/user/hive/warehouse/vuelos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Ejercicios
# MAGIC ### 4.1 Append Data
# MAGIC #### Vamos a añadir a nuestra tabla1 un nuevo registro. Pero antes vamos a ver cuántos archivos hay en la tabla de nuestro warehouse y en el directorio _delta_log

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/user/hive/warehouse/tabla1

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/user/hive/warehouse/tabla1/_delta_log

# COMMAND ----------

# MAGIC %md
# MAGIC #### Usamos DESCRIBE HISTORY para ver cuál es la versión actual de la tabla1

# COMMAND ----------

spark.sql("DESCRIBE HISTORY tabla1").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Introducimos el nuevo registro

# COMMAND ----------

spark.sql("INSERT INTO tabla1 VALUES('Federica', 'cuarenta y dos')")

# COMMAND ----------

# MAGIC %md
# MAGIC #### ¿Ha cambiado algo en los archivos que se encuentran en el warehouse? ¿Y en el directorio _delta_log?

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/user/hive/warehouse/tabla1

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/user/hive/warehouse/tabla1/_delta_log

# COMMAND ----------

# MAGIC %md
# MAGIC #### ¿Cuál es ahora la versión actual de la tabla1? Compruébalo

# COMMAND ----------

spark.sql("DESCRIBE HISTORY tabla1").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Append, Update Data
# MAGIC #### Vamos a añadir un nuevo registro a tabla1: Dora, de dieciocho años

# COMMAND ----------

spark.sql("INSERT INTO tabla1 VALUES ('Dora', 'dieciocho')")
spark.sql("SELECT * FROM tabla1").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ahora vamos a actualizar la edad de Dora, pues en realidad tiene diecinueve

# COMMAND ----------

spark.sql("UPDATE tabla1 SET edad = 'diecinueve' WHERE nombre = 'Dora'")
spark.sql("SELECT * FROM tabla1").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Vamos a crear una tabla parquet a partir de tabla1

# COMMAND ----------

spark.sql("CREATE TABLE tabla1_parquet STORED AS PARQUET AS SELECT * FROM tabla1")
spark.sql("SELECT * FROM tabla1_parquet").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Insertamos un nuevo registro en tabla1_parquet

# COMMAND ----------

spark.sql("INSERT INTO tabla1_parquet VALUES ('Ágata', 'treinta')")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Si intentásemos actualizar la edad de Ágata en tabla1_parquet, de treinta a veinte años. ¿Cuál sería el resultado?

# COMMAND ----------

#spark.sql("UPDATE tabla1_parquet SET edad = 'veinte' WHERE nombre = 'Ágata'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Overwrite Data
# MAGIC #### Creamos dos tablas llamadas music y music_new, con dos columnas tipo STRING

# COMMAND ----------

spark.sql("CREATE OR REPLACE TABLE music(cancion STRING, genero STRING) USING DELTA")
spark.sql("CREATE OR REPLACE TABLE music_new(cancion STRING, genero STRING) USING DELTA")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Insertamos algunos registros diferentes para cada una de las tablas

# COMMAND ----------

spark.sql("INSERT INTO music VALUES('let it be', 'pop rock'), ('some say', 'pop punk'), ('einmal um die welt', 'rap')")
spark.sql("INSERT INTO music_new VALUES('here comes the sun', 'pop rock'), ('still waiting', 'pop punk'), ('bye bye', 'rap')")

# COMMAND ----------

spark.sql("SELECT * FROM music").show()

# COMMAND ----------

spark.sql("SELECT * FROM music_new").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sobreescribimos la tabla music con el contenido de music_new

# COMMAND ----------

spark.sql("INSERT OVERWRITE TABLE music SELECT * FROM music_new")
spark.sql("SELECT * FROM music").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Vamos a hacer una consulta para ver la versión anterior

# COMMAND ----------

#Esto no funciona en local: https://github.com/delta-io/delta/issues/634
spark.sql("SELECT * FROM music VERSION AS OF 1").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 MERGE
# MAGIC #### Hacemos un merge entre la tabla1 y la tabla2. Cuando el nombre sea igual en ambas tablas, tomará el valor de edad correspondiente a tabla2. Si los nombres no coinciden entre las dos tablas, simplemente se insertarán como registros nuevos.

# COMMAND ----------

spark.sql("MERGE INTO tabla1 USING tabla2 ON tabla1.nombre = tabla2.nombre WHEN MATCHED THEN UPDATE SET edad = tabla2.edad WHEN NOT MATCHED THEN INSERT (nombre, edad) VALUES(tabla2.nombre, tabla2.edad)")

spark.sql("SELECT * FROM tabla1").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### ¿Qué ha sucedido en el directorio _delta_log?

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/user/hive/warehouse/tabla1/_delta_log

# COMMAND ----------

# MAGIC %md
# MAGIC #### Abre los archivos .json y compáralos. ¿Hay algún tipo de información interesante en ellos?

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5. Time Travel, versionado
# MAGIC #### Vamos a eliminar a Alfredo de tabla1

# COMMAND ----------

spark.sql("DELETE FROM tabla1 WHERE nombre = 'Alfredo'")
spark.sql("SELECT * FROM tabla1").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### ¿Cuál es ahora la versión actual de la tabla1? Compruébalo

# COMMAND ----------

spark.sql("DESCRIBE HISTORY tabla1").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Utilizando VERSION AS OF, cargamos una versión de la tabla1 anterior a la eliminación de Alfredo y la mostramos por pantalla para asegurarnos de que aparece

# COMMAND ----------

#Esto no funciona en local: https://github.com/delta-io/delta/issues/634
spark.sql("SELECT * FROM tabla1 VERSION AS OF 1").show()

#También es posible:
#spark.read.format("delta").option("versionAsOf", 1).load("/user/hive/warehouse/tabla1").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Comprueba si esto último ha sustituido la tabla1 por una versión anterior

# COMMAND ----------

spark.sql("SELECT * FROM tabla1").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Restaurar una versión anterior

# COMMAND ----------

#Esto no funciona en local
spark.sql("RESTORE tabla1 VERSION AS OF 1")

#También es posible:
#deltaTable = DeltaTable.forName(spark, "tabla1")
#deltaTable.restoreToVersion(1)

spark.sql("SELECT * FROM tabla1").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. API DeltaTable
# MAGIC https://docs.delta.io/latest/api/python/index.html
# MAGIC ### Cargar tabla por ruta con forPath()

# COMMAND ----------

dtTabla1 = DeltaTable.forPath(spark, "/user/hive/warehouse/tabla1")
dtTabla1.toDF().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cargar tabla por nombre con forName()

# COMMAND ----------

dtTabla2 = DeltaTable.forName(spark, "tabla2")
dtTabla2.toDF().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ver historia de la tabla

# COMMAND ----------

dtTabla1.history().show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Si añadimos un número n como argumento al método history() podemos ver únicamente las últimas n versiones de la tabla. Compruébalo

# COMMAND ----------

dtTabla1.history(2).show()

# COMMAND ----------

dtTabla1.history(1).show()

# COMMAND ----------

dtTabla1.history(0).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Restaurar tabla a versión específica 

# COMMAND ----------

#No funciona en local

#deltaTable = DeltaTable.forName(spark, "tabla1")
#deltaTable.restoreToVersion(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 DDL
# MAGIC #### Crear tabla delta
# MAGIC #### create()

# COMMAND ----------

dtExample = DeltaTable.create(spark).tableName("example") \
  .addColumn("c1", dataType = "INT", nullable = False) \
  .addColumn("c2", dataType = IntegerType(), generatedAlwaysAs = "c1 + 1") \
  .partitionedBy("c1") \
  .execute()
dtExample.toDF().show()

# COMMAND ----------

# MAGIC %md
# MAGIC También es posible pasar un schema al método addColumns():
# MAGIC .addColumns(df.schema)
# MAGIC ##### replace()

# COMMAND ----------

'''dtExample = DeltaTable.replace(spark).tableName("example") \
  .addColumn("c1", dataType = "INT", nullable = False) \
  .addColumn("c2", dataType = IntegerType(), generatedAlwaysAs = "c1 + 1") \
  .partitionedBy("c1") \
  .execute()
dtExample.toDF().show()'''

# COMMAND ----------

# MAGIC %md
# MAGIC ##### createOrReplace()

# COMMAND ----------

'''dtExample = DeltaTable.createOrReplace(spark).tableName("example") \
  .addColumn("c1", dataType = "INT", nullable = False) \
  .addColumn("c2", dataType = IntegerType(), generatedAlwaysAs = "c1 + 1") \
  .partitionedBy("c1") \
  .execute()
dtExample.toDF().show()'''

# COMMAND ----------

# MAGIC %md
# MAGIC ##### createIfNotExists()

# COMMAND ----------

'''dtExample = DeltaTable.createIfNotExists(spark).tableName("example") \
  .addColumn("c1", dataType = "INT", nullable = False) \
  .addColumn("c2", dataType = IntegerType(), generatedAlwaysAs = "c1 + 1") \
  .partitionedBy("c1") \
  .execute()
dtExample.toDF().show()'''

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 DML
# MAGIC #### Insertamos unos registros a la tabla2

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM tabla2;
# MAGIC 
# MAGIC INSERT INTO tabla2 VALUES('Juanita', 'trece'), ('Manolita', 'catorce'), ('Lolita', 'quince');
# MAGIC 
# MAGIC SELECT * FROM tabla2;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Borrar registros de una tabla con delete()

# COMMAND ----------

dtTabla2.delete("nombre = 'Juanita'")

#También es posible:
#dtTabla2.delete(F.col("nombre") == "Juanita")

dtTabla2.toDF().show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Borrar todos los registros de una tabla con delete() sin pasar ningún argumento

# COMMAND ----------

dtTabla2.delete()
dtTabla2.toDF().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ¿Qué hay en tabla2?

# COMMAND ----------

spark.sql("SELECT * FROM tabla2").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Actualizar registros con el método update()
# MAGIC ##### Cargamos la tabla vuelos en un deltaTable

# COMMAND ----------

dtVuelos = DeltaTable.forName(spark, "vuelos")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Sin usar las funciones de Spark SQL

# COMMAND ----------

dtVuelos.update(
    condition = "Dest != 'TPA'",
    set = { "Dest": "'TPA'" } )

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Usando las funciones de Spark SQL

# COMMAND ----------

'''dtVuelos.update(
    condition = F.col("Dest") != "TPA",
    set = { "Dest": F.lit("TPA") } )'''

# COMMAND ----------

# MAGIC %md
# MAGIC #### Upsert con merge()
# MAGIC ##### Creamos dos tablas delta. Las columnas serán “nombre” y “descripcion”, de tipo STRING

# COMMAND ----------

dtMuebles1 = DeltaTable.createOrReplace(spark).tableName("muebles1") \
  .addColumn("nombre", dataType = "STRING", nullable = False) \
  .addColumn("descripcion", dataType = "STRING", nullable = False) \
  .execute()

dtMuebles2 = DeltaTable.createOrReplace(spark).tableName("muebles2") \
  .addColumn("nombre", dataType = "STRING", nullable = False) \
  .addColumn("descripcion", dataType = "STRING", nullable = False) \
  .execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Insertamos algunos registros en las tablas. Nos aseguramos de que algunos nombres son iguales, aunque las descripciones serán diferentes

# COMMAND ----------

spark.sql("INSERT INTO muebles1 VALUES('silla', 'grande'), ('mesita', 'azul'), ('sofá', 'cuero'), ('escritorio', 'caoba')")
spark.sql("INSERT INTO muebles2 VALUES('silla', 'pequeña'), ('mesa', 'blanca'), ('cama', 'doble'), ('escritorio', 'plástico')")

# COMMAND ----------

dtMuebles1.toDF().show()

# COMMAND ----------

dtMuebles2.toDF().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Al hacer merge, una de las dos tablas tiene que ser un objeto tipo DataFrame, así que convertimos muebles2 en un df

# COMMAND ----------

dfMuebles2 = dtMuebles2.toDF()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Sin usar las funciones de Spark SQL

# COMMAND ----------

dtMuebles1.alias("muebles1").merge(
    source = dfMuebles2.alias("muebles2"),
    condition = "muebles1.nombre = muebles2.nombre"
  ).whenMatchedUpdate(set =
    {
      "muebles1.descripcion": "muebles2.descripcion"
    }
  ).whenNotMatchedInsertAll() \
   .execute()

dtMuebles1.toDF().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Usando las funciones de Spark SQL

# COMMAND ----------

'''dtMuebles1.alias("muebles1").merge(
    source = dfMuebles2.alias("muebles2"),
    condition = F.expr("muebles1.nombre = muebles2.nombre")
  ).whenMatchedUpdate(set =
    {
      "descripcion": F.col("muebles2.descripcion")
    }
  ).whenNotMatchedInsertAll() \
   .execute()

dtMuebles1.toDF().show()
'''

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Clone Table
# MAGIC Podemos clonar las tablas, pero cada una de ellas mantendrá su propio transaction log.
# MAGIC Hay dos formas de clonar las tablas delta:
# MAGIC - shallow: los archivos originales no se duplican, la tabla clonada utiliza los archivos de la original, pero tiene su propio transaction log.
# MAGIC - deep: los archivos originales se copian en la nueva tabla clonada.
# MAGIC 
# MAGIC ### 6.1 Shallow
# MAGIC #### Creamos una tabla llamada profesores e insertamos algunos valores

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE profesores(nombre STRING, asignatura STRING) USING DELTA;
# MAGIC INSERT INTO profesores VALUES('Arturo', 'música'), ('Lola', 'historia'), ('Mercedes', 'lengua');
# MAGIC SELECT * FROM profesores;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clonamos la tabla profesores a profesores_sc con SHALLOW CLONE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE profesores_sc SHALLOW CLONE profesores;
# MAGIC SELECT * FROM profesores_sc;

# COMMAND ----------

# MAGIC %md
# MAGIC #### ¿Qué (no) hay en el directorio profesores_sc?

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /user/hive/warehouse/profesores_sc

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Deep
# MAGIC #### Clonamos la tabla profesores a profesores_dc con DEEP CLONE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE profesores_dc DEEP CLONE profesores;
# MAGIC SELECT * FROM profesores_dc;

# COMMAND ----------

# MAGIC %md
# MAGIC #### ¿Qué hay en el directorio profesores_dc?

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /user/hive/warehouse/profesores_dc/

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Z-ORDER
# MAGIC Z-ORDER funciona como un clustered index. Agrupa los datos por una columna X para que al hacer una query haya archivos que no sea necesario leer, ya que el valor buscado no estará en todos ellos. Eso se llama data skipping.
# MAGIC ### Casos de uso
# MAGIC - Usamos Z-ORDER cuando queremos optimizar las consultas por una columna.
# MAGIC - Cuando queremos evitar el uso de IO innecesario, especialmente si hay un coste asociado.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vamos a añadir los registros de nombres.csv a una tabla nueva

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE tabla3 (nombre STRING, edad STRING);

# COMMAND ----------

dfTemp = spark.read.format("csv").load("dbfs:/FileStore/tables/nombres.csv")
dfTemp.createOrReplaceTempView("nombres_temp")
spark.sql("INSERT OVERWRITE tabla3 SELECT * FROM nombres_temp")
spark.sql("SELECT * FROM tabla3").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora vamos a hacer una consulta de todos los Pericos. ¿Cuánto ha tardado? Puedes ejecutar este código varias veces

# COMMAND ----------

spark.sql("SELECT * FROM tabla3 WHERE nombre = 'Perico' AND edad = 'diecinueve'").show(100000000)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Por una parte, tenemos OPTIMIZE, un proceso costoso que reorganiza las partes en que se divide una tabla. A OPTIMIZE se le puede añadir ZORDER. En este caso, añadimos ZORDER BY a la tabla1 por el nombre

# COMMAND ----------

spark.sql("OPTIMIZE tabla3 ZORDER BY nombre")

# COMMAND ----------

# MAGIC %md
# MAGIC ### De nuevo hacemos una consulta de todos los Pericos. ¿Cuánto ha tardado esta vez? Puedes ejecutar este código varias veces

# COMMAND ----------

spark.sql("SELECT * FROM tabla3 WHERE nombre = 'Perico' AND edad = 'diecinueve'").show(100000000)

# COMMAND ----------

tabla3_df = spark.read.format("delta").load("dbfs:/user/hive/warehouse/tabla3")
tabla3_df.write.format("delta").mode('overwrite').save("dbfs:/FileStore/tabla3")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Delta Lake
# MAGIC ### 8.1 Un solo almacenamiento para Batch y Streaming
# MAGIC Ya hemos visto cómo las tablas delta admiten lecturas y escrituras en batch. Del mismo modo, Delta Lake está integrado con Spark Streaming.
# MAGIC 
# MAGIC ### 8.2 Change Data Feed (CDF)
# MAGIC #### Casos de uso
# MAGIC - Tablas Silver y Gold: mejora el rendimiento procesando únicamente los cambios a nivel de fila.
# MAGIC - Vistas actualizadas: crea vistas actualizadas para BI y analytics sin tener que reprocesar las tablas.
# MAGIC - Transmite los cambios: puede enviar los cambios por ejemplo a Kafka o a un RDBMS.
# MAGIC - Genera un registro de auditoría: puede mostrar los cambios a través del tiempo.
# MAGIC 
# MAGIC (https://docs.databricks.com/delta/delta-change-data-feed.html)
# MAGIC 
# MAGIC #### Change data feed no está activado por defecto. Vamos a crear una tabla que lo tenga activado

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE perro (nombre STRING, raza STRING)
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Es posible activarlos para tablas existentes con ALTER TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC --ALTER TABLE tabla2 SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Otra forma de hacerlo es activarlo para todas las tablas nuevas

# COMMAND ----------

# MAGIC %sql
# MAGIC --SET spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Insertamos nuevos datos a la tabla

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO perro VALUES('Rex', 'pastor alemán'), ('Dexter', 'doberman'), ('Poppy', 'caniche');

# COMMAND ----------

# MAGIC %md
# MAGIC #### Observamos el directorio de la tabla perro

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/user/hive/warehouse/perro

# COMMAND ----------

# MAGIC %md
# MAGIC #### Vamos a actualizar el nombre de Rex a Regex

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE perro SET nombre='Regex' WHERE nombre='Rex';
# MAGIC 
# MAGIC SELECT * FROM perro;

# COMMAND ----------

# MAGIC %md
# MAGIC #### ¿Qué ha cambiado en el directorio de la tabla perro?

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/user/hive/warehouse/perro

# COMMAND ----------

# MAGIC %md
# MAGIC #### Formas de ver los cambios en las tablas con CDF en batch
# MAGIC Más información: https://docs.databricks.com/delta/delta-change-data-feed.html
# MAGIC #### SQL – Escribimos el nombre de la tabla, la versión o timestamp inicial y la versión o timestamp final (opcional) entre las que queremos ver los cambios

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM table_changes('perro', 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### En python / Scala (sin '\')

# COMMAND ----------

spark.read.format("delta") \
  .option("readChangeFeed", "true") \
  .option("startingVersion", 0) \
  .option("endingVersion", 10) \
  .table("perro").show()

#O bien:

#ATENCIÓN: los timestamps deben ser correctos, de lo contrario dará error
'''spark.read.format("delta") \
  .option("readChangeFeed", "true") \
  .option("startingTimestamp", '2021-04-21 05:45:46') \
  .option("endingTimestamp", '2021-07-19 13:38:57.0') \
  .table("perro").show()'''

# COMMAND ----------

# MAGIC %md
# MAGIC #### Lectura de CDF en streaming
# MAGIC ##### En python / Scala (con 'val' y sin '\')

# COMMAND ----------

sStream = spark.readStream.format("delta") \
  .option("readChangeFeed", "true") \
  .option("startingVersion", 0) \
  .table("perro")

display(sStream)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Cuando el comando anterior esté ejecutándose, insertamos un nuevo valor

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO perro VALUES("Doc", "San Bernardo")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Actualizamos el nombre del registro nuevo, de Doc a Dog

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE perro SET nombre='Dog' WHERE raza='San Bernardo'

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Observa el display(sStream) y asegúrate de que está ordenado por _commit_version de manera descendente para ver los cambios

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notas (a 21/07/2021)
# MAGIC 
# MAGIC ### Cosas que no funcionan en local pero sí en databricks
# MAGIC - Restaurar tabla a versión X, ni SQL ni DeltaTable API.
# MAGIC - Usar VERSION AS OF en SQL. Sí funciona en DeltaTable API.
# MAGIC - Optimize / ZORDER.
# MAGIC - VACUUM.
# MAGIC - Change Data Feed - se puede poner SET TBLPROPERTIES (delta.enableChangeDataFeed = true), pero no se puede usar con delta. Y si la tabla no es delta esa propiedad es inútil.
# MAGIC - CLONE.
# MAGIC 
# MAGIC ### Otros
# MAGIC - Databricks crea las tablas delta por defecto, sin necesidad de especificar el formato.
# MAGIC - No es posible hacer un vacuum únicamente al Change Data Feed, pero al hacer vacuum a una tabla también lo hace al CDF.
# MAGIC - Z-ORDER
# MAGIC   - no funciona consistentemente en el ejemplo de este notebook en cuanto a la rapidez. Lo esperado es que la consulta sea más rápida después de usar OPTIMIZE ZORDER BY, pero la realidad es que a veces sí, a veces no.
# MAGIC   - ZORDER BY parece modificar los minvalues y los maxvalues dentro de los archivos .json del delta_log. Parece que ahí es donde se almacena este índice.
# MAGIC - DeltaTable.convertToDelta() no ha funcionado en ningún momento, ni en databricks ni en local. Por el contrario, CONVERT TO DELTA funciona sin problemas tanto en databricks como en local.
