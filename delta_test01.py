#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

spark = SparkSession.builder.appName('delta_test')     .config('spark.jars.packages', "io.delta:delta-core_2.12:1.0.0")     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")     .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")     .getOrCreate()

from delta import *
from delta.tables import *


# ## Borramos las tablas si ya existen

# In[2]:


spark.sql("DROP TABLE IF EXISTS tabla1")
spark.sql("DROP TABLE IF EXISTS tabla2")


# ## Borramos los archivos usando bash (linux)

# In[3]:


get_ipython().system(' hdfs dfs -rm -r /user/hive/warehouse')


# In[4]:


get_ipython().system(' rm -r /home/manjaro/delta-table')


# ## Creamos una tabla delta con dos columnas, nombre y edad, e insertamos 3 registros diferentes.

# In[5]:


spark.sql("CREATE TABLE tabla1(nombre STRING, edad STRING) USING DELTA")
spark.sql('INSERT INTO tabla1 VALUES("Pepe", "dieciséis"), ("Juan", "veinticinco"), ("Antonia", "ochenta y cinco"), ("Margarita", "veintiuno")')
spark.sql("SELECT * FROM tabla1").show()


# ## Guardamos la tabla1 en un directorio local.

# In[6]:


df1 = spark.table("tabla1")
df1.write.format("delta").mode("overwrite").save("file:///home/manjaro/delta-table/tabla1")
df1.show()


# ## Creamos otra tabla delta con dos columnas, nombre y edad, e insertamos 3 registros. Dos de ellos diferentes a los de la tabla1. En el caso del tercer registro, tenemos que actualizar la edad de Antonia, pues era un error.

# In[7]:


spark.sql("CREATE TABLE tabla2(nombre STRING, edad STRING) USING DELTA")
spark.sql('INSERT INTO tabla2 VALUES("Juanita", "quince"), ("Alfredo", "treinta y dos"), ("Antonia", "setenta y cinco")')
spark.sql("SELECT * FROM tabla2").show()


# ## Guardamos la tabla2 en un directorio local.

# In[8]:


df2 = spark.table("tabla2")
df2.write.format("delta").mode("overwrite").save("file:///home/manjaro/delta-table/tabla2")
df2.show()


# ### Observa los archivos que se han creado en la ruta local. ¿Cuántos hay? ¿Qué hay en el directorio _delta_log?

# In[9]:


get_ipython().system(' ls -l /home/manjaro/delta-table/tabla1')


# In[10]:


get_ipython().system(' ls -l /home/manjaro/delta-table/tabla1/_delta_log')


# ## Cargamos con DeltaTable.forPath() la tabla1 como un objeto de tipo DeltaTable.

# In[11]:


deltaTable1 = DeltaTable.forPath(spark, "file:///home/manjaro/delta-table/tabla1")


# ## Hacemos un merge entre deltaTable1 y el df2. Cuando el nombre sea igual en ambas tablas, tomará el valor de edad correspondiente a tabla2. Si los nombres no coinciden en las dos tablas, simplemente se insertarán como registros nuevos.

# In[12]:


deltaTable1.alias("tabla1").merge(df2.alias("tabla2"), "tabla1.nombre = tabla2.nombre").whenMatchedUpdate(set = { "edad" : "tabla2.edad"}).whenNotMatchedInsertAll().execute()


# In[31]:


deltaTable1.toDF().show()


# ### Observa los archivos de la tabla1 en la ruta local. ¿Cuántos hay? ¿Han cambiado? ¿Qué ha sucedido en el directorio _delta_log? Abre los archivos .json y compáralos.

# In[15]:


get_ipython().system(' ls -l /home/manjaro/delta-table/tabla1')


# In[29]:


get_ipython().system(' ls -l /home/manjaro/delta-table/tabla1/_delta_log')


# In[30]:


get_ipython().system(' cat /home/manjaro/delta-table/tabla1/_delta_log/00000000000000000000.json')


# In[18]:


get_ipython().system(' cat /home/manjaro/delta-table/tabla1/_delta_log/00000000000000000001.json')


# ### Añade algunos registros nuevos, elimínalos y obseva qué sucede dentro de la ruta local al guardar la tabla.

# ## Vamos a crear un df que sea la tabla1 filtrada para que Alfredo no aparezca. A continuación, guardamos la tabla en el directorio local de tabla1.

# In[19]:


df_merged = spark.read.format("delta").load("file:///home/manjaro/delta-table/tabla1").filter(F.col("nombre") != "Alfredo")
df_merged.show()
df_merged.write.format("delta").mode("overwrite").save("file:///home/manjaro/delta-table/tabla1")


# ## Utilizando la opción versionAsOf, cargamos una versión de la tabla1 anterior a la eliminación de Alfredo y la mostramos por pantalla para asegurarnos de que aparece.

# In[20]:


df_v01 = spark.read.format("delta").option("versionAsOf", 1).load("file:///home/manjaro/delta-table/tabla1")
df_v01.show()


# In[21]:


df_00 = spark.read.format("delta").option("versionAsOf", 0).load("file:///home/manjaro/delta-table/tabla1")
df_00.show()


