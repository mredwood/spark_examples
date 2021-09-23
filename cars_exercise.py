#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
#from delta import *
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Car").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


# In[3]:


dfCar = spark.read.options(header=True).csv("file:///home/manjaro/Documents/Car exercise/")


# In[4]:


dfCar.show()


# ## Renombrar columnas

# In[5]:


colNames = dfCar.schema.names
newColNames = []


# In[6]:


for name in colNames:
    newName = name.lower().replace(" ", "_")
    newColNames.append(newName)


# In[7]:


for i in range(len(colNames)):
    dfCar = dfCar.withColumnRenamed(colNames[i], newColNames[i])
    print("Modificado", colNames[i], "->", newColNames[i])


# In[8]:


dfCar.show()


# ## Sustituir vacíos por 0 en engine_cylinders cuando transmisión es DIRECT_DRIVE

# In[9]:


dfCar.select(F.col("engine_cylinders")).filter((F.col("transmission_type") == "DIRECT_DRIVE") & (F.col("engine_cylinders").isNull())).show()


# In[10]:


dfCar2 = dfCar.withColumn("engine_cylinders",
                          F.when((F.col("transmission_type") == "DIRECT_DRIVE") & (F.col("engine_cylinders").isNull()), 0)
                          .otherwise(F.col("engine_cylinders"))
                         )


# In[11]:


dfCar2.select(F.col("engine_cylinders")).filter(F.col("transmission_type") == "DIRECT_DRIVE").show()


# ## Agrupar por transmission type y engine cylinders y calcular la media

# In[12]:


dfCarAgg = dfCar2.groupBy(F.col("transmission_type"), F.col("engine_cylinders")).agg(F.avg("msrp").alias("media_msrp"))
# ¿Se podría añadir esto al final? .filter(F.col("engine_cylinders").isNotNull())


# In[13]:


dfCarAgg.orderBy("engine_cylinders").show(60)


# In[14]:


dfCarJoin = dfCar2.join(dfCarAgg, (dfCar2.transmission_type == dfCarAgg.transmission_type) & (dfCar2.engine_cylinders == dfCarAgg.engine_cylinders), how="full").select(dfCar2["*"], dfCarAgg.media_msrp)


# In[15]:


dfCarJoin.where(F.col("msrp").isNull() & F.col("media_msrp").isNotNull()).show()


# In[16]:


dfCar3 = dfCarJoin.withColumn("msrp", 
                              F.when((F.col("msrp").isNull()) & (F.col("media_msrp").isNotNull()), F.col("media_msrp"))
                              .otherwise(F.col("msrp"))
                             )


# In[17]:


dfCar3.filter(F.col("media_msrp") == 24921.076923076922).show()


# ## Calcular coches en cada categoría

# In[18]:


dfCar4 = dfCar3.groupBy("market_category").agg(F.count("model").alias("num_models"))


# In[19]:


dfCar4.show()

