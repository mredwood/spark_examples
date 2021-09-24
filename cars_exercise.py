#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
#from delta import *
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Car").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


# In[2]:


dfCar = spark.read.options(header=True).csv("file:///home/manjaro/Documents/Car exercise/")


# In[3]:


dfCar.show()


# ## Renombrar columnas

# In[4]:


colNames = dfCar.schema.names
newColNames = []


# In[5]:


for name in colNames:
    newName = name.lower().replace(" ", "_")
    newColNames.append(newName)


# In[6]:


for i in range(len(colNames)):
    dfCar = dfCar.withColumnRenamed(colNames[i], newColNames[i])
    print("Modificado", colNames[i], "->", newColNames[i])


# In[7]:


dfCar.count()


# ## Sustituir vacíos por 0 en engine_cylinders cuando transmisión es DIRECT_DRIVE

# In[8]:


dfCar.select(F.col("engine_cylinders")).filter((F.col("transmission_type") == "DIRECT_DRIVE") & (F.col("engine_cylinders").isNull())).show()


# In[9]:


dfCar2 = dfCar.withColumn("engine_cylinders",
                          F.when((F.col("transmission_type") == "DIRECT_DRIVE") & (F.col("engine_cylinders").isNull()), 0)
                          .otherwise(F.col("engine_cylinders"))
                         )


# In[10]:


dfCar2.count()


# In[11]:


dfCar2.select(F.col("engine_cylinders")).filter(F.col("transmission_type") == "DIRECT_DRIVE").show()


# ## Agrupar por transmission type, vehicle_size, vehicle_style y calcular la media de engine_cylinders

# In[12]:


dfCarAgg = dfCar2.groupBy(F.col("transmission_type"), F.col("vehicle_size"), F.col("vehicle_style")).agg(F.avg("engine_cylinders").alias("media_engine_cylinders"))


# In[13]:


dfCarAgg.orderBy("media_engine_cylinders").show(60)


# In[14]:


dfCarJoin = dfCar2.join(dfCarAgg, (dfCar2.transmission_type == dfCarAgg.transmission_type) & (dfCar2.vehicle_size == dfCarAgg.vehicle_size) & (dfCar2.vehicle_style == dfCarAgg.vehicle_style) & (dfCar2.engine_cylinders.isNull())).select(dfCar2["*"], dfCarAgg.media_engine_cylinders)


# In[15]:


dfCarJoin.select("engine_cylinders", "media_engine_cylinders").show(200)


# In[16]:


dfCarJoin.count()


# In[17]:


dfCar3 = dfCarJoin.withColumn("engine_cylinders", 
                              F.when(F.col("engine_cylinders").isNull(), F.col("media_engine_cylinders"))
                              .otherwise(F.col("engine_cylinders"))
                             ).drop("media_engine_cylinders")


# In[18]:


dfCar3.select("engine_cylinders").show()


# In[19]:


dfCar4 = dfCar3.union(dfCar2.filter("engine_cylinders is not null"))


# In[20]:


dfCar4.show()


# In[21]:


dfCar.count()


# In[22]:


dfCar2.count()


# In[23]:


dfCar2.filter("engine_cylinders is null and transmission_type is null").count()


# In[24]:


dfCar2.filter("transmission_type is not null and engine_cylinders is null").count()


# In[25]:


dfCar3.count()


# In[26]:


dfCar4.count()


# ## Calcular coches en cada categoría

# In[27]:


dfCar5 = dfCar4.groupBy("market_category").agg(F.count("model").alias("num_models"))


# In[28]:


dfCar5.show()


# In[29]:


from pyspark.sql.types import IntegerType


# In[33]:


dfCar4 = dfCar4.withColumn("msrp", dfCar4["msrp"].cast(IntegerType()))
dfCar6 = dfCar4.groupBy("make").agg(F.max("msrp"))
dfCar6.orderBy("make", desc=False).show()


# In[34]:


dfCar7 = dfCar4.groupBy("make").agg(F.avg("msrp"))
dfCar7.orderBy("make").show()

