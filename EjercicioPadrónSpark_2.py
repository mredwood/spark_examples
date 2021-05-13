# encoding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Padrón SPARK").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.options(header=True, inferSchema=True, quotes='"', delimiter=";").csv("file:////home/marcos/Descargas/Rango_Edades_Seccion_202104.csv")


## Quitamos los espacios en blanco de las columnas DESC_BARRIO y DESC_DISTRITO
print("Quitamos los espacios en blanco de las columnas DESC_BARRIO y DESC_DISTRITO")

df = df.withColumn("DESC_DISTRITO", F.trim("DESC_DISTRITO"))
df = df.withColumn("DESC_BARRIO", F.trim("DESC_BARRIO"))

## Transformamos los nulls en 0
print("Transformamos los nulls en 0")
df = df.fillna(0)

df.printSchema()
df.limit(5).show()

## Creamos una vista temporal llamada padron
df.createOrReplaceTempView("padron")

## Enumeramos los barrios diferentes
print("Enumeramos los barrios diferentes")
dft = df.select("DESC_BARRIO").distinct()
dft.select(F.row_number().over(Window.partitionBy().orderBy("DESC_BARRIO")).alias("NUM_BARRIO"), "DESC_BARRIO").show()

## Contamos todos los barrios diferentes a través de la vista temporal. Dos opciones.
print("Contamos todos los barrios diferentes a través de la vista temporal. Dos opciones:")
## -Modo 1
query1 = """SELECT COUNT(DISTINCT DESC_BARRIO) AS NUM_TOTAL_BARRIOS FROM padron"""
spark.sql(query1).show()
## -Modo 2
print(df.select("DESC_BARRIO").distinct().count())

## Añadimos la columna longitud a la tabla padron
print("Añadimos la columna longitud a la tabla padron")
query2 = """SELECT *, LENGTH(DESC_DISTRITO) AS longitud FROM padron"""
df = spark.sql(query2)
df.createOrReplaceTempView("padron")
df.limit(3).show()

## Añadimos una columna con el valor 5 para todos los registros
print("Añadimos una columna con el valor 5 para todos los registros")
query3 = """SELECT *, 5 AS NUM5 FROM padron"""
df = spark.sql(query3)
df.createOrReplaceTempView("padron")
df.limit(3).show()

## Borramos esa última columna
print("Borramos esa última columna")
df = df.drop("NUM5")
df.createOrReplaceTempView("padron")
df.limit(3).show()


## Particionamos el dataframe por DESC_DISTRITO Y DESC_BARRIO
print("Particionamos el dataframe por DESC_DISTRITO y DESC_BARRIO")
df = df.repartition("DESC_DISTRITO", "DESC_BARRIO")
df.createOrReplaceTempView("padron")
df.limit(3).show()


## Lo almacenamos en caché
print("Lo almacenamos en caché")
df.cache()
print(df.count()) # Con esta acción hacemos que se invoque realmente el df.cache()

## Consultamos total de españoles, españolas, extranjeros y extranjeras por cada barrio y por cada distrito
print("Consultamos total de españoles, españolas, extranjeros y extranjeras por cada barrio y por cada distrito")
query4 = """SELECT DESC_DISTRITO, DESC_BARRIO, SUM(EspanolesHombres) AS TOTAL_ESPANOLES, SUM(EspanolesMujeres) AS TOTAL_ESPANOLAS, SUM(ExtranjerosHombres) AS TOTAL_EXTRANJEROS, SUM(ExtranjerosMujeres) AS TOTAL_EXTRANJERAS FROM padron GROUP BY DESC_DISTRITO, DESC_BARRIO ORDER BY SUM(ExtranjerosMujeres) DESC, SUM(ExtranjerosHombres)"""
spark.sql(query4).show(n=200)

## Eliminamos el registro en caché
print("Eliminamos el registro en caché")
df.unpersist()

## Creamos un nuevo dataframe a partir del original
print("Creamos un nuevo dataframe a partir del original")
query5 = """SELECT DESC_BARRIO, DESC_DISTRITO, SUM(EspanolesHombres) AS TOTAL_ESPANOLES_HOMBRES FROM padron GROUP BY DESC_BARRIO, DESC_DISTRITO"""
df2 = spark.sql(query5)
df2.show()
df2.createOrReplaceTempView("padron2")

## Hacemos join entre los dos dataframes
print("Hacemos join entre los dos dataframes")
query6 = """SELECT p.*, p2.TOTAL_ESPANOLES_HOMBRES FROM padron p JOIN padron2 p2 ON p.DESC_DISTRITO = p2.DESC_DISTRITO AND p.DESC_BARRIO = p2.DESC_BARRIO"""
spark.sql(query6).show(vertical=True)

## Repetimos la acción anterior usando funciones de ventana
print("Repetimos la acción anterior usando funciones de ventana")
df.select("DESC_BARRIO", "DESC_DISTRITO", F.sum("EspanolesHombres").over(Window.partitionBy("DESC_BARRIO", "DESC_DISTRITO")).alias("TOTAL_ESPANOLES_HOMBRES")).show()

## Hacemos una tabla de contingencia usando pivot, mostrando el total de españolas que hay según el COD_EDAD_INT
print("Hacemos una tabla de contingencia usando pivot, mostrando el total de españolas que hay según el COD_EDAD_INT")
pivotDF = df.groupBy("COD_EDAD_INT").pivot("DESC_DISTRITO", ["BARAJAS", "CENTRO", "RETIRO"]).sum("EspanolesMujeres").orderBy("COD_EDAD_INT")
pivotDF.show()
### Comprobación: spark.sql("""SELECT SUM(EspanolesMujeres), COD_EDAD_INT, DESC_DISTRITO FROM padron WHERE DESC_DISTRITO IN ('BARAJAS', 'CENTRO', 'RETIRO') GROUP BY COD_EDAD_INT, DESC_DISTRITO ORDER BY COD_EDAD_INT""").show()

## Creamos tres columnas nuevas, que indiquen el porcentaje de edad que representa cada distrito
print("Creamos tres columnas nuevas, que indiquen el porcentaje de edad que representa cada distrito")
pivotDF = pivotDF.withColumn("PORCENTAJE_BARAJAS", F.round((pivotDF.BARAJAS * 100) / (pivotDF.BARAJAS + pivotDF.CENTRO + pivotDF.RETIRO), 2))
pivotDF = pivotDF.withColumn("PORCENTAJE_CENTRO",  F.round((pivotDF.CENTRO * 100) / (pivotDF.BARAJAS + pivotDF.CENTRO + pivotDF.RETIRO), 2))
pivotDF = pivotDF.withColumn("PORCENTAJE_RETIRO",  F.round((pivotDF.RETIRO * 100) / (pivotDF.BARAJAS + pivotDF.CENTRO + pivotDF.RETIRO), 2))
pivotDF.show()

## Guardamos el dataframe original en .csv
print("Guardamos el dataframe original en .csv")
df.write.csv("file:////home/marcos/Descargas/PadronCSV/")

## Guardamos el dataframe original en parquet
print("Guardamos el dataframe original en parquet")
df.write.parquet("file:///home/marcos/Descargas/PadronParquet")
