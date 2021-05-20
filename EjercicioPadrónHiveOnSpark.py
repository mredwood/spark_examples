from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Padrón HIVE ON SPARK").enableHiveSupport().getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

## Borramos todo.

spark.sql("DROP TABLE IF EXISTS datos_padron.padron_txt_string")
spark.sql("DROP TABLE IF EXISTS datos_padron.padrontemp")
spark.sql("DROP TABLE IF EXISTS datos_padron.padron_txt")
spark.sql("DROP TABLE IF EXISTS datos_padron.padron_txt_2")
spark.sql("DROP DATABASE IF EXISTS datos_padron")

## Creamos una base de datos llamada datos_padron.

spark.sql("CREATE DATABASE datos_padron")

spark.sql("SHOW DATABASES").show()

spark.sql("USE datos_padron")

## Creamos una tabla padron_txt_String.

spark.sql(r"""CREATE TABLE padron_txt_string(COD_DISTRITO int, DESC_DISTRITO string, COD_DIST_BARRIO int, DESC_BARRIO string, COD_BARRIO int, COD_DIST_SECCION int, COD_SECCION int, COD_EDAD_INT int, EspanolesHombres int, EspanolesMujeres int, ExtranjerosHombres int, ExtranjerosMujeres int) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ("separatorChar" = ';', "quoteChar" = '"', "escapeChar" = '\\') STORED AS TEXTFILE""")

spark.sql("SHOW TABLES").show()


spark.sql("DESCRIBE padron_txt_string").show()


#spark.sql("""ALTER TABLE padron_txt_string SET TBLPROPERTIES ("skip.header.line.count"="1")""") ## ¡No funciona! Parece ser un bug conocido.

spark.sql("""LOAD DATA LOCAL INPATH '/home/manjaro/Downloads/Rango_Edades_Seccion_202105.csv' INTO TABLE padron_txt_string""")

spark.sql("""CREATE TABLE padronTemp AS SELECT * FROM padron_txt_string WHERE cod_distrito != 'COD_DISTRITO'""") # Esto es un hack, pero funciona.


## Creamos una tabla que transforme los strings en int.

spark.sql("""CREATE TABLE padron_txt AS SELECT CAST(COD_DISTRITO AS INT) AS COD_DISTRITO, DESC_DISTRITO, CAST(COD_DIST_BARRIO AS INT) AS COD_DIST_BARRIO, DESC_BARRIO, CAST(COD_BARRIO AS INT) AS COD_BARRIO, CAST(COD_DIST_SECCION AS INT) AS COD_DIST_SECCION, CAST(COD_SECCION AS INT) AS COD_SECCION, CAST(COD_EDAD_INT AS INT) AS COD_EDAD_INT, CAST(EspanolesHombres AS INT) AS EspanolesHombres, CAST(EspanolesMujeres AS INT) AS EspanolesMujeres, CAST(ExtranjerosHombres AS INT) AS ExtranjerosHombres, CAST(ExtranjerosMujeres AS INT) AS ExtranjerosMujeres FROM padronTemp""")

## Creamos una tabla padron_txt_2, recortando los espacios y poniendo cero en lugar de null.

spark.sql("""CREATE TABLE padron_txt_2 AS SELECT COD_DISTRITO, TRIM(DESC_DISTRITO) AS DESC_DISTRITO, COD_DIST_BARRIO, TRIM(DESC_BARRIO) AS DESC_BARRIO, COD_BARRIO, COD_DIST_SECCION, COD_SECCION, COD_EDAD_INT, (CASE WHEN EspanolesHombres IS NULL THEN 0 ELSE EspanolesHombres END) AS EspanolesHombres, (CASE WHEN EspanolesMujeres IS NULL THEN 0 ELSE EspanolesMujeres END) AS EspanolesMujeres, (CASE WHEN ExtranjerosHombres IS NULL THEN 0 ELSE ExtranjerosHombres END) AS ExtranjerosHombres, (CASE WHEN ExtranjerosMujeres IS NULL THEN 0 ELSE ExtranjerosMujeres END) AS ExtranjerosMujeres FROM padron_txt""")

spark.sql("SELECT * FROM padron_txt_2 LIMIT 10").show()
