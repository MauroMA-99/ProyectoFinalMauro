# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------


# Constantes

catalogo = "adbproyectofinal_prod"
esquema_source = "silver"
esquema_sink = "golden"

# COMMAND ----------

# Leemos la tabla transformada (silver)

df_movies_silver = spark.table(f"{catalogo}.{esquema_source}.movies_ratings_silver")

# COMMAND ----------

# Creamos métricas agregadas (golden layer)

df_golden = (
    df_movies_silver.groupBy("year")
    .agg(
        F.countDistinct("movieId").alias("num_peliculas"),
        F.countDistinct("userId").alias("num_usuarios"),
        F.round(F.avg("rating"), 2).alias("promedio_rating"),
        F.round(F.min("rating"), 2).alias("rating_minimo"),
        F.round(F.max("rating"), 2).alias("rating_maximo"),
        F.first("decada").alias("decada")
    )
    .orderBy(F.col("year").desc())
)

# COMMAND ----------

# Agregamos columna de fecha de carga
df_golden = df_golden.withColumn("load_date", F.current_timestamp())

# COMMAND ----------

df_golden.display()

# COMMAND ----------

# Guardamos el resultado en la capa Golden
df_golden.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_sink}.movies_insights")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

