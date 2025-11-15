# Databricks notebook source
#Librerias

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

# Constantes

catalogo = "adbproyectofinal_prod"
esquema_source = "bronze"
esquema_sink = "silver"

# COMMAND ----------

# Leer tabla desde bronze

df_movies = spark.table(f"{catalogo}.{esquema_source}.movies")
df_ratings = spark.table(f"{catalogo}.{esquema_source}.ratings")

# COMMAND ----------

# Limpieza 

df_movies = df_movies.dropna(how="all").filter(F.col("movieid").isNotNull())

# COMMAND ----------

# Extraer el año de la columna 'title' (que está entre paréntesis)

df_movies = df_movies.withColumn(
    "year",
    F.regexp_extract(F.col("title"), r"\((\d{4})\)", 1).cast(IntegerType())
)


# COMMAND ----------

# Extraer el nombre de la película sin el año

df_movies = df_movies.withColumn(
    "title_clean",
    F.trim(F.regexp_replace(F.col("title"), r"\(\d{4}\)", ""))
)

# COMMAND ----------

# Contar la cantidad de géneros

df_movies = df_movies.withColumn(
    "num_genres",
    F.size(F.split(F.col("genres"), "\|"))
)


# COMMAND ----------

# Clasificar la película según número de géneros

def clasificar_complejidad(num_genres):
    if num_genres == 1:
        return "Monogénero"
    elif num_genres <= 3:
        return "Multigénero Moderado"
    else:
        return "Multigénero Extenso"


# COMMAND ----------

clasificar_udf = F.udf(clasificar_complejidad, StringType())


# COMMAND ----------

# Aplicacion de la funcion UDF

df_movies = df_movies.withColumn(
    "complejidad_genero",
    clasificar_udf(F.col("num_genres"))
)

# COMMAND ----------

# Agregar columna de fecha de transformación

df_movies = df_movies.withColumn("transformation_date", F.current_timestamp())


# COMMAND ----------

df_movies.display()

# COMMAND ----------

# --- Ratings ---

df_ratings = df_ratings.dropna(how="all").filter(F.col("movieId").isNotNull())

# COMMAND ----------

# Convertir timestamp UNIX a fecha legible

df_ratings = df_ratings.withColumn("rating_date", F.from_unixtime("timestamp").cast(TimestampType()))


# COMMAND ----------

# Clasificar rating

def clasificar_rating(valor):
    if valor <= 2.0:
        return "Bajo"
    elif valor <= 4.0:
        return "Medio"
    else:
        return "Alto"

# COMMAND ----------

rating_udf = F.udf(clasificar_rating, StringType())
df_ratings = df_ratings.withColumn("rating_categoria", rating_udf("rating"))

# COMMAND ----------

df_ratings.display()

# COMMAND ----------

df_movies.display()

# COMMAND ----------

# Alias para cada DataFrame

df_movies = df_movies.alias("m")
df_ratings = df_ratings.alias("r")


# =====================
# JOIN ENTRE MOVIES Y RATINGS
# =====================

# Inner Join 
df_joined = df_ratings.join(df_movies, F.col("r.movieId") == F.col("m.movieId"), "inner")

# COMMAND ----------

# =====================
# FILTROS Y COLUMNAS DERIVADAS
# =====================

# Selección de columnas sin ambigüedad
df_filtered = df_joined.select(
    F.col("r.userId").alias("userId"),
    F.col("r.movieId").alias("movieId"),
    F.col("r.rating").alias("rating"),
    F.col("r.timestamp").alias("timestamp"),
    F.col("m.title").alias("title"),
    F.col("m.genres").alias("genres"),
    F.col("m.year").alias("year"),
    F.col("m.num_genres").alias("num_genres"),
    F.col("m.complejidad_genero").alias("complejidad_genero"),
    F.col("m.transformation_date").alias("transformation_date")
)

# COMMAND ----------


# Filtrar solo películas desde el año 1990
df_filtered = df_filtered.filter(F.col("year") >= 1990)

# COMMAND ----------

# Diferencia de años entre hoy y su estreno

df_filtered = df_filtered.withColumn("years_since_release", F.year(F.current_date()) - F.col("year"))


# COMMAND ----------

# Clasificar según época

df_filtered = df_filtered.withColumn(
    "decada",
    F.when(F.col("year").between(1990, 1999), "90s")
     .when(F.col("year").between(2000, 2009), "2000s")
     .when(F.col("year").between(2010, 2019), "2010s")
     .otherwise("2020+")
)

# COMMAND ----------

# Clasificar popularidad por promedio (agrupamos internamente)

df_avg = df_filtered.groupBy("movieId").agg(F.round(F.avg("rating"), 2).alias("avg_rating"))
df_final = df_filtered.join(df_avg, on="movieId", how="left")


# COMMAND ----------

df_final = df_final.withColumn(
    "popularidad",
    F.when(F.col("avg_rating") < 3, "Poco valorada")
     .when(F.col("avg_rating") < 4, "Aceptable")
     .otherwise("Muy valorada")
)

# COMMAND ----------

# Fecha de transformación

df_final = df_final.withColumn("transformation_date", F.current_timestamp())

# COMMAND ----------

df_final.display()

# COMMAND ----------

# =====================
# ESCRITURA FINAL EN SILVER
# =====================

df_final.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_sink}.movies_ratings_silver")

# COMMAND ----------

