# Databricks notebook source
# Librerias
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("storage-account", "adlsproyectofinalmauroma")

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

# Definición de constantes

storage_account = dbutils.widgets.get("storage-account")
bronze = "bronze"
silver = "silver"
golden = "golden"

scope = "accessScopeforADLS"
key = "storageAccessKey"

# COMMAND ----------

container = "raw"
ruta = f"abfss://{container}@{storage_account}.dfs.core.windows.net/movies.csv"

# COMMAND ----------

catalogo = "adbproyectofinal_prod"

# COMMAND ----------

# Definición del esquema

movies_schema = StructType([
    StructField("movieId", IntegerType(), False),
    StructField("title", StringType(), True),
    StructField("genres", StringType(), True)
])

# COMMAND ----------

# Leer CSV desde contenedor raw

df_movies = spark.read \
    .option("header", True) \
    .schema(movies_schema) \
    .csv(ruta)


# COMMAND ----------

# Agregar columna de fecha de ingesta

df_movies_final = df_movies.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# Guardar tabla en capa bronze

df_movies_final.write.mode("overwrite").saveAsTable(f"{catalogo}.{bronze}.movies")

# COMMAND ----------

