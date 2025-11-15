# Databricks notebook source
# Librerias
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# VARIABLES
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
ruta = f"dbfs:/mnt/{storage_account}/{container}/ratings.csv"

# COMMAND ----------

catalogo = "adbproyectofinal_prod"

# COMMAND ----------

# Definición del esquema

ratings_schema = StructType([
    StructField("userId", IntegerType(), False),
    StructField("movieId", IntegerType(), False),
    StructField("rating", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])

# COMMAND ----------

# Leer CSV desde contenedor raw

df_ratings = spark.read \
    .option("header", True) \
    .schema(ratings_schema) \
    .csv(ruta)

# COMMAND ----------

# Agregar columna de fecha de ingesta

df_ratings_final = df_ratings.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# Guardar tabla en capa bronze

df_ratings_final.write.mode("overwrite").saveAsTable(f"{catalogo}.{bronze}.ratings")

# COMMAND ----------

