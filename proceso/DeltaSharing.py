# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# Constantes

catalogo = "adbproyectofinal_prod"
esquema_source = "golden"

# COMMAND ----------

# Leemos la tabla load (golden)

df_movies_insights = spark.table(f"{catalogo}.{esquema_source}.movies_insights")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SHARE IF NOT EXISTS MOVIES_INSIGHTS_SHARE

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SHARES

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE PROCEDURE add_table_to_share_if_needed()
# MAGIC RETURNS STRING
# MAGIC LANGUAGE SQL
# MAGIC AS
# MAGIC BEGIN
# MAGIC     -- Verificar si la tabla ya está en el share
# MAGIC     IF (EXISTS (
# MAGIC         SELECT *
# MAGIC         FROM system.share_tables
# MAGIC         WHERE share_name = 'MOVIES_INSIGHTS_SHARE'
# MAGIC         AND full_table_name = 'adbproyectofinal_prod.golden.movies_insights'
# MAGIC     )) THEN
# MAGIC
# MAGIC         RETURN 'Tabla ya existe en share';
# MAGIC
# MAGIC     ELSE
# MAGIC         ALTER SHARE MOVIES_INSIGHTS_SHARE
# MAGIC         ADD TABLE adbproyectofinal_prod.golden.movies_insights WITH HISTORY;
# MAGIC
# MAGIC         RETURN 'Tabla sumada a share';
# MAGIC     END IF;
# MAGIC END;
# MAGIC

# COMMAND ----------

CALL add_table_to_share_if_needed();

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE RECIPIENT IF NOT EXISTS ExternalCompanys

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT SELECT 
# MAGIC ON SHARE MOVIES_INSIGHTS_SHARE
# MAGIC TO RECIPIENT ExternalCompanys

# COMMAND ----------

