# Databricks notebook source
# MAGIC %sql
# MAGIC -- Permisos de lectura
# MAGIC GRANT SELECT ON TABLE catalog_dev.bronze.ventas TO `mauro2017pre@gmail.com`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Permisos de uso del catalogo para el grupo desarrollo1
# MAGIC GRANT USE CATALOG ON CATALOG adbproyectofinal_prod TO `desarrollo1`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Permiso para que el grupo Desarrollo2 use el schema y crear tablas nuevas
# MAGIC GRANT USE SCHEMA ON SCHEMA adbproyectofinal_prod.bronze TO `Desarrollo2`;
# MAGIC GRANT CREATE ON SCHEMA adbproyectofinal_prod.bronze TO `Desarrollo2`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON TABLE adbproyectofinal_prod.bronze.movies;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON SCHEMA adbproyectofinal_prod.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON CATALOG adbproyectofinal_prod;