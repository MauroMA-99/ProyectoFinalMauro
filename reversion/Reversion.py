# Databricks notebook source
# MAGIC %sql
# MAGIC -- Quitar permisos de lectura
# MAGIC REVOKE SELECT ON TABLE catalog_dev.bronze.ventas TO `mauro2017pre@gmail.com`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quitar permisos de uso del catalogo para el grupo desarrollo1
# MAGIC REVOKE USE CATALOG ON CATALOG adbproyectofinal_prod TO `desarrollo1`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quitar permiso para que el grupo Desarrollo2 use el schema y crear tablas nuevas
# MAGIC REVOKE USE SCHEMA ON SCHEMA adbproyectofinal_prod.bronze TO `Desarrollo2`;
# MAGIC REVOKE CREATE ON SCHEMA adbproyectofinal_prod.bronze TO `Desarrollo2`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW REVOKE ON TABLE adbproyectofinal_prod.bronze.movies;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON TABLE adbproyectofinal_prod.bronze.movies;