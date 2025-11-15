# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# VARIABLES WIDGETS
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("storage-account", "adlsproyectofinalmauroma")

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

storage_account = dbutils.widgets.get("storage-account")
bronze = "bronze"
silver = "silver"
golden = "golden"

scope = "accessScopeforADLS"
key = "storageAccessKey"

# COMMAND ----------

client_id            = dbutils.secrets.get(scope="accessScopeforADLS", key="databricks-app-client-id1")
tenant_id            = dbutils.secrets.get(scope="accessScopeforADLS", key="databricks-app-tenant-id1")
client_secret        = dbutils.secrets.get(scope="accessScopeforADLS", key="databricks-app-client-secret1")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

def mount_adls(container_name):
    mount_point = f"/mnt/{storage_account}/{container_name}"
    
    # Verifica si el mount ya existe
    mounts = [m.mountPoint for m in dbutils.fs.mounts()]
    
    if mount_point in mounts:
        print(f"El mount {mount_point} ya existe. No se vuelve a montar.")
        return
    
    # Si no existe, montarlo
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/",
        mount_point = mount_point,
        extra_configs = configs
    )
    print(f"Mount creado en {mount_point}")

# COMMAND ----------

mount_adls("raw")

# COMMAND ----------

mount_adls("bronze")

# COMMAND ----------

mount_adls("silver")

# COMMAND ----------

mount_adls("golden")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists adbproyectofinal_prod;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists adbproyectofinal_prod.bronze;
# MAGIC create schema if not exists adbproyectofinal_prod.silver;
# MAGIC create schema if not exists adbproyectofinal_prod.golden;

# COMMAND ----------

