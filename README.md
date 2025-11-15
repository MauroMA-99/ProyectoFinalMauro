# ⭐ MovieRatings Analytics Pipeline
### Arquitectura Medallion en Azure Databricks

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com/)
[![Azure](https://img.shields.io/badge/Azure-0078D4?style=for-the-badge&logo=microsoft-azure&logoColor=white)](https://azure.microsoft.com/)
[![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=for-the-badge&logo=delta&logoColor=white)](https://delta.io/)
[![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=power-bi&logoColor=black)](https://powerbi.microsoft.com/)
[![CI/CD](https://img.shields.io/badge/CI%2FCD-GitHub_Actions-2088FF?style=for-the-badge&logo=github-actions&logoColor=white)](https://github.com/features/actions)

*Pipeline automatizado de datos para análisis de peliculas por rating con arquitectura de tres capas y despliegue continuo*

</div>

## 🎯 Descripción

📄 MovieRatings Analytics Pipeline es un proyecto de ingeniería de datos que implementa un flujo ETL completo en Databricks para procesar la información de películas y calificaciones de usuarios.
Los archivos movies.csv y ratings.csv se ingieren en el nivel Bronze, se limpian y transforman en Silver, y luego se modelan en tablas Golden listas para análisis avanzado.

El proyecto incluye deduplicación de columnas, enriquecimiento de datos (años, géneros, complejidad), categorización de ratings y creación de métricas agregadas, permitiendo habilitar dashboards en Power BI y análisis de machine learning basados en preferencias de usuarios y características de películas.

### ✨ Características Principales

- 🔄 **ETL Automatizado** - Pipeline completo con despliegue automático via GitHub Actions
- 🏗️ **Arquitectura Medallion** - Separación clara de capas Bronze → Silver → Gold
- 📊 **Modelo Dimensional** - Star Schema optimizado para análisis de negocio
- 🚀 **CI/CD Integrado** - Deploy automático en cada push a master
- 📈 **Power BI Ready** - Conexión directa con SQL Warehouse
- ⚡ **Delta Lake** - ACID transactions y time travel capabilities

## 🏛️ Arquitectura

### Flujo de Datos

```
📄 CSV (Raw Data)
    ↓
🛢️ Raw (contenedor)
    ↓
🥉 Bronze Layer (Ingesta sin transformación)
    ↓
🥈 Silver Layer (Limpieza + Modelo Dimensional)
    ↓
🥇 Gold Layer (Agregaciones de Negocio)
    ↓
📊 Power BI (Visualización)
```

### 📦 Capas del Pipeline

<table>
<tr>
<td width="33%" valign="top">

#### 🥉 Bronze Layer  
**Propósito**: Zona de aterrizaje  

**Tablas**:  
- `movies`  
- `ratings`  

**Características**:  
- ✅ Datos tal cual vienen del CSV  
- ✅ Timestamp de ingesta (`ingestion_date`)  
- ✅ Sin transformaciones ni validaciones  
- ✅ Preserva estructura original  

</td>
<td width="33%" valign="top">

#### 🥈 Silver Layer  
**Propósito**: Limpieza y enriquecimiento  

**Tablas**:  
- `movies_ratings_silver`

**Características**:  
- ✅ Normalización de columnas  
- ✅ Eliminación de duplicados (ej.: `movieId` repetido)  
- ✅ Columnas derivadas (`year`, `title_clean`, `rating_date`)  
- ✅ UDFs para clasificaciones (`rating_categoria`, `complejidad_genero`)  
- ✅ Join entre movies y ratings para construir dataset unificado  

</td>
<td width="33%" valign="top">

#### 🥇 Gold Layer  
**Propósito**: Analytics-ready  

**Tablas**:  
- `movies_insights`  

**Características**:  
- ✅ Pre-agregados (ej.: años de antigüedad, métricas por película)  
- ✅ Listo para BI (Power BI, dashboards)  
- ✅ Optimizado para performance  
- ✅ KPIs y métricas listas para análisis avanzado  

</td>
</tr>
</table>


---

## 📁 Estructura del Proyecto

```
coffee-shop-etl/
│
├── 📂 .github/
│   └── 📂 workflows/
│       └── 📄 databricks-deploy.yml    # Pipeline CI/CD
│
├── 📂 dashboard/
│   ├── 📷 Dashboard_powerBi.png        # Imagen dashboard
│   └── 📄 Dashboard_AnalisisDePeliculas.pbix     # Archivo Power BI
│
├── 📂 reversion/
│   └── 🐍 Reversion.py     # REVOKES
│
├── 📂 .github/workflows/
│    └── 📄 deploy-notebook.yml       # Archivo yaml
│
├── 📂 seguridad/
│   └── 🐍 Permisos.py                # Grants
│
├── 📂 scripts/
│   └── 📄 CreacionSQL.py             # CReacion del catalog, schemas, etc.
│
├── 📂 proceso/
│   ├── 🐍 Ingest_movies.py            # Bronze Layer
│   ├── 🐍 Ingest_rating.py            # Bronze Layer
│   ├── 🐍 Transform.py                # Silver Layer
│   ├── 🐍 Load.py                     # Gold Layer
│   └── 🐍 DeltaSharing.py             # Exportacion de la tabla movies_insight
│
├── 📂 certificaiones/
│   ├── 📄 DatabricksFundamentals.jpeg                # Fundamentos de Databricks
│   ├── 📄 GenerativeAIFundamentals.jpg               # Fundamentos de AI Generativa
│   └── 📄             # 
│
└── 📄 README.md
```

---

## 🛠️ Tecnologías

<div align="center">

| Tecnología | Propósito |
|:----------:|:----------|
| ![Databricks](https://img.shields.io/badge/Azure_Databricks-FF3621?style=flat-square&logo=databricks&logoColor=white) | Motor de procesamiento distribuido Spark |
| ![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=flat-square&logo=delta&logoColor=white) | Storage layer con ACID transactions |
| ![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat-square&logo=apache-spark&logoColor=white) | Framework de transformación de datos |
| ![ADLS](https://img.shields.io/badge/ADLS_Gen2-0078D4?style=flat-square&logo=microsoft-azure&logoColor=white) | Data Lake para almacenamiento persistente |
| ![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?style=flat-square&logo=github-actions&logoColor=white) | Automatización CI/CD |
| ![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=flat-square&logo=power-bi&logoColor=black) | Business Intelligence y visualización |

</div>

---
## ⚙️ Requisitos Previos

- ☁️ Cuenta de Azure con acceso a Databricks
- 💻 Workspace de Databricks configurado
- 🖥️ Cluster activo (nombre: `CLUSTER COFFEE SHOP`)
- 🐙 Cuenta de GitHub con permisos de administrador
- 📦 Azure Data Lake Storage Gen2 configurado
- 📊 Power BI Desktop (opcional para visualización)

---

## 🚀 Instalación y Configuración

<span style="color:red;">### 1. Creación del grupo de recursos</span>

![I1](images/creacion_rg.png)


### 2. Creacion del storage account

![I2](images/creacion_storage_acount.png)

![I3](images/creacion_storage_acount2.png)


### 3. Creacion del access conector

![I4](images/creacion_access_conector.png)

![I5](images/creacion_access_conector2.png)


### 4. Add  role Assignment

![I6](images/Add_role_assigment.png)

![I7](images/Add_role_assigment2.png)


### 5. Containers

![I8](images/Creacion_contenedores.png)


### 6. Creacion Azure databricks: produccion y desarrollo

![I9](images/creacion_ad-prod.png)

![I10](images/creacion_ad-dev.png)

![I11](images/creacion_ad-dev y prod.png)


### 7. Creacion del cluster (en el databricks de desarrollo)

![I12](images/creacion_cluster.png)


### 8. Creacion del metastore (cuenta EXT)

![I13](images/unitycalatlog-directorio.png)

![I14](images/creacion_metastore.png)


### 9. Creacion del repositorio en github

![I15](images/Creacion_repositorio_github.png)


### 10. Creacion del branch construccion

![I16](images/creacion_rama_branch.png)


### 11. Repositorio en el databricks de desarrollo

















