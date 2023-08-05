import pandas as pd
#from extract.extract_public_project import extract
from transform.data_extractor_gcs import data_datalake
#from transform.data_quality_validator import quality_validator
from transform.datos_basicos__cleanup import datos_basicos_cleanup
from transform.datos_contratos_cleanup import datos_contratos_cleanup
from transform.models.modeler_proyectos import models_proyecto
from transform.models.modeler_contratos import models_contratos
from load.load import load_bigquery

#Extraer data de la API de datos abierto Colombia y subir a Cloud Storage.
#extract = extract()

#Transformación data que se almaceno en Cloud Storage para creación de tabla de análisis.
#Conexión al datalake y extracción de data
[df_datos_basicos, df_datos_contratos] = data_datalake()
#Validación de la calidad de datos
#df_datos_basicos = quality_validator(df_datos_basicos)
#Limpieza de la data
df_datos_basicos = datos_basicos_cleanup(df_datos_basicos)
df_datos_contratos = datos_contratos_cleanup(df_datos_contratos)
#Modelado de la data
[dim_proyectos_cer, dim_sector, dim_estado_proyecto, dim_tipo_proyecto, dim_plan, dim_subestado, fact_proyecto] = models_proyecto(df_datos_basicos)
[dim_tipo_doc, dim_prove, dim_estado_contrato,fact_contratos] = models_contratos(df_datos_contratos)

#Carga de datos a BigQuery
load_bigquery(dim_proyectos_cer,dim_sector, dim_estado_proyecto, dim_tipo_proyecto, dim_plan, \
              dim_subestado, fact_proyecto, dim_tipo_doc, dim_prove, dim_estado_contrato,fact_contratos)



