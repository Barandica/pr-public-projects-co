###########################################################################Importación de librerías
import numpy as np
import logging
from extract.extract_public_project import extraer_subir_datos_gcs
from transform.data_extractor_gcs import data_datalake
from transform.conteo import conteo
from transform.datos_basicos__cleanup import main_proyectos_cleanup
from transform.datos_contratos_cleanup import main_contratos_cleanup
from transform.models.modeler_proyectos import models_proyecto
from transform.models.modeler_contratos import models_contratos
from load.load import load_bigquery
from dotenv import load_dotenv
import os

load_dotenv()
#########################################################Configuración del registro de eventos
logger = logging.getLogger("general_log")
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] [%(asctime)s] [%(name)s]: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs_main_py.txt"),
    ])

###########################Extraer data de la API de datos abierto Colombia y subir a Cloud Storage
def extraccion_api(bucket_name, blob_name_1, blob_name_2, api_url_1, api_url_2):
    #Creación de  un objeto logger para el registro
    [columns_proyectos_e, size_proyectos_e, columns_contratos_e, size_contratos_e, t2_e] = \
    extraer_subir_datos_gcs(bucket_name, blob_name_1, blob_name_2, api_url_1, api_url_2)
    return [columns_proyectos_e, size_proyectos_e, columns_contratos_e, size_contratos_e, t2_e]

###################################Extraer data de Google Cloud Storage hacia el entorno de trabajo
def extraccion_gcs(bucket_name, blob_name_1, blob_name_2):
    [df_proyectos, df_contratos ,columns_proyectos_t, size_proyectos_t, columns_contratos_t, size_contratos_t, t2_t] =\
          data_datalake(bucket_name, blob_name_1, blob_name_2)
    return [df_proyectos, df_contratos ,columns_proyectos_t, size_proyectos_t, columns_contratos_t, size_contratos_t, t2_t]

###################Validacion de datos entre la carga de la API y la descarga al entorno de trabajo
def validaccion_api_gcs(columns_proyectos_t,size_proyectos_t,columns_contratos_t,size_contratos_t,columns_proyectos_e,\
            size_proyectos_e, columns_contratos_e, size_contratos_e):
        bool_c, t2_c = conteo(columns_proyectos_t,size_proyectos_t,columns_contratos_t,size_contratos_t,columns_proyectos_e, \
                      size_proyectos_e, columns_contratos_e, size_contratos_e)
        return [bool_c, t2_c]

################################################Limpieza de los archivos df_proyectos y df_contratos
def limpieza_data(df_proyectos_e, df_contratos_e, bool_c):
    if bool_c:
        #Limpieza de los datos
        df_proyectos, df_inconsistencias_pr, t2_pr = main_proyectos_cleanup(df_proyectos_e)
        df_contratos, df_inconsistencias_ct, t2_ct = main_contratos_cleanup(df_contratos_e)
        #Validacion de integridad luego de la limpieza
        registros_salientes_pr = df_proyectos.shape[0] + df_inconsistencias_pr.shape[0]
        registros_salientes_ct = df_contratos.shape[0] + df_inconsistencias_ct.shape[0]
        if registros_salientes_ct ==  df_contratos_e.shape[0] and registros_salientes_pr == df_proyectos_e.shape[0]:
            logger.info("Después de la limpieza, los datos mantienen su integridad")
            return [df_proyectos, df_inconsistencias_pr, t2_pr,df_contratos, df_inconsistencias_ct, t2_ct, True]
        else:
            logger.critical("Después de la limpieza, los datos NO mantienen su integridad ¡REVISAR!")
            return [df_proyectos, df_inconsistencias_pr, t2_pr,df_contratos, df_inconsistencias_ct, t2_ct, False]
        
    else:
        logger.critical("Los datos extraidos de la API y los extraidos en GCS NO mantienen su integridad")

##################################################################Creación del modelo copo de nieve
def modelacion_data(df_proyectos,df_contratos,bool_cl):
    if bool_cl:
        dim_proyectos_cer, dim_sector, dim_estado_proyecto, dim_tipo_proyecto, dim_subestado, dim_proyectos, t2_mp = models_proyecto(df_proyectos)
        dim_tipo_doc, dim_prove, dim_estado_contrato,fact_contratos, t2_mc = models_contratos(df_contratos)
        return [dim_proyectos_cer, dim_sector, dim_estado_proyecto, dim_tipo_proyecto, dim_subestado, dim_proyectos, t2_mp,dim_tipo_doc, dim_prove, dim_estado_contrato,fact_contratos, t2_mc]
    else:
        logger.critical("Después de la limpieza, los datos NO mantienen su integridad por tanto, NO se crea el modelo")

########################################################################Carga del modelo a BigQuery
def carga_data_bigquery(dataset, dim_proyectos_cer,dim_sector, dim_estado_proyecto, dim_tipo_proyecto,dim_subestado, dim_proyectos,\
        dim_tipo_doc, dim_prove, dim_estado_contrato,fact_contratos, df_inconsistencias_pr, df_inconsistencias_ct):    
    t2_l = load_bigquery (dataset, dim_proyectos_cer,dim_sector, dim_estado_proyecto, dim_tipo_proyecto,dim_subestado, dim_proyectos,\
            dim_tipo_doc, dim_prove, dim_estado_contrato,fact_contratos, df_inconsistencias_pr, df_inconsistencias_ct)
    return t2_l

##################################################################################Funcion principal
if __name__ == "__main__":
    logger.info("--------------------------------------------------------------------------------------------------------------")
    #Parametros necesarios para conexiones exteriores
    bucket_name = 'raw-public-projects'
    blob_name_1 = 'proyectos.json'
    blob_name_2 = 'contratos.json'
    api_url_1 = "https://www.datos.gov.co/resource/cf9k-55fw.json" #Datos de los proyectos 
    api_url_2 = "https://www.datos.gov.co/resource/uwns-mbwd.json" #Datos de los contratos
    dataset = "dwh_public_projects_co" # Esquema en BigQuery
    os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') #Credenciales de GCP

    #1. Extraccion desde la API hacia GCS
    [columns_proyectos_e, size_proyectos_e, columns_contratos_e, size_contratos_e, t2_e] = \
            extraccion_api(bucket_name, blob_name_1, blob_name_2, api_url_1, api_url_2)
    
    #2. Extraccion desde GCS hacia el entorno de trabajo
    [df_proyectos, df_contratos,columns_proyectos_t,size_proyectos_t,columns_contratos_t,size_contratos_t, t2_t] = \
            extraccion_gcs(bucket_name, blob_name_1, blob_name_2)
    
    #3. Validacion data entre el paso 1 y 2 
    [bool_c, t2_c] = validaccion_api_gcs(columns_proyectos_t,size_proyectos_t,columns_contratos_t,size_contratos_t,columns_proyectos_e,\
            size_proyectos_e, columns_contratos_e, size_contratos_e)

    #4. Limpieza de los datos
    [df_proyectos, df_inconsistencias_pr, t2_pr,df_contratos, df_inconsistencias_ct, t2_ct, bool_cl] = \
            limpieza_data(df_proyectos, df_contratos,bool_c=True)
    
    #5.Modelado de datos
    [dim_proyectos_cer, dim_sector, dim_estado_proyecto, dim_tipo_proyecto, dim_subestado, dim_proyectos, t2_mp,dim_tipo_doc, \
            dim_prove, dim_estado_contrato,fact_contratos, t2_mc] = modelacion_data(df_proyectos,df_contratos,bool_cl)
    
    #6. Carga de datos a BigQuery
    t2_l = carga_data_bigquery(dataset, dim_proyectos_cer,dim_sector, dim_estado_proyecto, dim_tipo_proyecto,dim_subestado, dim_proyectos,\
            dim_tipo_doc, dim_prove, dim_estado_contrato,fact_contratos, df_inconsistencias_pr, df_inconsistencias_ct)


    logger.info(f"Demore {np.round((t2_e + t2_t + t2_c + t2_pr + t2_ct + t2_mp + t2_mc + t2_l) / 60)} minutos en correr toda la tubería")




