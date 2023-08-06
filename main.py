###########################################################################Importación de librerías
import numpy as np
from extract.extract_public_project import extraer_subir_datos_gcs
from transform.data_extractor_gcs import data_datalake
from transform.conteo import conteo
from transform.datos_basicos__cleanup import main_proyectos_cleanup
from transform.datos_contratos_cleanup import main_contratos_cleanup
from transform.models.modeler_proyectos import models_proyecto
from transform.models.modeler_contratos import models_contratos
from load.load import load_bigquery

###########################Extraer data de la API de datos abierto Colombia y subir a Cloud Storage
def extraccion_api(bucket_name, blob_name_1, blob_name_2, api_url_1, api_url_2):
    #Creación de  un objeto logger para el registro
    [columns_proyectos_e, size_proyectos_e, columns_contratos_e, size_contratos_e, bool_e, t2_e] = \
    extraer_subir_datos_gcs(bucket_name, blob_name_1, blob_name_2, api_url_1, api_url_2)

    return [columns_proyectos_e, size_proyectos_e, columns_contratos_e, size_contratos_e, bool_e, t2_e]

###################################Extraer data de Google Cloud Storage hacia el entorno de trabajo
def extraccion_gcs(bucket_name, blob_name_1, blob_name_2, bool_e):
    if bool_e:
        [df_proyectos, df_contratos ,columns_proyectos_t, size_proyectos_t, columns_contratos_t, size_contratos_t, bool_t, t2_t] =\
          data_datalake(bucket_name, blob_name_1, blob_name_2)
        return [df_proyectos, df_contratos ,columns_proyectos_t, size_proyectos_t, columns_contratos_t, size_contratos_t, bool_t, t2_t]
    else:
        raise Exception("No se ha podido extraer los datos desde la API")

###################Validacion de datos entre la carga de la API y la descarga al entorno de trabajo
def validaccion_api_gcs(columns_proyectos_t,size_proyectos_t,columns_contratos_t,size_contratos_t,columns_proyectos_e, \
                      size_proyectos_e, columns_contratos_e, size_contratos_e, bool_t):
    if bool_t:
        bool_c, t2_c = conteo(columns_proyectos_t,size_proyectos_t,columns_contratos_t,size_contratos_t,columns_proyectos_e, \
                      size_proyectos_e, columns_contratos_e, size_contratos_e)
        return [bool_c, t2_c]
    else:
        raise Exception("No se ha podido comparar los archivos")

################################################Limpieza de los archivos df_proyectos y df_contratos
def limpieza_data(df_proyectos, df_contratos,bool_c):
    if bool_c:
        df_proyectos, df_inconsistencias_pr, bool_pr, t2_pr = main_proyectos_cleanup(df_proyectos)
        df_contratos, df_inconsistencias_ct, bool_ct, t2_ct = main_contratos_cleanup(df_contratos)
        return [df_proyectos, df_inconsistencias_pr, bool_pr, t2_pr,df_contratos, df_inconsistencias_ct, bool_ct, t2_ct]
    else:
        raise Exception("No se ha podido limpiar los archivos")

##################################################################Creación del modelo copo de nieve
def modelacion_data(df_proyectos,df_contratos,bool_pr,bool_ct):
    if bool_pr == True and bool_ct == True:
        dim_proyectos_cer, dim_sector, dim_estado_proyecto, dim_tipo_proyecto, dim_subestado, dim_proyectos, bool_mp, t2_mp = models_proyecto(df_proyectos)
        dim_tipo_doc, dim_prove, dim_estado_contrato,fact_contratos, bool_mc, t2_mc = models_contratos(df_contratos)
        return [dim_proyectos_cer, dim_sector, dim_estado_proyecto, dim_tipo_proyecto, dim_subestado, dim_proyectos, bool_mp, t2_mp,dim_tipo_doc, dim_prove, dim_estado_contrato,fact_contratos, bool_mc, t2_mc]
    else:
        raise Exception("No se ha podido crear el modelo")

########################################################################Carga del modelo a BigQuery
def carga_data_bigquery(dataset, dim_proyectos_cer,dim_sector, dim_estado_proyecto, dim_tipo_proyecto,dim_subestado, dim_proyectos,\
                        dim_tipo_doc, dim_prove, dim_estado_contrato,fact_contratos,bool_mp,bool_mc, df_inconsistencias_pr, df_inconsistencias_ct):
    if bool_mp == True and bool_mc == True:
        t2_l = load_bigquery(dataset, dim_proyectos_cer,dim_sector, dim_estado_proyecto, dim_tipo_proyecto, 
                dim_subestado, dim_proyectos, dim_tipo_doc, dim_prove, dim_estado_contrato,fact_contratos, df_inconsistencias_pr, df_inconsistencias_ct)
        return t2_l
    else:
        raise Exception("No se ha podido subir el modelo a BigQuery")

##################################################################################Funcion principal
if __name__ == "__main__":
    #Parametros necesarios para conexiones exteriores
    bucket_name = 'raw-public-projects'
    blob_name_1 = 'proyectos.json'
    blob_name_2 = 'contratos.json'
    api_url_1 = "https://www.datos.gov.co/resource/cf9k-55fw.json" #Datos de los proyectos 
    api_url_2 = "https://www.datos.gov.co/resource/uwns-mbwd.json" #Datos de los contratos
    dataset = "dwh_public_projects_co" # Esquema en BigQuery

    #1. Extraccion desde la API hacia GCS
    [columns_proyectos_e, size_proyectos_e, columns_contratos_e, size_contratos_e, bool_e, t2_e] = \
            extraccion_api(bucket_name, blob_name_1, blob_name_2, api_url_1, api_url_2)
    
    #2. Extraccion desde GCS hacia el entorno de trabajo
    [df_proyectos, df_contratos ,columns_proyectos_t, size_proyectos_t, columns_contratos_t, size_contratos_t, bool_t, t2_t] = \
            extraccion_gcs(bucket_name, blob_name_1, blob_name_2, bool_e)
    
    #3. Validacion data entre el paso 1 y 2 
    [bool_c, t2_c] = validaccion_api_gcs(columns_proyectos_t,size_proyectos_t,columns_contratos_t,size_contratos_t,columns_proyectos_e, \
            size_proyectos_e, columns_contratos_e, size_contratos_e, bool_t)
    
    #4. Limpieza de los datos
    [df_proyectos, df_inconsistencias_pr, bool_pr, t2_pr,df_contratos, df_inconsistencias_ct, bool_ct, t2_ct] = \
            limpieza_data(df_proyectos, df_contratos,bool_c)
    
    #5.Modelado de datos
    [dim_proyectos_cer, dim_sector, dim_estado_proyecto, dim_tipo_proyecto, dim_subestado, dim_proyectos, bool_mp, t2_mp,dim_tipo_doc, \
            dim_prove, dim_estado_contrato,fact_contratos, bool_mc, t2_mc] = modelacion_data(df_proyectos,df_contratos,bool_pr,bool_ct)
    
    #6. Carga de datos a BigQuery
    t2_l = carga_data_bigquery(dataset, dim_proyectos_cer,dim_sector, dim_estado_proyecto, dim_tipo_proyecto,dim_subestado, dim_proyectos,\
            dim_tipo_doc, dim_prove, dim_estado_contrato,fact_contratos,bool_mp,bool_mc,df_inconsistencias_pr, df_inconsistencias_ct)

    print(f"Demore {np.round((t2_e + t2_t + t2_c + t2_pr + t2_ct + t2_mp + t2_mc + t2_l) / 60)} minutos en correr toda la tubería")




