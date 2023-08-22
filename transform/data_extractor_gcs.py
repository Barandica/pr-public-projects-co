####################################################################Importación de librerias
from google.cloud import storage
import pandas as pd
import logging
import time as time
import numpy as np

######################################################Configuración del registro de eventos
logger = logging.getLogger("transform_extractor")
file_handler = logging.FileHandler("logs_main_py.txt")
file_handler.setFormatter(logging.Formatter("[%(levelname)s] [%(asctime)s] [%(name)s]: %(message)s"))
logger.addHandler(file_handler)
logging.StreamHandler()

######Conexión al bucket de Cloud Storage y descarga de los archivos para convertirlo es df
def donwload_data_datalake(bucket_name, blob_name):
    try:
        #Conexión y solicitud de datos con Cloud Storage
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        content = blob.download_as_text()
        logger.info(f"Archivo {blob_name} descargado de Google Cloud Storage con éxito")
        #Creación del Dataframe
        df = pd.read_json(content)
        logger.info(f"Archivo {blob_name} convertido a df con éxito")
        return df
    #Captura de las excepciones
    except Exception as e:
        logger.error(f"Error en el procesamiento del archivo {blob_name} de Cloud Storage: {e}")
    except storage.exceptions.GoogleCloudError as e:
        logger.critical(f"Error al descargar el archivo {blob_name} de Cloud Storage: {e}")
    finally:
        #Cerrar la conexión a Google Cloud Storage
        storage_client.close()

###########################################################################Función principal
def data_datalake(bucket_name, blob_name_1, blob_name_2):
    t1 = time.time()  
    #Creación de DataFrame Proyectos para su posterior tranformación
    df_proyectos = donwload_data_datalake(bucket_name, blob_name_1)
    columns_proyectos = df_proyectos.columns.values
    size_proyectos = df_proyectos.shape
    #Creación de DataFrame Contratos para su posterior tranformación
    df_contratos = donwload_data_datalake(bucket_name, blob_name_2)
    columns_contratos = df_contratos.columns.values
    size_contratos = df_contratos.shape
    #Registro de tiempo
    t2 = time.time()
    t2 = np.round(t2-t1)
    logger.info(f"Demoré {t2} segundos en extraer desde la GCS")
    return [df_proyectos, df_contratos,columns_proyectos,size_proyectos,columns_contratos,size_contratos, t2]

   
    