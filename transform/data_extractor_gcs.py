####################################################################Importación de librerias
from google.cloud import storage
import pandas as pd
import logging
import time as time
import numpy as np

######################################################Configuración del registro de eventos
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] [%(asctime)s] [%(name)s]: %(message)s",
    handlers=[
        #Salida en consola
        logging.StreamHandler(),  
        #Salida en archivo local
        logging.FileHandler("transform\logs\extractor.log.txt"), 
    ])

######Conexión al bucket de Cloud Storage y descarga de los archivos para convertirlo es df
def donwload_data_datalake(bucket_name, blob_name):
    #Creación un objeto logger para el registro
    logger = logging.getLogger("transform_extractor") 
    try:
        #Conexión y solicitud de datos con Cloud Storage
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        content = blob.download_as_text()
        logger.info(f"Archivo {blob_name} descargado de Cloud Storage con éxito")
        #Creación del Dataframe
        df = pd.read_json(content)
        logger.info(f"Archivo {blob_name} convertido a df")
        return df, True
    #Captura de las excepciones
    except pd.errors.PandasError as e:
        logger.error(f"Error al convertir el archivo {blob_name}: {e}")
        return False
    except Exception as e:
        logger.error(f"Error en el procesamiento del archivo {blob_name} de Cloud Storage: {e}")
        return False
    except storage.exceptions.GoogleCloudError as e:
        logger.critical(f"Error al descargar el archivo {blob_name} de Cloud Storage: {e}")
        return False

###########################################################################Función principal
def data_datalake(bucket_name, blob_name_1, blob_name_2):
    t1 = time.time()
    #Creación de DataFrame para su posterior tranformación
    df_proyectos, bool_1 = donwload_data_datalake(bucket_name, blob_name_1)
    columns_proyectos = df_proyectos.columns.values
    size_proyectos = df_proyectos.shape
    df_contratos, bool_2 = donwload_data_datalake(bucket_name, blob_name_2)
    columns_contratos = df_contratos.columns.values
    size_contratos = df_contratos.shape
    if bool_1 == True & bool_2 == True:
        bool == True
    else:
        raise Exception("No se pudo correr el codigo")
    t2 = time.time()
    t2 = np.round(t2-t1)
    print(f"Demoré {t2} segundos en extraer desde la GCS")
    return [df_proyectos, df_contratos,columns_proyectos,size_proyectos,columns_contratos,size_contratos,bool, t2]

   
    