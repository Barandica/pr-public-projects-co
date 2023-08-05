from google.cloud import storage
import pandas as pd
import logging

#Configuración del registro de eventos
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] [%(asctime)s] [%(name)s]: %(message)s",
    handlers=[
        #Salida en consola
        logging.StreamHandler(),  
        #Salida en archivo local
        logging.FileHandler("transform\logs\extractor.log.txt"), 
    ]
)

#Manejo de excepciones y logs
def manejar_excepciones(file_name, exception):
    #Creación de  un objeto logger para el registro
    logger = logging.getLogger("Extractor")  
    logger.error(f"Error en el archivo {file_name}: {exception}")
    #Subir el log a Cloud Storage
    log_bucket_name = "logs-public-projects"  
    log_blob_name = f"extractor.log" 
    storage_client = storage.Client()
    log_bucket = storage_client.bucket(log_bucket_name)
    log_blob = log_bucket.blob(log_blob_name)
    log_blob.upload_from_string(f"Error en el archivo {file_name}: {exception}")

#Conexión al bucket de Cloud Storage y descarga de los archivo para convertirlo es df
def donwload_data_datalake(bucket_name, file_name):
    #Creación un objeto logger para el registro
    logger = logging.getLogger("Extractor") 
    try:
        #Conexión y solicitud de datos con Cloud Storage
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(file_name)
        content = blob.download_as_text()
        logger.info(f"Archivo {file_name} descargado de Cloud Storage con éxito")
        #Creación del Dataframe
        df = pd.read_json(content)
        logger.info(f"Archivo {file_name} convertido a df")
    #Captura de las excepciones
    except pd.errors.PandasError as e:
        logger.error(f"Error al convertir el archivo {file_name}: {e}")
        manejar_excepciones(file_name, e)
    except Exception as e:
        logger.error(f"Error al descargar el archivo {file_name} de Cloud Storage: {e}")
        manejar_excepciones(file_name, e)
    return df

def data_datalake():
    #Información de los archivos dentro del bucket
    bucket_name = 'raw-public-projects'
    file_name_1 = 'datos_basicos.json'
    file_name_2 = 'datos_contratos.json'
    #Creación de DataFrame para su posterior tranformación
    df_datos_basicos = donwload_data_datalake(bucket_name, file_name_1)
    df_datos_contratos = donwload_data_datalake(bucket_name, file_name_2)
    return [df_datos_basicos, df_datos_contratos]

   
    