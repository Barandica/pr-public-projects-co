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
        logging.FileHandler("transform\logs\clean_datos_contratos.log.txt"),  
    ]
)

#Manejo de excepciones y logs
def manejar_excepciones(file_name, exception):
    #Creación de  un objeto logger para el registro
    logger = logging.getLogger("CleanUp2")  
    logger.error(f"Error en el archivo {file_name}: {exception}")
    #Subir el log a Cloud Storage
    log_bucket_name = "logs-public-projects"  
    log_blob_name = f"transform_error.log" 
    storage_client = storage.Client()
    log_bucket = storage_client.bucket(log_bucket_name)
    log_blob = log_bucket.blob(log_blob_name)
    log_blob.upload_from_string(f"Error en el archivo {file_name}: {exception}")

#Limpieza de los datos
def cleanup(df):
    #Creación de  un objeto logger para el registro
    logger = logging.getLogger("CleanUp2")  
    try:
        #Columnas de nuestro df y el tipo de dato nuevo
        columns = {
        "bpin" : str,
        "tipodocproveedor" : str,
        "documentoproveedor" : str,
        "proveedor" : object,
        "estadocontrato" : str,
        "referenciacontrato" : str,
        "valorcontrato" : float,
        "urlproceso" : object,
        "vigenciacontrato" : int,
        "objetodelcontrato" : object
        } 
        # Filtramos las columnas que están presentes en el diccionario
        df_columns = [col for col in df if col in columns]    
        # Creamos un nuevo DataFrame con las columnas seleccionadas
        df = df[df_columns]
        #Cambiar el tipo de dato de nuestras columnas
        df = df.astype(columns)     
        logger.info(f"El archivo se ha limpiado con éxito")
        return df
    except pd.errors.PandasError as e:
        manejar_excepciones(e)
        logger.error(f"Error al limpiar el df: {e}")
    except Exception as e:
        # Manejo de otras excepciones que no sean específicas de Pandas
        manejar_excepciones(e)
        logger.error(f"Error al limpiar el df: {e}")

#Función ejecutora
def datos_contratos_cleanup(df):
    #Limpiamos la data
    df = cleanup(df)
    return df