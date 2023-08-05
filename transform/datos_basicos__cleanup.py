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
        logging.FileHandler("transform\logs\clean_datos_basicos.log.txt"),  
    ]
)

#Manejo de excepciones y logs
def manejar_excepciones(file_name, exception):
    #Creación de  un objeto logger para el registro
    logger = logging.getLogger("CleanUp1")  
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
    logger = logging.getLogger("CleanUp1")  
    try:
        #Columnas de nuestro df y el tipo de dato nuevo
        columns = {
        "bpin" : str,
        "nombreproyecto" : object,
        "objetivogeneral" : object, 
        "estadoproyecto" : str,
        "horizonte" : str, 
        "sector" : str,
        "entidadresponsable" : object,
        "programapresupuestal" : object,
        "tipoproyecto" : str,
        "plandesarrollonacional" : str,
        "valortotalproyecto" : float,
        "valorvigenteproyecto" : float,
        "valorobligacionproyecto" : float,
        "valorpagoproyecto" : float,
        "subestadoproyecto" : str,
        "codigoentidadresponsable" : str, 
        "ano_ini" : int,
        "ano_fin" : int
        } 
        #Separar los años de la columna horizonte
        df[["ano_ini", "ano_fin"]] = df["horizonte"].str.extract(r"(\d+)-(\d+)")
        #Cambiar el tipo de dato de nuestras columnas
        df = df.astype(columns)   
        #Trabajo con nulos
        df["entidadresponsable"].fillna("SIN ENTIDAD", inplace=True)
        df["codigoentidadresponsable"].fillna("SIN CODIGO", inplace=True)
        df["plandesarrollonacional"].fillna("SIN PLAN", inplace=True)   
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
def datos_basicos_cleanup(df):
    #Limpiamos la data
    df = cleanup(df)
    return df