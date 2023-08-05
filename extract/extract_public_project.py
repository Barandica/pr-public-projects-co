#Importación de librerías
import requests
import logging
from google.cloud import storage
import json

#Configuración del registro de eventos
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] [%(asctime)s] [%(name)s]: %(message)s",
    handlers=[
        #Salida en consola
        logging.StreamHandler(), 
        #Salida en archivo local
        logging.FileHandler("extract\logs\extract_log.txt"), 
    ]
)

#Función que maneja las excepciones
def manejar_excepciones(api_url, blob_name, exception):
    #Creación de  un objeto logger para el registro
    logger = logging.getLogger("Extract")  
    logger.error(f"Error en la API {api_url}: {exception}")
    #Subir el log a Cloud Storage
    log_bucket_name = "logs-public-projects"  
    log_blob_name = f"extract_error.log" 
    storage_client = storage.Client()
    log_bucket = storage_client.bucket(log_bucket_name)
    log_blob = log_bucket.blob(log_blob_name)
    log_blob.upload_from_string(f"Error en la API {api_url}: {exception}")

#Función que solicita datos a la API y las sube a Cloud Storage
def extraer_y_subir_datos(api_url, blob_name):
    #Creación un objeto logger para el registro
    logger = logging.getLogger("Extract")  

    try:
        #Request de datos a la API
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
        data = json.dumps(data, indent=4)
        logger.info(f"Se ha obtenido los datos de la API: {api_url}")
        #Carga de datos en el bucket
        bucket_name = "raw-public-projects"
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(str(data), content_type="application/json")
        logger.info(f"Archivo {blob_name} cargado en Cloud Storage con éxito")
        #Manejo de excepciones
    except requests.exceptions.RequestException as e:
        manejar_excepciones(api_url, blob_name, e)
        logger.error(f"Error al cargar el archivo de la API {api_url} en Cloud Storage: {e}")
    except Exception as e:
        logger.error(f"Error al cargar el archivo de la API {api_url} en Cloud Storage: {e}")
        manejar_excepciones(api_url, blob_name, e)
    
#Función principal que ejecuta la anterior función
def extract():
    #datos_basicos
    api_url_1 = "https://www.datos.gov.co/resource/cf9k-55fw.json"
    blob_name_1 = "datos_basicos.json"
    extraer_y_subir_datos(api_url_1, blob_name_1)
    #datos_contratos
    api_url_2 = "https://www.datos.gov.co/resource/uwns-mbwd.json"  
    blob_name_2 = "datos_contratos.json"  
    extraer_y_subir_datos(api_url_2, blob_name_2)
    return