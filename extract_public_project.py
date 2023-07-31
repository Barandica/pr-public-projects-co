#Importación de librerías
import requests
import logging
from google.cloud import storage

# Configuración del registro de eventos
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] [%(asctime)s] [%(name)s]: %(message)s",
    handlers=[
        #Salida en consola
        logging.StreamHandler(),  
        #Registro en un archivo de texto
        logging.FileHandler("extract_logs.txt")  
    ]
)

def extraer_y_subir_datos(api_url, blob_name):
    #Creación de un objeto logger para el registro
    logger = logging.getLogger("extract")  

    try:
        #Request y obtención de datos de la API
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Se ha obtenido los datos de la API: {api_url}")
        #Carga de datos al bucket en cloud storage
        bucket_name = "raw-public-projects"
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(str(data), content_type="application/json")
        logger.info(f"Archivo {blob_name} cargado en Cloud Storage con éxito.")
        #Excepciones de request y carga
    except requests.exceptions.RequestException as e:
        logger.error(f"Error al obtener los datos de la API {api_url}: {e}")
    except Exception as e:
        logger.error(f"Error al cargar el archivo de la API {api_url} en Cloud Storage: {e}")

if __name__ == "__main__":
    # API 1
    api_url_1 = "https://www.datos.gov.co/resource/cf9k-55fw.json" # Datos básicos
    blob_name_1 = "datos_basicos.json"
    extraer_y_subir_datos(api_url_1, blob_name_1)

    # API 2
    api_url_2 = "https://www.datos.gov.co/resource/uwns-mbwd.json"  # URL de la segunda API
    blob_name_2 = "datos_contratos.json"  # Nombre para el archivo de la API 2
    extraer_y_subir_datos(api_url_2, blob_name_2)
