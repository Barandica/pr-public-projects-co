######################################################################Importación de librerías
import requests
import logging
from google.cloud import storage
import json
import concurrent.futures
import pandas as pd
import time as time
import numpy as np

#########################################################Configuración del registro de eventos
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] [%(asctime)s] [%(name)s]: %(message)s",
    handlers=[
        #Salida en consola
        logging.StreamHandler(), 
        #Salida en archivo local
        logging.FileHandler("extract\logs\extract_log.txt"), 
    ])

#################Función que carga los datos extraidos desde la API hasta Google Cloud Storage
def cargar_datos(data,blob_name, bucket_name):
    #Creación un objeto logger para el registro
    logger = logging.getLogger("extract_load")
    try:
        #Carga de datos en el bucket
        #bucket_name = "raw-public-projects"
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(str(data), content_type="application/json")
        logger.info(f"Archivo {blob_name} cargado en Cloud Storage con éxito")
        return True
    except storage.exceptions.GoogleCloudError as e:
        logger.critical(f"Error al cargar el archivo {blob_name} en Cloud Storage: {e}")
        return False
    except Exception as e:
        logger.error(f"Error en el procesamiento de carga en Cloud Storage {e}")
        return False

#######################################################Función que solicita los datos a la API
def extraer_datos(api_url, page_size=1000):
    #Creación un objeto logger para el registro
    logger = logging.getLogger("extract_extract")
    try:
        #Creamos una lista donde agregaremos la data de las distintas páginas que vayamos solicitando de la API
        data = []
        page = 1

        #Solicitud de datos por páginas y en paralelo
        while True:
            params = {'$limit': page_size, '$offset': (page - 1) * page_size}
            response = requests.get(api_url, params=params)
            response.raise_for_status()
            page_data = response.json()
            if not page_data:
            #if len(data) >= 1000:
                break  # Si no hay más datos, sal del bucle
            #Agregamos la data en la lista
            data.extend(page_data)
            page += 1
        #Creamos un df
        df = pd.DataFrame(data)
        columns = df.columns.values
        size = df.shape
        #Creamos un .JSON para subir al GCS 
        data = json.dumps(data, indent=4)        
        logger.info("Se han obtenido correctamente los datos desde la API")
        return data, columns, size
        #Manejo de excepciones
    except json.JSONDecodeError as e:
        logger.critical(f"Error al solicitar los datos: {e}")
    except Exception as e:
        logger.error(f"Error al procesar los datos: {e}")

    

##########################################################################Función principal
def extraer_subir_datos_gcs(bucket_name, blob_name_1, blob_name_2, api_url_1, api_url_2):
    t1 = time.time()
    #Request de datos con paralelismo
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_1 = executor.submit(extraer_datos, api_url_1)
        future_2 = executor.submit(extraer_datos, api_url_2)
    data_proyectos, columns_proyectos, size_proyectos = future_1.result()
    data_contratos, columns_contratos, size_contratos = future_2.result()
    #Subir datos a Google Cloud Storage
    bool_1 = cargar_datos(data_proyectos,blob_name_1, bucket_name)
    bool_2 = cargar_datos(data_contratos,blob_name_2, bucket_name)
    if bool_1 == True & bool_2 == True:
        bool == True
    else:
        raise Exception("No se pudo correr el codigo")
    t2 = time.time()
    t2 = np.round(t2-t1)
    print(f"Demoré {t2} segundos en extraer desde la API")
    return [columns_proyectos, size_proyectos, columns_contratos, size_contratos, bool, t2]
