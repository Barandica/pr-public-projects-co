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
        logging.FileHandler("transform\logs\quality.log.txt"),  
    ]
)

#Manejo de excepciones y logs
def manejar_excepciones(file_name, exception):
    #Creación de  un objeto logger para el registro
    logger = logging.getLogger("Quality")  
    logger.error(f"Error en el archivo {file_name}: {exception}")
    #Subir el log a Cloud Storage
    log_bucket_name = "logs-public-projects"  
    log_blob_name = f"transform_error.log" 
    storage_client = storage.Client()
    log_bucket = storage_client.bucket(log_bucket_name)
    log_blob = log_bucket.blob(log_blob_name)
    log_blob.upload_from_string(f"Error en el archivo {file_name}: {exception}")

#Validación datos_basicos
def verificar_columna_unica(df, columna):
    #Creación un objeto logger para el registro
    logger = logging.getLogger("Quality")
    if df[columna].nunique() == len(df):
        logger.info(f"La columna {columna} tiene únicos")
    else:
        logger.error(f"La columna {columna} tiene duplicados, no se puede continuar")
        raise ValueError(f"La columna {columna} no tiene valores únicos. Detener la ejecución.")

#Función de ejecución    
def quality_validator(df_datos_basicos):
    try:
        verificar_columna_unica(df_datos_basicos, 'bpin')
        return df_datos_basicos
    except ValueError as e:
        print(f"Error: {e}")
        return None
    


    ##############################################################

    import pandas as pd
from google.cloud import bigquery

# Supongamos que ya tienes los DataFrames dim_tipo_doc, dim_prove, dim_estado_contrato, fact_contratos listos

# Función para cargar datos en BigQuery
def load_to_bigquery(dataframe, table_id):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        # Otros parámetros de configuración según tus necesidades
    )
    client.load_table_from_dataframe(dataframe, table_id, job_config=job_config).result()

# Función para ejecutar controles de calidad
def execute_data_quality_checks():
    # Control de datos faltantes
    if any(dim_tipo_doc.isnull().values.flatten()):
        raise ValueError("Se encontraron datos faltantes en el DataFrame dim_tipo_doc.")

    if any(dim_prove.isnull().values.flatten()):
        raise ValueError("Se encontraron datos faltantes en el DataFrame dim_prove.")

    if any(dim_estado_contrato.isnull().values.flatten()):
        raise ValueError("Se encontraron datos faltantes en el DataFrame dim_estado_contrato.")

    if any(fact_contratos.isnull().values.flatten()):
        raise ValueError("Se encontraron datos faltantes en el DataFrame fact_contratos.")

    # Otros controles de calidad según tus necesidades, por ejemplo:
    # Control de valores negativos en montos
    if any(fact_contratos['monto'] < 0):
        raise ValueError("Se encontraron montos negativos en el DataFrame fact_contratos.")

    # Control de duplicados en tablas
    if dim_tipo_doc.duplicated().any():
        raise ValueError("Se encontraron registros duplicados en el DataFrame dim_tipo_doc.")

    if dim_prove.duplicated().any():
        raise ValueError("Se encontraron registros duplicados en el DataFrame dim_prove.")

    if dim_estado_contrato.duplicated().any():
        raise ValueError("Se encontraron registros duplicados en el DataFrame dim_estado_contrato.")

    if fact_contratos.duplicated().any():
        raise ValueError("Se encontraron registros duplicados en el DataFrame fact_contratos.")

# Ejecutar controles de calidad antes de cargar los datos en BigQuery
try:
    execute_data_quality_checks()
    load_to_bigquery(dim_tipo_doc, 'proyecto.dataset.dim_tipo_doc')
    load_to_bigquery(dim_prove, 'proyecto.dataset.dim_prove')
    load_to_bigquery(dim_estado_contrato, 'proyecto.dataset.dim_estado_contrato')
    load_to_bigquery(fact_contratos, 'proyecto.dataset.fact_contratos')
except ValueError as e:
    print(f"Error en los controles de calidad: {str(e)}")

    