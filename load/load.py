####################################################################Importación de librerias
import pandas as pd
from google.cloud import bigquery
import logging
import numpy as np
import time as time

######################################################Configuración del registro de eventos
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] [%(asctime)s] [%(name)s]: %(message)s",
    handlers=[
        #Salida en consola
        logging.StreamHandler(), 
        #Salida en archivo local
        logging.FileHandler("load\logs\load.txt"),  
    ])

########################################Crea la conexion a BigQuery y carga las tablas allí
def cargar_datos_en_bigquery(dataframe,  dataset, tabla):
    #Creación de  un objeto logger para el registro
    logger = logging.getLogger("load_BigQuery")  
    try:
        #Configura el cliente de BigQuery
        client = bigquery.Client()
        #Especifica la tabla destino en BigQuery
        table_ref = client.dataset(dataset).table(tabla)
        #Crea la tabla si no existe (opcional)
        table = bigquery.Table(table_ref)
        try:
            table = client.create_table(table)
        except:
            pass
        #Forma el nombre completo de la tabla en el formato correcto
        full_table_name = f"{client.project}.{dataset}.{tabla}"
        # Carga el DataFrame en BigQuery
        dataframe.to_gbq(destination_table=full_table_name, project_id=client.project, if_exists='replace')
        logger.info(f"Datos de la tabla {tabla} cargados correctamente en BigQuery")
        return True
    except pd.errors.PandasError as e:
        logger.critical(f"Error al procesar la carga: {e}")
    except bigquery.exceptions.GoogleCloudError as e:
        logger.critical(f"Error al crear la conexión con BigQuery: {e}")
    except Exception as e:
        logger.error(f"Error al cargar los datos a BigQuery: {e}")

##########################################################################Función principal
def load_bigquery(dataset, dim_proyectos_cer,dim_sector, dim_estado_proyecto, dim_tipo_proyecto, dim_subestado, dim_proyectos,
                dim_tipo_doc, dim_prove, dim_estado_contrato,fact_contratos, df_inconsistencias_pr, df_inconsistencias_ct):
    t1 = time.time()
    #Define una lista con todas las tablas a poblar y sus nombres
    tables = [
        ("dim_pr_entidades", dim_proyectos_cer),
        ("dim_pr_sector", dim_sector),
        ("dim_pr_estado", dim_estado_proyecto),
        ("dim_pr_tipo", dim_tipo_proyecto),
        ("dim_pr_subestado", dim_subestado),
        ("dim_pr_proyecto", dim_proyectos),
        ("dim_ct_tipo_doc", dim_tipo_doc),
        ("dim_ct_proveedor", dim_prove),
        ("dim_ct_estado", dim_estado_contrato),
        ("fact_contratos", fact_contratos),
        ("errores_proyectos", df_inconsistencias_pr),
        ("errores_contratos", df_inconsistencias_ct)
    ]
    #Carga los datos en BigQuery para cada tabla
    for table_name, dataframe in tables:
        cargar_datos_en_bigquery(dataframe, dataset, table_name)
    t2 = time.time()
    t2 = np.round(t2-t1)
    print(f"Demoré {t2} segundos en subir los datos a BigQuery")  
    return t2

