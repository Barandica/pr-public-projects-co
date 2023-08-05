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
        logging.FileHandler("transform\logs\modeler_proyectos.log.txt"),  
    ]
)

#Manejo de excepciones y logs
def manejar_excepciones(file_name, exception):
    #Creación de  un objeto logger para el registro
    logger = logging.getLogger("Modeler proyecto")  
    logger.error(f"Error en el archivo {file_name}: {exception}")
    #Subir el log a Cloud Storage
    log_bucket_name = "logs-public-projects"  
    log_blob_name = f"transform_error.log" 
    storage_client = storage.Client()
    log_bucket = storage_client.bucket(log_bucket_name)
    log_blob = log_bucket.blob(log_blob_name)
    log_blob.upload_from_string(f"Error en el archivo {file_name}: {exception}")

 #Función que construye las dimensiones 
def build_dimension(df, column_name, dimension_name): 
    # Extraer la columna única para la dimensión y eliminar duplicados
    dim_data = df[[column_name]].drop_duplicates(subset=column_name)
    dim_data.reset_index(drop=True, inplace=True)    
     # Agregar una columna de identificación única a la dimensión
    dim_data['id_{}'.format(dimension_name)] = dim_data.index.astype(int) + 1  
    return dim_data


#Creación de dimensiones de la tabla df_datos_basicos
def dim_models(df):
    #Creación de  un objeto logger para el registro
    logger = logging.getLogger("Modeler proyecto")  
    try:
        # Construcción de la dimensión 'dim_sector'
        dim_tipo_doc = build_dimension(df, 'tipodocproveedor', 'tipo_doc')
        # Construcción de la dimensión 'dim_proveedor'
        dim_prove = df[['tipodocproveedor','documentoproveedor','proveedor']].drop_duplicates(subset='documentoproveedor')
        dim_prove["id_tipo_doc"] = dim_prove["tipodocproveedor"].map(dim_tipo_doc.set_index("tipodocproveedor")["id_tipo_doc"])
        dim_prove = dim_prove[['id_tipo_doc', 'documentoproveedor', 'proveedor' ]]
        # Construcción de la dimensión 'dim_estado_contrato'
        dim_estado_contrato = build_dimension(df, 'estadocontrato', 'estado_contrato')
        logger.info("Se han construido las dimensiones de los contratos con éxito")
        return [dim_tipo_doc, dim_prove, dim_estado_contrato]
    except pd.errors.PandasError as e:
        manejar_excepciones(e)
        logger.error(f"Error al crear las dimensiones: {e}")
    except Exception as e:
        # Manejo de otras excepciones que no sean específicas de Pandas
        manejar_excepciones(e)
        logger.error(f"Error al crear las dimensiones: {e}")
            
#Contrucción de la tabla de hechos de proyectos
def fact_models(df, dim_estado_contrato):
    #Creación de  un objeto logger para el registro
    logger = logging.getLogger("Modeler proyecto")
    try:
        df["id_estado_contrato"] = df["estadocontrato"].map(dim_estado_contrato.set_index("estadocontrato")["id_estado_contrato"])
        
        columns = ['bpin', 'referenciacontrato', 'documentoproveedor', 'id_estado_contrato', 'valorcontrato', 'urlproceso',\
                   'vigenciacontrato', 'objetodelcontrato' ] 
        #Creación de tabla de hechos
        fact_contratos = df[columns]
        logger.info("Se ha construido la tablas fact_contratos")   
        return fact_contratos
    except pd.errors.PandasError as e:
        manejar_excepciones(e)
        logger.error(f"Error crear los hechos: {e}")
    except Exception as e:
        # Manejo de otras excepciones que no sean específicas de Pandas
        manejar_excepciones(e)
        logger.error(f"Error al crear los hechos: {e}")
    
#Función principal
def models_contratos(df):
    #Manipulación de tabla df_datos_basicos
    [dim_tipo_doc, dim_prove, dim_estado_contrato] = dim_models(df)
    fact_contratos = fact_models(df, dim_estado_contrato)
    
    return [dim_tipo_doc, dim_prove, dim_estado_contrato,fact_contratos]
