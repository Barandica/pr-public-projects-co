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
        #Construcción de dim de codigo entidad responsable
        columns_proyectos_cer = ['codigoentidadresponsable','entidadresponsable']
        dim_proyectos_cer = df[columns_proyectos_cer]
        dim_proyectos_cer = dim_proyectos_cer.drop_duplicates(subset='codigoentidadresponsable')
        dim_proyectos_cer = dim_proyectos_cer.reset_index(drop=True)

        # Construcción de la dimensión 'dim_sector'
        dim_sector = build_dimension(df, 'sector', 'sector')
        # Construcción de la dimensión 'dim_estado_proyecto'
        dim_estado_proyecto = build_dimension(df, 'estadoproyecto', 'estado_proyecto')
        # Construcción de la dimensión 'dim_tipo_proyecto'
        dim_tipo_proyecto = build_dimension(df, 'tipoproyecto', 'tipo_proyecto')
        # Construcción de la dimensión 'dim_plan'
        dim_plan = build_dimension(df, 'plandesarrollonacional', 'plan_nal')
        # Construcción de la dimensión 'dim_subestado'
        dim_subestado = build_dimension(df, 'subestadoproyecto', 'subestado')
        logger.info("Se han construido las dimensiones de los proyectos con éxito")
        return [dim_proyectos_cer, dim_sector, dim_estado_proyecto, dim_tipo_proyecto, dim_plan, dim_subestado]
    except pd.errors.PandasError as e:
        manejar_excepciones(e)
        logger.error(f"Error crear las dimensiones: {e}")
    except Exception as e:
        # Manejo de otras excepciones que no sean específicas de Pandas
        manejar_excepciones(e)
        logger.error(f"Error al crear las dimensiones: {e}")
            
#Contrucción de la tabla de hechos de proyectos
def fact_models(df, dim_sector, dim_estado_proyecto, dim_tipo_proyecto,dim_plan,dim_subestado):
    #Creación de  un objeto logger para el registro
    logger = logging.getLogger("Modeler proyecto")
    try:
        df["id_sector"] = df["sector"].map(dim_sector.set_index("sector")["id_sector"])
        df["id_estado_proyecto"] = df["estadoproyecto"].map(dim_estado_proyecto.set_index("estadoproyecto")["id_estado_proyecto"])
        df["id_tipo_proyecto"] = df["tipoproyecto"].map(dim_tipo_proyecto.set_index("tipoproyecto")["id_tipo_proyecto"])
        df["id_plan_nal"] = df["plandesarrollonacional"].map(dim_plan.set_index("plandesarrollonacional")["id_plan_nal"])
        df["id_subestado"] = df["subestadoproyecto"].map(dim_subestado.set_index("subestadoproyecto")["id_subestado"])

        columns = ['bpin', 'nombreproyecto', 'objetivogeneral', 'id_estado_proyecto', 'id_subestado', 'ano_ini', 'ano_fin',\
               'id_sector', 'id_tipo_proyecto', 'id_plan_nal', 'codigoentidadresponsable', 'programapresupuestal',  \
                'valortotalproyecto','valorvigenteproyecto', 'valorobligacionproyecto', 'valorpagoproyecto'] 
        #Creación de tabla de hechos
        fact_proyectos = df[columns] 
        logger.info("Se ha construido la tablas fact_proyectos")   
        return fact_proyectos
    except pd.errors.PandasError as e:
        manejar_excepciones(e)
        logger.error(f"Error crear los hechos: {e}")
    except Exception as e:
        # Manejo de otras excepciones que no sean específicas de Pandas
        manejar_excepciones(e)
        logger.error(f"Error al crear los hechos: {e}")
    
#Función principal
def models_proyecto(df):
    #Manipulación de tabla df_datos_basicos
    [dim_proyectos_cer, dim_sector, dim_estado_proyecto, dim_tipo_proyecto, dim_plan, dim_subestado]\
          = dim_models(df)
    fact_proyecto = fact_models(df, dim_sector, dim_estado_proyecto, dim_tipo_proyecto, dim_plan, dim_subestado)
    
    return [dim_proyectos_cer, dim_sector, dim_estado_proyecto, dim_tipo_proyecto, dim_plan, dim_subestado, fact_proyecto]
