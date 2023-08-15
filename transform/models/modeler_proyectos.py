####################################################################Importación de librerias
import pandas as pd
import logging
import numpy as np
import time as time

#######################################################Configuración del registro de eventos
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] [%(asctime)s] [%(name)s]: %(message)s",
    handlers=[
        #Salida en consola
        logging.StreamHandler(), 
        #Salida en archivo local
        logging.FileHandler("transform\logs\modeler_proyectos.txt"),  
    ])

#####################################################Función que construye las dimensiones 
def build_dimension(df, column_name, dimension_name): 
    # Extraer la columna única para la dimensión y eliminar duplicados
    dim_data = df[[column_name]].drop_duplicates(subset=column_name)
    dim_data.reset_index(drop=True, inplace=True)    
     # Agregar una columna de identificación única a la dimensión
    dim_data['id_{}'.format(dimension_name)] = dim_data.index.astype(int) + 1  
    return dim_data

###########################################Creación de dimensiones de la tabla df_proyectos
def dim_submodels(df):
    #Creación de  un objeto logger para el registro
    logger = logging.getLogger("modeler_subdimensiones_proyectos")  
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
        # Construcción de la dimensión 'dim_subestado'
        dim_subestado = build_dimension(df, 'subestadoproyecto', 'subestado')
        logger.info("Se han construido las dimensiones de los proyectos con éxito")
        return [dim_proyectos_cer, dim_sector, dim_estado_proyecto, dim_tipo_proyecto, dim_subestado]
    except pd.errors.PandasError as e:
        logger.error(f"Error crear las dimensiones: {e}")
    except Exception as e:
        logger.error(f"Error al crear las dimensiones: {e}")
            
##############################################Contrucción de la tabla dimensional proyectos
def dim_models_proyectos(df, dim_sector, dim_estado_proyecto, dim_tipo_proyecto, dim_subestado):
    #Creación de  un objeto logger para el registro
    logger = logging.getLogger("modeler_dimension_proyectos")
    try:
        df["id_sector"] = df["sector"].map(dim_sector.set_index("sector")["id_sector"])
        df["id_estado_proyecto"] = df["estadoproyecto"].map(dim_estado_proyecto.set_index("estadoproyecto")["id_estado_proyecto"])
        df["id_tipo_proyecto"] = df["tipoproyecto"].map(dim_tipo_proyecto.set_index("tipoproyecto")["id_tipo_proyecto"])
        df["id_subestado"] = df["subestadoproyecto"].map(dim_subestado.set_index("subestadoproyecto")["id_subestado"])

        columns = ['bpin', 'nombreproyecto', 'objetivogeneral', 'id_estado_proyecto', 'id_subestado', 'ano_ini', 'ano_fin',\
               'id_sector', 'id_tipo_proyecto', 'codigoentidadresponsable', 'programapresupuestal',  \
                'valortotalproyecto','valorvigenteproyecto', 'valorobligacionproyecto', 'valorpagoproyecto'] 
        #Creación de tabla de hechos
        dim_proyectos = df[columns] 
        logger.info("Se ha construido la tablas dim_proyectos")   
        return dim_proyectos, True
    except pd.errors.PandasError as e:
        logger.error(f"Error crear los hechos: {e}")
    except Exception as e:
        logger.error(f"Error al crear los hechos: {e}")
    
##########################################################################Función principal
def models_proyecto(df):
    t1 = time.time()
    #Creación de la subdimensiones
    [dim_proyectos_cer, dim_sector, dim_estado_proyecto, dim_tipo_proyecto, dim_subestado]\
          = dim_submodels(df)
    #Creacion de la dimension
    dim_proyectos, bool = dim_models_proyectos(df, dim_sector, dim_estado_proyecto, dim_tipo_proyecto, dim_subestado)
    t2 = time.time()
    t2 = np.round(t2-t1)
    print(f"Demoré {t2} segundos en modelar el df_proyectos")    
    return [dim_proyectos_cer, dim_sector, dim_estado_proyecto, dim_tipo_proyecto, dim_subestado, dim_proyectos, bool, t2]
