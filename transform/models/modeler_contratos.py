####################################################################Importación de librerias
import pandas as pd
import logging
import numpy as np
import time as time

######################################################Configuración del registro de eventos
logger = logging.getLogger("modeler_df_contratos")
file_handler = logging.FileHandler("logs_main_py.txt")
file_handler.setFormatter(logging.Formatter("[%(levelname)s] [%(asctime)s] [%(name)s]: %(message)s"))
logger.addHandler(file_handler)
logging.StreamHandler()

#####################################################Función que construye las dimensiones 
def build_dimension(df, column_name, dimension_name): 
    # Extraer la columna única para la dimensión y eliminar duplicados
    dim_data = df[[column_name]].drop_duplicates(subset=column_name)
    dim_data.reset_index(drop=True, inplace=True)    
     # Agregar una columna de identificación única a la dimensión
    dim_data['id_{}'.format(dimension_name)] = dim_data.index.astype(int) + 1  
    return dim_data

######################################Creación de dimensiones de la tabla df_datos_basicos
def dim_models(df):  
    try:
        # Construcción de la dimensión 'dim_estado_contrato'
        dim_estado_contrato = build_dimension(df, 'estadocontrato', 'estado_contrato')
        # Construcción de la dimensión 'dim_tip_doc'
        dim_tipo_doc = build_dimension(df, 'tipodocproveedor', 'tipo_doc')
        # Construcción de la dimensión 'dim_proveedor'
        dim_prove = df[['tipodocproveedor','documentoproveedor','proveedor']].drop_duplicates(subset='documentoproveedor')
        dim_prove.reset_index(drop=True, inplace=True)
        dim_prove['id_proveedor'] = dim_prove.index.astype(int) + 1 
        #dim_prove = build_dimension(df,'documentoproveedor', 'documentoproveedor')
        dim_prove["id_tipo_doc"] = dim_prove["tipodocproveedor"].map(dim_tipo_doc.set_index("tipodocproveedor")["id_tipo_doc"])
        dim_prove = dim_prove[['id_proveedor', 'id_tipo_doc', 'documentoproveedor', 'proveedor' ]]       
        logger.info("Se han construido las dimensiones del df_contratos con éxito")
        return [dim_tipo_doc, dim_prove, dim_estado_contrato]
    except Exception as e:
        logger.error(f"Error al crear las dimensiones del df_contratos: {e}")
            
#############################################Contrucción de la tabla de hechos de proyectos
def fact_models_contratos(df, dim_estado_contrato):
    try:
        df["id_estado_contrato"] = df["estadocontrato"].map(dim_estado_contrato.set_index("estadocontrato")["id_estado_contrato"])
        
        columns = ['bpin', 'documentoproveedor', 'id_estado_contrato', 'valorcontrato', 'urlproceso', 'vigenciacontrato', 'objetodelcontrato' ] 
        #Creación de tabla de hechos
        fact_contratos = df[columns]
        logger.info("Se ha construido fact_contratos con éxito")   
        return fact_contratos
    except Exception as e:
        logger.error(f"Error al crear fact_contratos: {e}")
    
##########################################################################Función principal
def models_contratos(df):
    t1 = time.time()
    #Manipulación de tabla df_contratos
    [dim_tipo_doc, dim_prove, dim_estado_contrato] = dim_models(df)
    fact_contratos = fact_models_contratos(df, dim_estado_contrato)
    #Registro de tiempo
    t2 = time.time()
    t2 = np.round(t2-t1)
    logger.info(f"Demoré {t2} segundos en modelar el df_contratos")    
    return [dim_tipo_doc, dim_prove, dim_estado_contrato,fact_contratos, t2]
