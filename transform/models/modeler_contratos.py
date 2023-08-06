####################################################################Importación de librerias
import pandas as pd
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
        logging.FileHandler("transform\logs\modeler_proyectos.log.txt"),  
    ])

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
    #Creación de  un objeto logger para el registro
    logger = logging.getLogger("modeler_dimensiones_contratos")  
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
        dim_prove['proveedor'] = dim_prove['proveedor'].fillna('Sin nombre')       
        logger.info("Se han construido las dimensiones de los contratos con éxito")
        return [dim_tipo_doc, dim_prove, dim_estado_contrato]
    except pd.errors.PandasError as e:
        logger.error(f"Error al crear las dimensiones: {e}")
    except Exception as e:
        logger.error(f"Error al crear las dimensiones: {e}")
            
#############################################Contrucción de la tabla de hechos de proyectos
def fact_models_contratos(df, dim_estado_contrato):
    #Creación de  un objeto logger para el registro
    logger = logging.getLogger("modeler_fact_contratos")
    try:
        df["id_estado_contrato"] = df["estadocontrato"].map(dim_estado_contrato.set_index("estadocontrato")["id_estado_contrato"])
        
        columns = ['bpin', 'documentoproveedor', 'id_estado_contrato', 'valorcontrato', 'urlproceso', 'vigenciacontrato', 'objetodelcontrato' ] 
        #Creación de tabla de hechos
        fact_contratos = df[columns]
        logger.info("Se ha construido la tablas fact_contratos")   
        return fact_contratos, True
    except pd.errors.PandasError as e:
        logger.error(f"Error crear los hechos: {e}")
    except Exception as e:
        logger.error(f"Error al crear los hechos: {e}")
    
##########################################################################Función principal
def models_contratos(df):
    t1 = time.time()
    #Manipulación de tabla df_contratos
    [dim_tipo_doc, dim_prove, dim_estado_contrato] = dim_models(df)
    fact_contratos, bool = fact_models_contratos(df, dim_estado_contrato)
    t2 = time.time()
    t2 = np.round(t2-t1)
    print(f"Demoré {t2} segundos en modelar el df_contratos")    
    return [dim_tipo_doc, dim_prove, dim_estado_contrato,fact_contratos, bool, t2]
