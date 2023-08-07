####################################################################Importación de librerias
import pandas as pd
import logging
import numpy as np
import time as time

#Configuración del registro de eventos
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] [%(asctime)s] [%(name)s]: %(message)s",
    handlers=[
        #Salida en consola
        logging.StreamHandler(), 
        #Salida en archivo local
        logging.FileHandler("transform\logs\clean_datos_contratos.log.txt"),  
    ])

def reemplazar_estado(estado):
    estados_proyecto = {
            'EN EJECUCION': ['En ejecución', 'Activo', 'Convocado', 'Adjudicado', 'En aprobación', 'Celebrado'],
            'FINALIZADO': ['Terminado', 'Liquidado', 'terminado', 'Cerrado'],
            'TERMINADO SIN LIQUIDAR': ['Terminado sin Liquidar'],
            'TERMINADO ANORMALMENTE DESPUES DE CONVOCADO': ['Terminado Anormalmente después de Convocado'],
            'MODIFICADO': ['Modificado'],
            'CEDIDO': ['Cedido', 'cedido'],
            'SUSPENDIDO': ['Suspendido', 'suspendido'],
            'CONTRATADO SIN EJECUCION': ['Contratado sin ejecución'],
            'BORRADOR': ['Borrador'],
            'ENVIADO PROVEEDOR': ['enviado Proveedor']
        }
    for clave, lista_estados in estados_proyecto.items():
        if estado in lista_estados:
            return clave
    return estado

#######################################################################Limpieza de los datos
def cleanup(df):
    #Creación de  un objeto logger para el registro
    logger = logging.getLogger("CleanUp2")  
    try:
        #Columnas de nuestro df y el tipo de dato nuevo
        columns = {
        "bpin" : str,
        "tipodocproveedor" : str,
        "documentoproveedor" : str,
        "proveedor" : object,
        "estadocontrato" : str,
        "valorcontrato" : float,
        "urlproceso" : object,
        "vigenciacontrato" : int,
        "objetodelcontrato" : object
        } 
        # Filtramos las columnas que están presentes en el diccionario
        df_columns = [col for col in df if col in columns]    
        # Creamos un nuevo DataFrame con las columnas seleccionadas
        df = df[df_columns]
        #Agrupar nulos
        valores_atipicos = ["Sin Dato", "No Definido", "Sin Descripcion"]
        df['tipodocproveedor'] = df['tipodocproveedor'].replace(valores_atipicos, "No definido")   
        #Registros nulos documentoproveedor
        df = df.dropna(subset=['documentoproveedor'])
        df = df[~df['documentoproveedor'].str.match(r'^[a-zA-Z]+$')]
        df_docproveedor = df[df['documentoproveedor'].isnull()]  
        registros_solo_letras = df[df['documentoproveedor'].str.match(r'^[a-zA-Z]+$')]
        df_inconsistencias = pd.concat([df_docproveedor, registros_solo_letras], ignore_index=True)
        #Registros nulos vigencia_contrato
        df_vigencia_contrato = df[df['vigenciacontrato'].isnull()]  
        df_inconsistencias = pd.concat([df_inconsistencias, df_vigencia_contrato], ignore_index=True)
        df = df.dropna(subset=['vigenciacontrato'])
        #Valores negativos
        valores_negativos = df[df['valorcontrato'] < 0]
        df_inconsistencias = pd.concat([df_inconsistencias, valores_negativos], ignore_index=True)
        df = df[df['valorcontrato'] >= 0]
        #Agrupacion de los estados
        df['estadocontrato'] = df['estadocontrato'].apply(reemplazar_estado) 
        #Cambiar el tipo de dato de nuestras columnas
        df = df.astype(columns)
        logger.info(f"El archivo se ha limpiado con éxito")
        return df, df_inconsistencias, True
    except pd.errors.PandasError as e:
        logger.error(f"Error al limpiar el df: {e}")
    except Exception as e:
        logger.error(f"Error al limpiar el df: {e}")

###########################################################################Función principal
def main_contratos_cleanup(df):
    t1 = time.time()
    #Limpiamos la data
    df, df_inconsistencias, bool = cleanup(df)
    t2 = time.time()
    t2 = np.round(t2-t1)
    print(f"Demoré {t2} segundos en limpiar el df_contratos")
    return df, df_inconsistencias, bool, t2