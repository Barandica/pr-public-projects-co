####################################################################Importación de librerias
import pandas as pd
import logging
import numpy as np
import time as time

######################################################Configuración del registro de eventos
logger = logging.getLogger("clean_up_df_contratos")
file_handler = logging.FileHandler("logs_main_py.txt")
file_handler.setFormatter(logging.Formatter("[%(levelname)s] [%(asctime)s] [%(name)s]: %(message)s"))
logger.addHandler(file_handler)
logging.StreamHandler()

#########################################Reemplazamos los datos de la columna estadocontrato
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
        #Registros nulos documentoproveedor
        df_docproveedor = df[df['documentoproveedor'].isnull()]  
        df = df.dropna(subset=['documentoproveedor'])
        registros_solo_letras = df[df['documentoproveedor'].str.match(r'^[a-zA-Z]+$')]
        df_inconsistencias = pd.concat([df_docproveedor, registros_solo_letras], ignore_index=True)
        df = df[~df['documentoproveedor'].str.match(r'^[a-zA-Z]+$')]
        #Agrupar nulos
        valores_atipicos = ["Sin Dato", "No Definido", "Sin Descripcion"] 
        df.loc[df['tipodocproveedor'].isin(valores_atipicos), 'tipodocproveedor'] = "No definido"
        #Registros nulos vigencia_contrato
        df_vigencia_contrato = df[df['vigenciacontrato'].isnull()]  
        df_inconsistencias = pd.concat([df_inconsistencias, df_vigencia_contrato], ignore_index=True)
        df = df.dropna(subset=['vigenciacontrato'])
        #Valores negativos
        df['valorcontrato'] = pd.to_numeric(df['valorcontrato'], errors='coerce')
        valores_nulos = df[df['documentoproveedor'].isnull()]
        df_inconsistencias = pd.concat([df_inconsistencias, valores_nulos], ignore_index=True)
        df = df.dropna(subset=['valorcontrato'])
        valores_negativos = df[df['valorcontrato'] < 0]
        df_inconsistencias = pd.concat([df_inconsistencias, valores_negativos], ignore_index=True)
        df = df[df['valorcontrato'] >= 0]
        #Agrupacion de los estados
        sin_estado = df[df['estadocontrato'].isnull()] 
        df_inconsistencias = pd.concat([df_inconsistencias, sin_estado], ignore_index=True)
        df = df.dropna(subset=['estadocontrato'])
        df['estadocontrato'] = df['estadocontrato'].apply(reemplazar_estado) 
        df['proveedor'] = df['proveedor'].fillna('Sin nombre') 
        #Cambiar el tipo de dato de nuestras columnas
        df = df.astype(columns)
        logger.info("El archivo df_contratos se ha limpiado con éxito")
        return df, df_inconsistencias
    except Exception as e:
        logger.error(f"Error al limpiar el df_contratos: {e}")

###########################################################################Función principal
def main_contratos_cleanup(df):
    t1 = time.time()
    #Limpiamos la data
    df, df_inconsistencias = cleanup(df)
    #Registro de tiempo
    t2 = time.time()
    t2 = np.round(t2-t1)
    logger.info(f"Demoré {t2} segundos en limpiar el df_contratos")
    return df, df_inconsistencias, t2