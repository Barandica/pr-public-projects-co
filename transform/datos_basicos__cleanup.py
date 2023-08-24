####################################################################Importación de librerias
import pandas as pd
import logging
import numpy as np
import time as time

######################################################Configuración del registro de eventos
logger = logging.getLogger("clean_up_df_proyectos")
file_handler = logging.FileHandler("logs_main_py.txt")
file_handler.setFormatter(logging.Formatter("[%(levelname)s] [%(asctime)s] [%(name)s]: %(message)s"))
logger.addHandler(file_handler)
logging.StreamHandler()

######################################################################Limpieza de los datos
def cleanup(df):  
    try:
        #Columnas de nuestro df y el tipo de dato nuevo
        columns = {
        "bpin" : str,
        "nombreproyecto" : object,
        "objetivogeneral" : object, 
        "estadoproyecto" : str,
        "horizonte" : str, 
        "sector" : str,
        "entidadresponsable" : object,
        "programapresupuestal" : object,
        "tipoproyecto" : str,
        "valortotalproyecto" : float,
        "valorvigenteproyecto" : float,
        "valorobligacionproyecto" : float,
        "valorpagoproyecto" : float,
        "subestadoproyecto" : str,
        "codigoentidadresponsable" : str, 
        "ano_ini" : int,
        "ano_fin" : int
        } 
        # Filtramos las columnas que están presentes en el diccionario
        df_columns = [col for col in df if col in columns]    
        # Creamos un nuevo DataFrame con las columnas seleccionadas
        df = df[df_columns]
        # Diccionario de combinaciones
        combinaciones = {
            'PGN': 'PRESUPUESTO GENERAL DE LA NACION',
            'T': 'TERRITORIAL',
            'SGR': 'SISTEMA GENERAL DE REGALIAS'}
        # Combinar los registros de la columna usando el diccionario
        df.loc[:, 'tipoproyecto'] = df['tipoproyecto'].replace(combinaciones) 
        #Separar los años de la columna horizonte
        df[["ano_ini", "ano_fin"]] = df["horizonte"].str.extract(r"(\d+)-(\d+)")
        #Cambiar el tipo de dato de nuestras columnas
        df = df.astype(columns)   
        #Quitar los duplicados de la columna bpin
        df_inconsistencias = df[df.duplicated('bpin', keep=False)]
        df = df.drop_duplicates('bpin', keep=False)
        #Registros nulos
        null_df = df[df['codigoentidadresponsable'].isnull()]
        df_inconsistencias = pd.concat([df_inconsistencias, null_df], ignore_index=True)
        df = df.dropna(subset=['codigoentidadresponsable'])          
        logger.info("El archivo df_contratos se ha limpiado con éxito")
        return df, df_inconsistencias
    except Exception as e:
        logger.error(f"Error al limpiar el df_contratos: {e}")

###########################################################################Función principal
def main_proyectos_cleanup(df):
    t1 = time.time()
    #print(f"Dimension de df_proyectos entrante: {df_proyectos.shape}")
    #Mayusculas en la columna estadoproyecto
    df.loc[:, 'estadoproyecto'] = df['estadoproyecto'].str.upper() 
    #Limpiamos la data
    df, df_inconsistencias = cleanup(df)
    #Registro de tiempo
    t2 = time.time()
    t2 = np.round(t2-t1)
    logger.info(f"Demoré {t2} segundos en limpiar el df_proyectos")
    return df, df_inconsistencias, t2