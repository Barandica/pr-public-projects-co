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
        logging.FileHandler("transform\logs\clean_datos_basicos.log.txt"),  
    ])

######################################################################Limpieza de los datos
def cleanup(df):
    #Creación de  un objeto logger para el registro
    logger = logging.getLogger("CleanUp1")  
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
        df['tipoproyecto'] = df['tipoproyecto'].str.strip().str.upper().replace(combinaciones)
        #df.loc[:, 'estadoproyecto'] = df['estadoproyecto'].replace(combinaciones) 
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
        logger.info(f"El archivo se ha limpiado con éxito")
        return df, df_inconsistencias, True
    except pd.errors.PandasError as e:
        logger.error(f"Error al limpiar el df: {e}")
    except Exception as e:
        logger.error(f"Error al limpiar el df: {e}")

###########################################################################Función principal
def main_proyectos_cleanup(df):
    t1 = time.time()
    #Mayusculas en la columna estadoproyecto
    df.loc[:, 'estadoproyecto'] = df['estadoproyecto'].str.upper() 
    #Limpiamos la data
    df, df_inconsistencias, bool = cleanup(df)
    t2 = time.time()
    t2 = np.round(t2-t1)
    print(f"Demoré {t2} segundos en limpiar el df_proyectos")
    return df, df_inconsistencias, bool, t2