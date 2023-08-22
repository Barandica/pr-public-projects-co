####################################################################Importación de librerias
import time as time
import logging
import time as time
import numpy as np

######################################################Configuración del registro de eventos
logger = logging.getLogger("validator_api_gcs")
file_handler = logging.FileHandler("logs_main_py.txt")
file_handler.setFormatter(logging.Formatter("[%(levelname)s] [%(asctime)s] [%(name)s]: %(message)s"))
logger.addHandler(file_handler)
logging.StreamHandler()

##########################################################################Funcion principal
def conteo(columns_proyectos_t,size_proyectos_t,columns_contratos_t,size_contratos_t,columns_proyectos_e, size_proyectos_e,\
            columns_contratos_e, size_contratos_e):
    t1 = time.time()
    try:
    #Comparaciones entre los archivos desde la API y GCS
        all_conditions_successful = True

        if not np.array_equal(columns_proyectos_e, columns_proyectos_t):
            logger.error("Archivos proyectos NO tienen las mismas columnas")
            all_conditions_successful = False

        if not np.array_equal(columns_contratos_e, columns_contratos_t):
            logger.error("Archivos contratos NO tienen las mismas columnas")
            all_conditions_successful = False

        if size_proyectos_e != size_proyectos_t:
            logger.error("Archivos proyectos NO tienen el mismo tamaño")
            all_conditions_successful = False

        if size_contratos_e != size_contratos_t:
            logger.error("Archivos contratos NO tienen el mismo tamaño")
            all_conditions_successful = False

        # Imprimir el resultado final
        if all_conditions_successful:
            logger.info("Los archivos desde la API y GCS son identicos")
            bool = True
        else:
            logger.info("Los archivos desde la API y GCS tienen diferencias")
            bool = False
            logger.info(f"Los archivos son identicos")
    except Exception as e:
        logger.error(f"Error al comparar los archivos: {e}")
    #Registro de tiempo
    t2 = time.time()
    t2 = np.round(t2-t1)
    logger.info(f"Demoré {t2} segundos en comparar los archivos de la API y GCS")
    return bool, t2