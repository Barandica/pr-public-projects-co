####################################################################Importación de librerias
import time as time
import logging
import time as time
import numpy as np

######################################################Configuración del registro de eventos
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] [%(asctime)s] [%(name)s]: %(message)s",
    handlers=[
        #Salida en consola
        logging.StreamHandler(),  
        #Salida en archivo local
        logging.FileHandler("transform\logs\conteo.log.txt"), 
    ])

##########################################################################Funcion principal
def conteo(columns_proyectos_t,size_proyectos_t,columns_contratos_t,size_contratos_t,columns_proyectos_e, size_proyectos_e,\
            columns_contratos_e, size_contratos_e):
    #Creación de  un objeto logger para el registro
    logger = logging.getLogger("CleanUp2") 
    t1 = time.time()
    try:

        #Creación un objeto logger para el registro
        logger = logging.getLogger("conteo") 
    
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
        logger.error(f"Error comparar los archivos: {e}")

    t2 = time.time()
    t2 = np.round(t2-t1)
    print(f"Demoré {t2} segundos en comparar los archivos de la API y GCS")
    return bool, t2