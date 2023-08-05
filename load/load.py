import pandas as pd
from google.cloud import bigquery

def cargar_datos_en_bigquery(dataframe,  dataset, tabla):
    # Configura el cliente de BigQuery
    client = bigquery.Client()

    # Especifica la tabla destino en BigQuery
    table_ref = client.dataset(dataset).table(tabla)

    # Crea la tabla si no existe (opcional)
    table = bigquery.Table(table_ref)
    try:
        table = client.create_table(table)
    except:
        pass

    # Forma el nombre completo de la tabla en el formato correcto
    full_table_name = f"{client.project}.{dataset}.{tabla}"

    # Carga el DataFrame en BigQuery
    dataframe.to_gbq(destination_table=full_table_name, project_id=client.project, if_exists='replace')

    print("Datos cargados correctamente en BigQuery.")

def load_bigquery(dim_proyectos_cer, dim_sector, dim_estado_proyecto, dim_tipo_proyecto, dim_plan,
                  dim_subestado, fact_proyecto, dim_tipo_doc, dim_prove, dim_estado_contrato, fact_contratos):
    # Especifica los detalles del proyecto y dataset
    dataset = "dwh_public_projects_co"

    # Define una lista con todas las tablas y sus nombres
    tables = [
        ("dim_proyectos_cer", dim_proyectos_cer),
        ("dim_sector", dim_sector),
        ("dim_estado_proyecto", dim_estado_proyecto),
        ("dim_tipo_proyecto", dim_tipo_proyecto),
        ("dim_plan", dim_plan),
        ("dim_subestado", dim_subestado),
        ("fact_proyecto", fact_proyecto),
        ("dim_tipo_doc", dim_tipo_doc),
        ("dim_prove", dim_prove),
        ("dim_estado_contrato", dim_estado_contrato),
        ("fact_contratos", fact_contratos)
    ]

    # Carga los datos en BigQuery para cada tabla
    for table_name, dataframe in tables:
        cargar_datos_en_bigquery(dataframe, dataset, table_name)

