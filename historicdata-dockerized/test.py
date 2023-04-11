import apache_beam as beam
import csv
from datetime import datetime
import pyodbc
# from apache_beam.io.gcp.sqlserver import WriteToSQLServer

PROJECT_ID = 'bigdatamigration-382020'
BUCKET_NAME = 'bucket_migracion_raw'
TABLE_NAME = 'jobs'

# Establecer opciones de conexión a Cloud SQL
connection_config = {
    'database': 'bdmigracion',
    'user': 'sqlserver',
    'password': '',
    'host': '34.66.80.135',
    'driver': '{ODBC Driver 18 for SQL Server}',
    
}


def parse_method(row):
    """
    Función para convertir cada fila a formato de diccionario
    y establecer el tipo correcto para cada columna
    """
    parsed_row = {}
    parsed_row['id'] = int(row[0])
    parsed_row['job'] = str(row[1])
    return parsed_row

def validate_schema(element):
    """
    Función para validar que el elemento tenga el número correcto de columnas
    """
    expected_num_fields = 2
    num_fields = len(element)
    if num_fields != expected_num_fields:
        raise ValueError(f'Número incorrecto de columnas. Esperado: {expected_num_fields}, encontrado: {num_fields}')
    return True

def validate_type(element):
    """
    Función para validar que los tipos de datos sean correctos
    """
    try:
        int(element[0])
        str(element[1])
        return True
    except ValueError:
        raise ValueError('Tipo de dato incorrecto. Revisar el archivo CSV')

conn_str = (
        'DRIVER={driver};'
        'SERVER={server};'
        'DATABASE={database};'
        'UID={username};'
        'PWD={password};'
        'TrustServerCertificate=yes;'
        'Encrypt=yes;'
        'ssl=require;'
        'Connection Timeout=30;'
    ).format(
        driver=connection_config['driver'],
        server=connection_config['host'],
        database=connection_config['database'],
        username=connection_config['user'],
        password=connection_config['password']
    )

def write_to_sql(element):
    """
    Función para escribir cada registro en la base de datos
    """
    print(element)
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    sql = f"INSERT INTO {TABLE_NAME} (id, job) VALUES (?, ?)"
    values = (element['id'], element['job'])
    cursor.execute(sql, values)
    conn.commit()
    conn.close()


def run():

    with beam.Pipeline() as p:
        # Leer archivos CSV desde un bucket de GCS
        lines = p | 'ReadFromGCS' >> beam.io.ReadFromText(f'gs://{BUCKET_NAME}/trabajos/*.csv')#,skip_header_lines=1

        # Parsear cada fila y convertirla a formato de diccionario
        records = lines | 'ParseCSV' >> beam.Map(lambda row: next(csv.reader([row]))) \
                        | 'ValidateSchema' >> beam.Filter(validate_schema) \
                        | 'ValidateType' >> beam.Filter(validate_type) \
                        | 'ParseToDict' >> beam.Map(parse_method) \
                        |'WriteToSQL' >> beam.Map(write_to_sql) \



if __name__ == '__main__':
    run()