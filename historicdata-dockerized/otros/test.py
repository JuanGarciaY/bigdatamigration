import apache_beam as beam
import csv
from datetime import datetime
import pyodbc
# from apache_beam.io.gcp.sqlserver import WriteToSQLServer
# from apache_beam.options.pipeline_options import PipelineOptions

PROJECT_ID = 'bigdatamigration-382020'
BUCKET_NAME = 'bucket_migracion_raw'
TABLE_NAME = 'jobs'

# Establecer opciones de conexión a Cloud SQL
connection_config = {
    'database': 'bdmigracion',
    'user': 'sqlserver',
    'password': '#',
    'host': '34.66.80.135',
    'driver': '{ODBC Driver 18 for SQL Server}',
    
}

# options = PipelineOptions(
#     runner='DataflowRunner',
#     project=PROJECT_ID,
#     temp_location=f'gs://{BUCKET_NAME}/tmp',
#     region='us-east1',
#     setup_file='./setup.py'
#     # Otras opciones de configuración de Dataflow
# )


def parse_method(row):
    """
    Función para convertir cada fila a formato de diccionario
    y establecer el tipo correcto para cada columna
    """
    parsed_row = {}
    parsed_row['id'] = int(row[0])
    parsed_row['job'] = str(row[1])
    print(parsed_row)
    return parsed_row

def run():
#options=options
    with beam.Pipeline() as p:
        # Leer archivos CSV desde un bucket de GCS
        lines = p | 'ReadFromGCS' >> beam.io.ReadFromText(f'gs://{BUCKET_NAME}/trabajos/*.csv')#,skip_header_lines=1

        # Parsear cada fila y convertirla a formato de diccionario
        records = lines | 'ParseCSV' >> beam.Map(lambda row: next(csv.reader([row]))) | 'PrintLines' >> beam.Map(parse_method)


if __name__ == '__main__':
    run()