import apache_beam as beam
import csv
from datetime import datetime
import pyodbc

BUCKET_NAME = 'bucket_migracion_raw'
TABLE_NAME = 'departments'

# Definir opciones de conexión a Cloud SQL
connection_config = {
    'database': 'bdmigracion',
    'user': 'sqlserver',
    'password': '',
    'host': '34.66.80.135',
    'driver': '{ODBC Driver 18 for SQL Server}',
}

# Crear la cadena de conexión a la base de datos
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

def parse_method(row):
    """
    Función para convertir cada fila a formato de diccionario
    y establecer el tipo correcto para cada columna
    """
    parsed_row = {}
    parsed_row['id'] = int(row[0])
    parsed_row['name'] = str(row[1])
    parsed_row['datetime'] = str(row[2])
    parsed_row['department_id'] = int(row[3])
    parsed_row['job_id'] = int(row[4])
    return parsed_row

def validate_schema(element):
    """
    Función para validar que el elemento tenga el número correcto de columnas
    """
    expected_num_fields = 5
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
        if len(element[1])==0: element[1]='NO PROPORCIONADO'
        str(element[1])
        if len(element[2])==0: element[2]='1969-01-01T01:01:01Z'
        datetime.strptime(element[2], '%Y-%m-%dT%H:%M:%SZ')
        #departments
        if len(element[3])==0: element[3]=0
        int(element[3])
        #jobs
        if len(element[4])==0: element[4]=0
        int(element[4])
        return True
    except ValueError:
        raise ValueError('Tipo de dato incorrecto. Revisar el archivo CSV')



def lookup_departments(element, conn_str):
    """
    Función para realizar join con la tabla de departamentos
    """
    conn = pyodbc.connect(conn_str)    
    department_id = element['department_id']
    cursor = conn.cursor()
    print(f"SELECT department FROM departments WHERE id = '{department_id}'")
    cursor.execute(f"SELECT department FROM departments WHERE id = '{department_id}'")
    department = cursor.fetchone()
    cursor.close()
    conn.close
    if department:
        element['department'] = department[0]
        print('DEPARTAMENTO')        
        print(element)
        return element
    else:
        element['department'] = None
        raise ValueError(f'El departamento con id {department_id} no existe')        


def lookup_jobs(element, conn_str):
    """
    Función para realizar join con la tabla de trabajos
    """
    conn = pyodbc.connect(conn_str)    
    job_id = element['job_id']
    cursor = conn.cursor()
    print(f"SELECT job FROM jobs WHERE id = '{job_id}'")
    cursor.execute(f"SELECT job FROM jobs WHERE id = '{job_id}'")
    job = cursor.fetchone()
    cursor.close()
    conn.close    
    if job:
        element['job'] = job[0]
        print('TRABAJO')
        print(element)
        return element
    else:
        element['job'] = None
        raise ValueError(f'El trabajo con id {job_id} no existe')


def write_to_sql(element):
    """
    Función para escribir cada registro en la base de datos
    """
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    sql = f"INSERT INTO hired_employees (id, name, datetime, department_id, job_id) VALUES (?, ?, ?, ?, ?)"
    values = (element['id'], element['name'], element['datetime'], element['department_id'], element['job_id'])
    print(sql,values)
    cursor.execute(sql, values)
    conn.commit()
    conn.close


def run():

    with beam.Pipeline() as p:
        # Leer archivos CSV desde un bucket de GCS
        lines = p | 'ReadFromGCS' >> beam.io.ReadFromText(f'gs://{BUCKET_NAME}/empleados/*.csv')# Lee todos los archivos de una ruta de GCS previamente cargados con gsutil

        # Parsear cada fila y convertirla a formato de diccionario
        records = (lines | 'ParseCSV' >> beam.Map(lambda row: next(csv.reader([row]))) #Lee filas
                        | 'ValidateSchema' >> beam.Filter(validate_schema) #Valida cantidad de columnas en esquema
                        | 'ValidateType' >> beam.Filter(validate_type) #Valida tipo de datos
                        | 'ParseToDict' >> beam.Map(parse_method) #Convierte filas a diccionario
                        | 'AddJobInfo' >> beam.Map(lookup_jobs, conn_str) #Validar integridad con Job
                        | 'AddDepartmentInfo' >> beam.Map(lookup_departments, conn_str) #Validar integridad con departamento
                        | 'WriteToSQL' >> beam.Map(write_to_sql))    #Escribir registros a CloudSQL      

if __name__ == '__main__':
    run()