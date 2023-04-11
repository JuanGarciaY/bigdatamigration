
import pytds
import os
from google.cloud.sql.connector import Connector, IPTypes
import sqlalchemy
import functions_framework

# Define la cadena de conexión a la base de datos
db_user = 'sqlserver'
db_pass = ''
db_name = 'bdmigracion'
db_host = '35.184.218.122'
instance_connection_name='bigdatamigration-382020:us-central1:bdmigracion'
db_port ='1433'

def validate_data(data,rules):
    
    errors=[]
    for row in data:
        for field,props in rules.items(): #una lista de tuplas que contienen la clave y el valor de cada elemento del diccionario
            
            #verifica que el campo requerido este en el objeto
            if field not in row and props["required"]:
                errors.append(f"Error en objeto {data.index(row)+1}: campo '{field}' requerido.")
                print("error")
                continue
            
            #verifica el tipo de dato del campo
            if field in row and not isinstance(row[field],props["type"]):
                errors.append(f"Error en objeto {data.index(row)+1}: campo '{field}' debe ser de tipo {props['type']}.")
    return errors
                 
# Register an HTTP function with the Functions Framework
@functions_framework.http
def insert_new_data(request):
    ip_type = IPTypes.PUBLIC
    connector = Connector(ip_type)
    def getconn() -> pytds.Connection:
            conn = connector.connect(
                instance_connection_name,
                "pytds",
                user=db_user,
                password=db_pass,
                db=db_name,
            )
            return conn

    # Crear un objeto de motor de base de datos utilizando la biblioteca SQLAlchemy.
    engine = sqlalchemy.create_engine(
        "mssql+pytds://",  # La cadena de conexión a la base de datos.

        creator=getconn,   # Una función que crea una nueva conexión de base de datos.

        pool_size=5, # El tamaño máximo de la piscina de conexiones permanentes.

        max_overflow=2, # El número máximo de conexiones adicionales que se pueden tener temporalmente si no hay conexiones permanentes disponibles.

        pool_timeout=30,  # 30 segundos  # El tiempo máximo que un hilo de ejecución esperará para obtener una conexión de la piscina de conexiones antes de lanzar una excepción.

        pool_recycle=1800,  # 30 minutos # El tiempo máximo que se permite que una conexión permanezca en la piscina antes de que sea descartada y reemplazada por una nueva conexión.
    )

    conn =engine.connect()
    data = request.json["objetos"]
    if len(data)>1000:
         return "No se pueden enviar más de 1000 registros en una misma solicitud"
    

    # Define las reglas del diccionario de datos
    job_rules = {
        "id": {"type": int, "required": False}, #autogenerado
        "job": {"type": str, "required": True}
    }
    
    errors=validate_data(data,job_rules) 
    if errors:
         #As of Flask 1.1, the return statement will automatically jsonify a dictionary in the first return value. You can return the data directly:
         return {'success': False, 'errors': errors},400
    last_id = conn.execute(sqlalchemy.text('SELECT MAX(id) FROM JOBS')).fetchone()
    next_id = last_id[0] + 1 if last_id else 1
    
    # Definir la sentencia INSERT parametrizada
    insert_query='INSERT INTO JOBS(id,job) VALUES(:id, :job)'

    i=0
    for row in data:       
        if i>0: next_id+=1
        values = {'id': next_id, 'job':row['job']}
        conn.execute(sqlalchemy.text(insert_query),values)
        conn.commit()
        stmt=conn.execute(sqlalchemy.text(""" SELECT COUNT(1) as count FROM JOBS """))
        print("Table JOBS, rows:",stmt.first()[0])
        i+=1
    conn.close()    
    return "OK"
