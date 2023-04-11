import os
import sqlalchemy
import pytds
from flask import Flask, request, jsonify

app = Flask(__name__)

# Define la cadena de conexión a la base de datos
db_user = 'sqlserver'
db_pass = ''
db_name = 'bdmigracion'
db_host = '35.184.218.122'

# Define las reglas del diccionario de datos
department_rules = {
    "id": {"type": "integer", "required": True},
    "department": {"type": "string", "required": True}
}

# Función para validar los datos recibidos
def validate_data(data, rules):
    errors = []
    for row in data:
        for field, props in rules.items():
            # Verifica que el campo requerido esté presente
            if props["required"] and field not in row:
                errors.append(f"Error en fila {data.index(row)}: campo '{field}' requerido.")
                continue
            
            # Verifica el tipo de dato del campo
            if field in row and not isinstance(row[field], props["type"]):
                errors.append(f"Error en fila {data.index(row)}: campo '{field}' debe ser de tipo {props['type']}.")
                
    return errors

# Endpoint para recibir los nuevos datos
@app.route('/api/departments', methods=['POST'])
def add_departments():
    data = request.json
    
    # # Valida los datos recibidos
    # errors = validate_data(data, department_rules)
    # if errors:
    #     return jsonify({'success': False, 'errors': errors}), 400
    
    # # Conecta con la base de datos y escribe los datos
    # try:
    #     conn = pytds.connect(host=db_host, database=db_name, user=db_user, password=db_pass)
    #     with conn.cursor() as cursor:
    #         for row in data:
    #             cursor.execute("INSERT INTO departments (id, department) VALUES (?, ?)", row["id"], row["department"])
    #         conn.commit()
    #         return jsonify({'success': True}), 200
    
    # except Exception as e:
    return jsonify({'success': False, 'errors': [str(e)]}), 500
    # finally:
    #     conn.close()

# Manejo de errores genérico
@app.errorhandler(Exception)
def handle_exception(e):
    return jsonify({'success': False, 'errors': [str(e)]}), 500

if __name__ == '__main__':
    app.run()