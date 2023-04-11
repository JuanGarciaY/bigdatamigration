import os
import sqlalchemy
from flask import Flask, jsonify

app = Flask(__name__)

# Define the database connection string
db_user = 'sqlserver'
db_pass = ''
db_name = 'bdmigracion'
unix_socket_path = '/cloudsql/{}'.format(os.environ.get('INSTANCE_CONNECTION_NAME'))

# Define the SQL query to retrieve the number of employees hired for each job and department in 2021 divided by quarter
query = '''
SELECT department, job,
  COUNT(CASE WHEN MONTH(hire_date) BETWEEN 1 AND 3 THEN 1 END) AS Q1,
  COUNT(CASE WHEN MONTH(hire_date) BETWEEN 4 AND 6 THEN 1 END) AS Q2,
  COUNT(CASE WHEN MONTH(hire_date) BETWEEN 7 AND 9 THEN 1 END) AS Q3,
  COUNT(CASE WHEN MONTH(hire_date) BETWEEN 10 AND 12 THEN 1 END) AS Q4
FROM employees
WHERE YEAR(hire_date) = 2021
GROUP BY department, job
ORDER BY department, job
'''

# Endpoint to retrieve the number of employees hired for each job and department in 2021 divided by quarter
@app.route('/api/employee-metrics', methods=['GET'])
def employee_metrics():
    # Connect to the database and execute the query
    try:
        pool = connect_unix_socket()
        with pool.connect() as conn:
            results = conn.execute(query).fetchall()
            # Convert the results to a list of dictionaries
            data = []
            for row in results:
                data.append({'department': row[0], 'job': row[1], 'Q1': row[2], 'Q2': row[3], 'Q3': row[4], 'Q4': row[5]})
            return jsonify({'success': True, 'data': data}), 200
    
    except Exception as e:
        return jsonify({'success': False, 'errors': [str(e)]}), 500

# Function to connect to the Cloud SQL instance via Unix sockets
def connect_unix_socket() -> sqlalchemy.engine.base.Engine:
    """ Initializes a Unix socket connection pool for a Cloud SQL instance of MySQL. """
    # Create the SQLAlchemy engine instance
    pool = sqlalchemy.create_engine(
        # Create the database connection URL
        # mysql+pymysql://<db_user>:<db_pass>@/<db_name>?unix_socket=<socket_path>/<cloud_sql_instance_name>
        sqlalchemy.engine.url.URL.create(
            drivername="mysql+pymysql",
            username=db_user,
            password=db_pass,
            database=db_name,
            query={"unix_socket": unix_socket_path},
        ),
        # Set some additional connection parameters
        pool_size=5,
        max_overflow=2,
        pool_timeout=30,  # 30 seconds
        pool_recycle=1800,  # 30 minutes
    )
    return pool

# Generic error handling
@app.errorhandler(Exception)
def handle_exception(e):
    return jsonify({'success': False, 'errors': [str(e)]}), 500

if __name__ == '__main__':
    app.run()