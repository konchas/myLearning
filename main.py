from flask import Flask, jsonify, request
import snowflake.connector

app = Flask(__name__)

# Set Snowflake credentials
account_name = 'ewhcreo-ot80498'
username = 'benten63'
password = 'Benten@63'
warehouse_name = 'COMPUTE_WH'
database_name = 'MYTESTDB'
schema_name = 'MYSCHEMA'

# Connect to Snowflake
conn = snowflake.connector.connect(
    account=account_name,
    user=username,
    password=password,
    warehouse=warehouse_name,
    database=database_name,
    schema=schema_name
)

#default route
@app.route('/')
def default_route():
    return "<p>welcome to the page </p>"


# route to execute a query
@app.route('/select')
def query():
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM table2')
    result = cursor.fetchall()
    cursor.close()
    return jsonify(result)

# route to insert data in a table
@app.route('/insert', methods=['POST'])
def insert():
    data = request.json
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO table2 (col1, col2) VALUES (%s, %s)",
        (data['col1'], data['col2'])
    )
    cursor.close()
    conn.commit()
    return 'Data inserted successfully', 201

# route to update data in a table
@app.route('/update', methods=['PUT'])
def update():
    data = request.json
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE table2 SET col1=%s WHERE col2=%s",
        (data['col1'], data['col2'])
    )
    cursor.close()
    conn.commit()
    return 'Data updated successfully', 200

if __name__ == '__main__':
    app.run()
