import pyodbc
from dotenv import load_dotenv
import re
import os

# Función para contar tokens en un texto SQL
def contar_tokens(sql_code):
    # Divide el código en tokens usando una expresión regular
    tokens = re.findall(r'\w+|\S', sql_code)
    return len(tokens)

# Cargar el archivo .env desde la raíz del proyecto
load_dotenv('.env.tersuave')

# Configuración de la conexión a SQL Server
serverDW = os.getenv('DB_serverDW')
databaseDW = os.getenv('DB_databaseDW')
usernameDW = os.getenv('DB_usernameDW')
passwordDW = os.getenv('DB_passwordDW')
connection_stringDW = f'DRIVER={{ODBC Driver 17 for SQL server}};SERVER={serverDW};DATABASE={databaseDW};UID={usernameDW};PWD={passwordDW}'

# Conexión a la base de datos
try:
    connection = pyodbc.connect(connection_stringDW)
except pyodbc.Error as e:
    print(f"Error al conectar a la base de datos: {e}")
    connection = None

if connection:
    try:
        tokens_total = 0
        sp_count = 0
        cursor = connection.cursor()
        # Ejecutar la consulta para obtener los procedimientos almacenados
        cursor.execute("""
        SELECT ROUTINE_NAME, ROUTINE_DEFINITION
        FROM INFORMATION_SCHEMA.ROUTINES
        WHERE ROUTINE_TYPE = 'PROCEDURE';
        """)
        
        procedimientos = cursor.fetchall()
        
        for nombre, codigo in procedimientos:
            sp_count += 1
            if nombre.startswith('zzz'):
                continue
            if codigo:  # Asegurarse de que el código no sea nulo
                token_count = contar_tokens(codigo)
                tokens_total += token_count
                print(f"SP: {nombre} - Tokens: {token_count}")
            else:
                print(f"SP: {nombre} - Sin código disponible")
    
    except pyodbc.Error as e:
        print(f"Error al ejecutar la consulta: {e}")
    finally:
        connection.close()
print(f"\nCantidad total de tokens: ", tokens_total)
print(f"Cantidad de SP analizados: ", sp_count)
