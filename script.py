import pyodbc
import cohere
from dotenv import load_dotenv
import os

# Cargar el archivo .env desde la raíz del proyecto
load_dotenv()

# Configura tu clave de API de Cohere
cohere_api_key = os.getenv('API_KEY_COHERE')
co = cohere.Client(api_key=cohere_api_key)

# Configuración de la conexión a SQL Server
server = os.getenv('DB_SERVER')
database = os.getenv('DB_DATABASE')
username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')
connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

# Función para extraer el código de los procedimientos almacenados
def obtener_store_procedures(connection):
    try:
        consulta = """
        SELECT ROUTINE_NAME, ROUTINE_DEFINITION
        FROM INFORMATION_SCHEMA.ROUTINES
        WHERE ROUTINE_TYPE = 'PROCEDURE'
        AND ROUTINE_NAME='JOB_A_EJECUTAR_NIFI';
        """
        cursor = connection.cursor()
        cursor.execute(consulta)
        return cursor.fetchall()
    except pyodbc.Error as e:
        print(f"Error al obtener procedimientos almacenados: {e}")
        return []

# Función para analizar código SQL con el modelo de Cohere
def analizar_codigo_sql(codigo_sql):
    prompt = f"""
    Analiza el siguiente procedimiento almacenado de SQL Server para ver si posee malas prácticas de desarrollo de software para el lenguaje Transact SQL (T-SQL).
    Describe cualquier mala práctica como variables y filtros de querys con valores hardcodeados, acoplamiento entre las funciones que implementa cada store procedure, baja cohesión, gran longitud de código, etc.
    Por cada mala práctica, proporciona sugerencia/s de mejora para corregir los problemas detectados.
    
    Sintaxis del procedimiento almacenado:
    {codigo_sql}

    Si crees que no tiene malas prácticas, simplemente no indiques nada.
    El formato de respuesta deben ser 2 secciones: una que comience con una línea que diga "Malas prácticas:" y abajo por cada mala práctica enumerada, una línea que comience con "- mala práctica...". 
    Luego en la otra sección, lo mismo para las recomendaciones: una línea que diga "Recomendaciones" y por debajo por cada recomendación enumerada, una línea que comience con "- recomendación...". 
    Respeta este formato, por favor.
    No me des recomendaciones generales, quiero que analices puntualmente el código que te envío.
    """

    try:
        # Llamada a la API de Cohere
        response = co.generate(
            prompt=prompt
        )
        resultado = response.generations[0].text.strip()
        
        # Dividir el resultado en malas prácticas y recomendaciones
        return dividir_respuesta_api(resultado)

    except Exception as e:
        print(f"Error al analizar el código SQL: {e}")
        return "Error en el análisis.", "Error en el análisis."

# Función que extrae las malas prácticas y recomendaciones de la respuesta de la API
def dividir_respuesta_api(respuesta_api):
    # Encontrar los índices donde comienzan las secciones   
    index_malas_practicas = respuesta_api.find("Malas prácticas:")
    index_recomendaciones = respuesta_api.find("Recomendaciones:")

     # Extraer las secciones
    malas_practicas_seccion = respuesta_api[index_malas_practicas:index_recomendaciones].strip()
    recomendaciones_seccion = respuesta_api[index_recomendaciones:].strip()

    # Dividir en porciones de texto que comienzan con "-"
    malas_practicas = [practica.strip() for practica in malas_practicas_seccion.split("-") if practica]
    recomendaciones = [recomendacion.strip() for recomendacion in recomendaciones_seccion.split("-") if recomendacion]

    return malas_practicas, recomendaciones

# Función para insertar el análisis en la base de datos
def insertar_resultado(connection, nombre_procedimiento, malas_practicas, recomendaciones):
    try:
        consulta = """
        INSERT INTO ZZ_AnalisisSintaxisSP_IA (Modelo, NombreProcedimiento, MalasPracticas, Recomendaciones)
        VALUES ('Cohere', ?, ?, ?);
        """
        cursor = connection.cursor()
        
        # Insertar cada mala práctica con su recomendación
        for mala_practica, recomendacion in zip(malas_practicas, recomendaciones):
            if mala_practica == 'Malas prácticas:':
                continue
            cursor.execute(consulta, (nombre_procedimiento, mala_practica.strip(), recomendacion.strip()))
        
        connection.commit()
    except pyodbc.Error as e:
        print(f"Error al insertar resultados en la base de datos: {e}")

# Conexión a la base de datos
try:
    connection = pyodbc.connect(connection_string)
except pyodbc.Error as e:
    print(f"Error al conectar a la base de datos: {e}")
    connection = None

if connection:
    try:
        procedimientos = obtener_store_procedures(connection)
        
        for nombre, codigo in procedimientos:
            print(f"\nAnalizando el procedimiento almacenado: {nombre}")
            malas_practicas, recomendaciones = analizar_codigo_sql(codigo)
            
            # Insertar el resultado en la base de datos
            insertar_resultado(connection, nombre, malas_practicas, recomendaciones)
            
            print(f"Resultado del análisis insertado en la base de datos.\n")
            print("-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n")
            
    except Exception as e:
        print(f"Error en el proceso de análisis: {e}")
    finally:
        connection.close()
