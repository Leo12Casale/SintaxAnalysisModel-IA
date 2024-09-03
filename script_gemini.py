import pyodbc
import google.generativeai as genai
import os
from dotenv import load_dotenv

# Cargar el archivo .env desde la raíz del proyecto
load_dotenv('.env.tersuave')

# Configura tu clave de API de Cohere
gemini_api_key = os.getenv('API_KEY_GEMINI')
genai.configure(api_key=gemini_api_key)

# Configuración de la conexión a SQL serverDW
serverDW = os.getenv('DB_serverDW')
databaseDW = os.getenv('DB_databaseDW')
usernameDW = os.getenv('DB_usernameDW')
passwordDW = os.getenv('DB_passwordDW')
connection_stringDW = f'DRIVER={{ODBC Driver 17 for SQL server}};SERVER={serverDW};DATABASE={databaseDW};UID={usernameDW};PWD={passwordDW}'

#Creación del modelo
generation_config = {
    "temperature": 0.1,
    "max_output_tokens": 500
}
model = genai.GenerativeModel(
        model_name='gemini-1.5-flash',
        generation_config=generation_config
    )

# Función para extraer el código de los procedimientos almacenados
def obtener_store_procedures(connection):
    try:
        consulta = """
        SELECT ROUTINE_NAME, ROUTINE_DEFINITION
        FROM INFORMATION_SCHEMA.ROUTINES
        WHERE ROUTINE_TYPE = 'PROCEDURE'
        AND (ROUTINE_NAME='sp_gd_FAC_Costo_Consumo'
            OR ROUTINE_NAME='sp_gd_FAC_Costo_Consumo_Subcontratados'
            OR ROUTINE_NAME='sp_gd_FAC_Costo_Demanda'
            OR ROUTINE_NAME='sp_gd_FAC_Costo_Formula'
            OR ROUTINE_NAME='sp_gd_FAC_Costo_Formula_Estructura_v2'
            OR ROUTINE_NAME='sp_gd_FAC_Costo_Formula_Resumen'
            OR ROUTINE_NAME='sp_gd_FAC_Costo_Ingreso_Subcontratacion'
            OR ROUTINE_NAME='sp_gd_FAC_Costo_Margen'
            OR ROUTINE_NAME='sp_gd_FAC_Costo_Ultima_Compra'
            );
        """
        cursor = connection.cursor()
        cursor.execute(consulta)
        return cursor.fetchall()
    except pyodbc.Error as e:
        print(f"Error al obtener procedimientos almacenados: {e}")
        return []


# Función que extrae las malas prácticas y recomendaciones de la respuesta de la API
def dividir_respuesta_api(respuesta_api):
    # Encontrar los índices donde comienzan las secciones   
    index_malas_practicas = respuesta_api.find("## Malas prácticas:")
    index_recomendaciones = respuesta_api.find("## Recomendaciones:")

     # Extraer las secciones
    malas_practicas_seccion = respuesta_api[index_malas_practicas:index_recomendaciones].strip()
    recomendaciones_seccion = respuesta_api[index_recomendaciones:].strip()

    # Dividir en porciones de texto que comienzan con "-"
    malas_practicas = [practica.strip() for practica in malas_practicas_seccion.split("-") if practica]
    recomendaciones = [recomendacion.strip() for recomendacion in recomendaciones_seccion.split("-") if recomendacion]

    return malas_practicas, recomendaciones


def analizar_codigo_sql(codigo_sql):
    promptSQL = f"""
    Analiza el siguiente procedimiento almacenado de SQL serverDW para ver si posee malas prácticas de desarrollo de software para el lenguaje Transact SQL (T-SQL).
    Describe cualquier mala práctica como variables y filtros de querys con valores hardcodeados, acoplamiento entre las funciones que implementa cada store procedure, baja cohesión, gran longitud de código, etc.
    Por cada mala práctica, proporciona sugerencia/s de mejora para corregir los problemas detectados.
    
    Sintaxis del procedimiento almacenado:
    {codigo_sql}

    Si crees que no tiene malas prácticas, simplemente no indiques nada.
    El formato de respuesta deben ser 2 secciones: una que comience con una línea que diga "Malas prácticas:" y abajo por cada mala práctica enumerada, una línea que comience con "- mala práctica...". 
    Luego en la otra sección, lo mismo para las recomendaciones: una línea que diga "Recomendaciones" y por debajo por cada recomendación enumerada, una línea que comience con "- recomendación...". 
    Respeta este formato y no uses acentos en las palabras, por favor.
    No me des recomendaciones generales, quiero que analices puntualmente el código que te envío.
    """

    try:
        # Llamada a la API de Cohere
        response =  model.generate_content(promptSQL)

        # Segmentar respuesta
        malas_practicas, recomendaciones = dividir_respuesta_api(response.text)

        return malas_practicas, recomendaciones
    except Exception as e:
        print(f"Error al analizar el código SQL: {e}")
        return "Error en el análisis.", "Error en el análisis."

# Función para insertar el análisis en la base de datos
def insertar_resultado(connection, nombre_procedimiento, malas_practicas, recomendaciones):
    serverSTG = os.getenv('DB_serverSTG')
    databaseSTG = os.getenv('DB_DATABASESTG')
    usernameSTG = os.getenv('DB_usernameSTG')
    passwordSTG = os.getenv('DB_passwordSTG')
    connection_stringSTG = f'DRIVER={{ODBC Driver 17 for SQL server}};SERVER={serverSTG};DATABASE={databaseSTG};UID={usernameSTG};PWD={passwordSTG}'
    connectionSTG = pyodbc.connect(connection_stringSTG)
    try:
        consulta = """
        INSERT INTO [DDD].[AnalisisSintaxisSP_IA] (Modelo, NombreProcedimiento, MalasPracticas, Recomendaciones)
        VALUES ('Gemini', ?, ?, ?);
        """
        cursorSTG = connectionSTG.cursor()
        
        # Insertar cada mala práctica con su recomendación
        for mala_practica, recomendacion in zip(malas_practicas, recomendaciones):
            if mala_practica == '## Malas prácticas:':
                continue
            cursorSTG.execute(consulta, (nombre_procedimiento, mala_practica.strip(), recomendacion.strip()))
        
        connectionSTG.commit()
    except pyodbc.Error as e:
        print(f"Error al insertar resultados en la base de datos: {e}")
    
# Conexión a la base de datos
try:
    connection = pyodbc.connect(connection_stringDW)
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
