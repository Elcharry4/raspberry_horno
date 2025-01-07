# db/db.py

import mysql.connector
from mysql.connector import pooling
import configparser
from threading import Thread
import os

# Obtener la ruta completa del archivo config.ini
config_path = os.path.join(os.path.dirname(__file__), 'config.ini')

# Verificar si el archivo existe
if not os.path.exists(config_path):
    raise FileNotFoundError(f"El archivo config.ini no se encontró en {config_path}")

# Leer la configuración
config = configparser.ConfigParser()
config.read(config_path)

db_config = {
    'host': config['mysql']['host'],
    'port': int(config['mysql']['port']),
    'user': config['mysql']['user'],
    'password': config['mysql']['password'],
    'database': config['mysql']['database'],
}

# Crear el pool de conexiones
connection_pool = pooling.MySQLConnectionPool(
    pool_name=config['mysql'].get('pool_name', 'my_pool'),
    pool_size=int(config['mysql'].get('pool_size', 10)),
    **db_config
)

def get_connection():
    """Obtiene una conexión del pool."""
    return connection_pool.get_connection()

def get_last_value(table_name, column_name, callback):
    """Obtiene el último valor de una columna específica en una tabla."""
    def db_task():
        conn = None
        cursor = None
        try:
            conn = get_connection()
            cursor = conn.cursor()
            query = f"SELECT {column_name} FROM {table_name} ORDER BY id DESC LIMIT 1;"
            cursor.execute(query)
            result = cursor.fetchone()
            value = result[0] if result and result[0] is not None else None
            callback(value, None)
        except Exception as e:
            print(f"Error al obtener el valor de {column_name}: {e}")
            callback(None, e)
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    Thread(target=db_task).start()

def save_record(table_name, data, callback):
    """Guarda un registro en la base de datos en un hilo separado."""
    def db_task():
        conn = None
        cursor = None
        try:
            conn = get_connection()
            cursor = conn.cursor()
            columns = ', '.join(f"`{col}`" for col in data.keys())
            placeholders = ', '.join(['%s'] * len(data))
            values = tuple(data.values())
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders});"
            cursor.execute(query, values)
            conn.commit()
            inserted_id = cursor.lastrowid  # Obtener el ID insertado
            callback(True, None, inserted_id)
        except Exception as e:
            print(f"Error al guardar el registro en {table_name}: {e}")
            callback(False, e, None)
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    Thread(target=db_task).start()

def update_record(table_name, record_id, data, callback):
    """Actualiza un registro en la base de datos en un hilo separado."""
    def db_task():
        conn = None
        cursor = None
        try:
            conn = get_connection()
            cursor = conn.cursor()
            set_clause = ', '.join(f"`{col}` = %s" for col in data.keys())
            values = tuple(data.values())
            query = f"UPDATE {table_name} SET {set_clause} WHERE id = %s;"
            cursor.execute(query, values + (record_id,))
            conn.commit()
            callback(True, None)
        except Exception as e:
            print(f"Error al actualizar el registro en {table_name}: {e}")
            callback(False, e)
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    Thread(target=db_task).start()