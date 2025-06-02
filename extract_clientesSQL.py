import pandas as pd
import pymysql
from pymysql.constants import CLIENT
import numpy as np

# Configuración
config = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '2025',
    'database': 'clientes_mf',
    'charset': 'utf8mb4'
}

# Ruta al archivo CSV
csv_file = 'C:/Users/movin/Downloads/extraccion_odoo/clientes_odoo.csv'

# 1. Leer CSV
df = pd.read_csv(csv_file)
print(f"CSV cargado: {len(df)} filas")

# 2. Conexión con verificación explícita
try:
    connection = pymysql.connect(**config)
    cursor = connection.cursor()
    
    # Verificación crítica
    cursor.execute("SELECT DATABASE()")
    db = cursor.fetchone()[0]
    print(f"Conectado a la base de datos: {db}")
    
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    print(f"Tablas existentes: {tables}")

    # 3. Crear tabla (con eliminación previa si existe)
    cursor.execute("DROP TABLE IF EXISTS clientes")
    
    create_table = "CREATE TABLE clientes ("
    for col in df.columns:
        dtype = 'TEXT' if df[col].dtype == 'object' else 'FLOAT' if 'float' in str(df[col].dtype) else 'INT'
        create_table += f"`{col}` {dtype}, "
    create_table = create_table[:-2] + ")"
    
    cursor.execute(create_table)
    print("Tabla creada:\n", create_table)

    # 4. Inserción con verificación inmediata
    df = df.replace({np.nan: None})
    cols = ", ".join([f"`{c}`" for c in df.columns])
    vals = ", ".join(["%s"]*len(df.columns))
    
    for i, row in enumerate(df.itertuples(index=False), 1):
        try:
            cursor.execute(f"INSERT INTO clientes ({cols}) VALUES ({vals})", row)
            if i % 100 == 0:
                connection.commit()
                print(f"Insertadas {i} filas")
        except Exception as e:
            print(f"Error en fila {i}: {e}\nDatos: {row}")
    
    connection.commit()
    print(f"Total filas insertadas: {len(df)}")

    # 5. Verificación POST inserción
    cursor.execute("SELECT COUNT(*) FROM clientes")
    count = cursor.fetchone()[0]
    print(f"Filas en tabla después de inserción: {count}")
    
    # Mostrar primeras filas para verificación
    cursor.execute("SELECT * FROM clientes LIMIT 5")
    print("\nPrimeras 5 filas en la tabla:")
    for row in cursor.fetchall():
        print(row)

except Exception as e:
    print(f"Error: {e}")
finally:
    if connection.open:
        cursor.close()
        connection.close()