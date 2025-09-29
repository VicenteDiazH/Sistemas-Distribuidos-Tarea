import os
import pandas as pd
import psycopg2

# Config desde variables de entorno
host = os.getenv("PGHOST", "localhost")
port = os.getenv("PGPORT", "5432")
user = os.getenv("PGUSER", "postgres")
password = os.getenv("PGPASSWORD", "postgres")
database = os.getenv("PGDATABASE", "yahoo_dataset")
csv_path = os.getenv("CSV_PATH", "/data/test.csv")
table_name = os.getenv("TABLE_NAME", "yahoo_answers")

print(f"Conectando a PostgreSQL {host}:{port}, db={database}, tabla={table_name}")
print(f"Leyendo CSV desde {csv_path} ...")

# Leer CSV
df = pd.read_csv(csv_path)

# Conexión
conn = psycopg2.connect(
    host=host, port=port, user=user, password=password, dbname=database
)
cur = conn.cursor()

# Crear tabla (drop + create para evitar duplicados)
cur.execute(f"""
    DROP TABLE IF EXISTS {table_name};
    CREATE TABLE {table_name} (
        id SERIAL PRIMARY KEY,
        class INT,
        question_title TEXT,
        question_content TEXT,
        best_answer TEXT
    );
""")

# Insertar filas
for _, row in df.iterrows():
    cur.execute(
        f"""
        INSERT INTO {table_name} (class, question_title, question_content, best_answer)
        VALUES (%s, %s, %s, %s)
        """,
        (int(row[0]), str(row[1]), str(row[2]), str(row[3])),
    )

conn.commit()
cur.close()
conn.close()

print("✅ Datos cargados en la tabla", table_name)
