import os
import time
import random
import psycopg2
import requests
from datetime import datetime
import numpy as np

# Configuración desde variables de entorno
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_NAME = os.getenv("DB_NAME", "yahoo_dataset")

LLM_SERVICE_URL = os.getenv("LLM_SERVICE_URL", "http://llm:5000/ask")
SCORE_SERVICE_URL = os.getenv("SCORE_SERVICE_URL", "http://score:6000/score")
STORAGE_SERVICE_URL = os.getenv("STORAGE_SERVICE_URL", "http://storage:7000/store")  # VARIABLE DE ENTORNO NUEVA STORAGE
SCORE_METHOD = os.getenv("SCORE_METHOD", "combined")

# Parámetros de distribución
DISTRIBUTION_TYPE = os.getenv("DISTRIBUTION_TYPE", "poisson")
LAMBDA = float(os.getenv("LAMBDA", "5"))
MIN_INTERVAL = int(os.getenv("MIN_INTERVAL", "100"))
MAX_INTERVAL = int(os.getenv("MAX_INTERVAL", "2000"))
TOTAL_QUERIES = int(os.getenv("TOTAL_QUERIES", "100"))

# Estadísticas
stats = {
    "total_sent": 0,
    "successful": 0,
    "failed": 0,
    "start_time": None,
    "intervals": [],
    "total_score": 0.0,
    "score_count": 0,
    "stored_count": 0  # AÑADIDO LA CANTIDAD DE STORED COUNT
}


def connect_db():
    """Conecta a la base de datos PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME
        )
        return conn
    except Exception as e:
        print(f"❌ Error al conectar a la base de datos: {e}")
        raise


def get_random_question(conn):
    """Obtiene una pregunta aleatoria del dataset"""
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, class, question_title, question_content, best_answer
            FROM yahoo_answers
            ORDER BY RANDOM()
            LIMIT 1
        """)
        row = cur.fetchone()
        cur.close()
        
        if not row:
            raise Exception("No hay preguntas en la base de datos")
        
        return {
            "id": row[0],
            "class": row[1],
            "question_title": row[2],
            "question_content": row[3],
            "best_answer": row[4]
        }
    except Exception as e:
        print(f"❌ Error al obtener pregunta: {e}")
        raise


def query_llm(question):
    """Consulta al LLM con una pregunta"""
    try:
        query = f"{question['question_title']} {question['question_content']}"
        
        response = requests.get(
            LLM_SERVICE_URL,
            params={"query": query},
            timeout=30
        )
        response.raise_for_status()
        
        data = response.json()
        return data.get("answer", "")
    
    except Exception as e:
        print(f"❌ Error al consultar LLM: {e}")
        raise


def calculate_score(llm_answer, best_answer):
    """Calcula el score de calidad entre la respuesta del LLM y la mejor respuesta"""
    try:
        response = requests.post(
            SCORE_SERVICE_URL,
            json={
                "llm_answer": llm_answer,
                "best_answer": best_answer,
                "method": SCORE_METHOD
            },
            timeout=10
        )
        response.raise_for_status()
        
        data = response.json()
        
        if SCORE_METHOD == "combined":
            return data.get("recommended_score", 0.0)
        else:
            return data.get("score", 0.0)
    
    except Exception as e:
        print(f"❌ Error al calcular score: {e}")
        return 0.0


# FUNCIÓN AÑADIDA DE ALMACENAR RESULTADO
def store_result(question, llm_answer, quality_score):
    """Almacena el resultado en el servicio de almacenamiento"""
    try:
        response = requests.post(
            STORAGE_SERVICE_URL,
            json={
                "question_id": question['id'],
                "question_title": question['question_title'],
                "question_content": question['question_content'],
                "best_answer": question['best_answer'],
                "llm_answer": llm_answer,
                "quality_score": quality_score
            },
            timeout=10
        )
        response.raise_for_status()
        
        data = response.json()
        return data
    
    except Exception as e:
        print(f"❌ Error al almacenar resultado: {e}")
        return None


def generate_poisson_interval(lambda_rate):
    """Genera un intervalo de tiempo siguiendo una distribución de Poisson"""
    u = random.random()
    interval_seconds = -np.log(1 - u) / lambda_rate
    return interval_seconds


def generate_uniform_interval(min_ms, max_ms):
    """Genera un intervalo uniforme entre min y max milisegundos"""
    return random.uniform(min_ms / 1000, max_ms / 1000)


def print_stats():
    """Imprime estadísticas del generador"""
    if stats["start_time"] is None:
        return
    
    elapsed = time.time() - stats["start_time"]
    rate = stats["total_sent"] / elapsed if elapsed > 0 else 0
    avg_score = stats["total_score"] / stats["score_count"] if stats["score_count"] > 0 else 0.0
    
    print("\n📊 === ESTADÍSTICAS ===")
    print(f"   Total enviadas: {stats['total_sent']}")
    print(f"   Exitosas: {stats['successful']}")
    print(f"   Fallidas: {stats['failed']}")
    print(f"   Almacenadas: {stats['stored_count']}")  # ALMACENADAS
    print(f"   Tiempo transcurrido: {elapsed:.2f}s")
    print(f"   Tasa promedio: {rate:.2f} consultas/s")
    print(f"   Score promedio: {avg_score:.4f}")
    
    if stats["intervals"]:
        avg_interval = np.mean(stats["intervals"])
        std_interval = np.std(stats["intervals"])
        print(f"   Intervalo promedio: {avg_interval*1000:.2f}ms (±{std_interval*1000:.2f}ms)")
    
    print("=" * 30 + "\n")


def generate_traffic():
    """Función principal del generador de tráfico"""
    print("🚀 Iniciando generador de tráfico...")
    print(f"📊 Distribución: {DISTRIBUTION_TYPE}")
    
    if DISTRIBUTION_TYPE == "poisson":
        print(f"📊 Lambda (λ): {LAMBDA} consultas/segundo")
        print(f"📊 Intervalo promedio esperado: {(1/LAMBDA):.3f}s ({(1000/LAMBDA):.2f}ms)")
    else:
        print(f"📊 Intervalo uniforme: {MIN_INTERVAL} - {MAX_INTERVAL} ms")
    
    print(f"📊 Total de consultas a generar: {TOTAL_QUERIES}\n")
    
    conn = connect_db()
    print("✅ Conectado a la base de datos\n")
    
    stats["start_time"] = time.time()
    
    try:
        for i in range(TOTAL_QUERIES):
            try:
                question = get_random_question(conn)
                
                print(f"📤 [{i + 1}/{TOTAL_QUERIES}] Pregunta ID: {question['id']}")
                print(f"   Título: {question['question_title'][:60]}...")
                
                start_query = time.time()
                llm_answer = query_llm(question)
                query_time = time.time() - start_query
                
                print(f"   ✅ Respuesta obtenida en {query_time:.2f}s")
                print(f"   LLM: {llm_answer[:80]}...")
                
                quality_score = calculate_score(llm_answer, question['best_answer'])
                print(f"   🎯 Score de calidad: {quality_score:.4f}")
                
                # ALMACENAR RESULTADOS
                storage_result = store_result(question, llm_answer, quality_score)
                if storage_result:
                    print(f"   💾 {storage_result['message']}")
                    stats["stored_count"] += 1
                
                stats["successful"] += 1
                stats["total_sent"] += 1
                stats["total_score"] += quality_score
                stats["score_count"] += 1
                
                if (i + 1) % 10 == 0:
                    print_stats()
                
                if i < TOTAL_QUERIES - 1:
                    if DISTRIBUTION_TYPE == "poisson":
                        interval = generate_poisson_interval(LAMBDA)
                    else:
                        interval = generate_uniform_interval(MIN_INTERVAL, MAX_INTERVAL)
                    
                    stats["intervals"].append(interval)
                    
                    print(f"   ⏳ Esperando {interval*1000:.2f}ms...\n")
                    time.sleep(interval)
                
            except Exception as e:
                print(f"❌ Error en iteración {i + 1}: {e}\n")
                stats["failed"] += 1
                stats["total_sent"] += 1
        
        print("\n✅ Generación de tráfico completada")
        print_stats()
        
    finally:
        conn.close()
        print("🔌 Conexión cerrada")


if __name__ == "__main__":
    try:
        print("⏳ Esperando que los servicios estén listos...")
        time.sleep(1)
        
        generate_traffic()
        
    except KeyboardInterrupt:
        print("\n⚠️  Generación interrumpida por el usuario")
        print_stats()
    except Exception as e:
        print(f"💥 Error fatal: {e}")
        import traceback
        traceback.print_exc()