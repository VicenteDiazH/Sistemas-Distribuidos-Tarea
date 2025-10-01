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

CACHE_SERVICE_URL = os.getenv("CACHE_SERVICE_URL", "http://cache:5002/query")

# Parámetros de distribución
DISTRIBUTION_TYPE = os.getenv("DISTRIBUTION_TYPE", "poisson")  # 'poisson' o 'uniform'
LAMBDA = float(os.getenv("LAMBDA", "5"))  # Para Poisson (consultas/segundo)
MIN_INTERVAL = int(os.getenv("MIN_INTERVAL", "100"))  # Para uniforme (ms)
MAX_INTERVAL = int(os.getenv("MAX_INTERVAL", "2000"))  # Para uniforme (ms)
TOTAL_QUERIES = int(os.getenv("TOTAL_QUERIES", "100"))

# Estadísticas
stats = {
    "total_sent": 0,
    "cache_hits": 0,
    "cache_misses": 0,
    "failed": 0,
    "start_time": None,
    "intervals": [],
    "total_score": 0.0,
    "score_count": 0,
    "hit_times": [],
    "miss_times": []
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
        
        # Manejar NaN en question_content
        question_content = row[3]
        
        # Si es None o string "nan", convertir a string vacío
        if question_content is None or str(question_content).lower() == 'nan':
            question_content = ""
        
        return {
            "id": row[0],
            "class": row[1],
            "question_title": row[2],
            "question_content": question_content,
            "best_answer": row[4]
        }
    except Exception as e:
        print(f"❌ Error al obtener pregunta: {e}")
        raise


def query_cache(question):
    """
    Consulta al sistema de caché.
    El cache se encarga de: verificar si existe, consultar LLM si no existe, 
    calcular score y guardar en storage.
    """
    try:
        # Construir la query combinando título y contenido
        title = str(question['question_title']).strip()
        content = str(question['question_content']).strip()
        
        # Limpiar "nan" strings
        if content.lower() == 'nan':
            content = ""
        
        # Validar título
        if not title or title.lower() == 'nan':
            print("⚠️ Query vacía, saltando...")
            return {
                "status": "error",
                "error": "empty_query"
            }
        
        # Hacer request al cache
        response = requests.get(
            CACHE_SERVICE_URL,
            params={
                "question_id": question["id"],
                "question_title": title,
                "question_content": content,
                "original_answer": question["best_answer"]
            },
            timeout=90  # Timeout largo porque el LLM puede tardar
        )
        response.raise_for_status()
        
        return response.json()
    
    except requests.exceptions.Timeout:
        print("⏱️ Timeout al consultar cache")
        return {
            "status": "error",
            "error": "timeout"
        }
    except Exception as e:
        print(f"❌ Error al consultar cache: {e}")
        return {
            "status": "error",
            "error": str(e)
        }


def generate_poisson_interval(lambda_rate):
    """
    Genera un intervalo de tiempo siguiendo una distribución de Poisson.
    Usa distribución exponencial para tiempos entre eventos.
    """
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
    
    hit_rate = (stats["cache_hits"] / stats["total_sent"] * 100) if stats["total_sent"] > 0 else 0
    miss_rate = (stats["cache_misses"] / stats["total_sent"] * 100) if stats["total_sent"] > 0 else 0
    
    avg_hit_time = np.mean(stats["hit_times"]) if stats["hit_times"] else 0
    avg_miss_time = np.mean(stats["miss_times"]) if stats["miss_times"] else 0
    
    print("\n" + "="*60)
    print("📊 ESTADÍSTICAS DEL GENERADOR DE TRÁFICO")
    print("="*60)
    print(f"Total consultas:       {stats['total_sent']}")
    print(f"Cache Hits:            {stats['cache_hits']} ({hit_rate:.1f}%)")
    print(f"Cache Misses:          {stats['cache_misses']} ({miss_rate:.1f}%)")
    print(f"Fallidas:              {stats['failed']}")
    print(f"Tiempo transcurrido:   {elapsed:.2f}s")
    print(f"Tasa promedio:         {rate:.2f} consultas/s")
    
    if stats["score_count"] > 0:
        print(f"Score promedio:        {avg_score:.4f}")
    
    if stats["hit_times"] and stats["miss_times"]:
        print(f"\nTiempos de respuesta:")
        print(f"  Hit promedio:        {avg_hit_time:.3f}s")
        print(f"  Miss promedio:       {avg_miss_time:.3f}s")
        print(f"  Mejora por cache:    {avg_miss_time/avg_hit_time:.1f}x más rápido")
    
    if stats["intervals"]:
        avg_interval = np.mean(stats["intervals"])
        std_interval = np.std(stats["intervals"])
        print(f"\nIntervalos entre queries:")
        print(f"  Promedio:            {avg_interval*1000:.2f}ms")
        print(f"  Desv. estándar:      {std_interval*1000:.2f}ms")
    
    print("="*60 + "\n")


def generate_traffic():
    """Función principal del generador de tráfico"""
    print("="*60)
    print("🚀 GENERADOR DE TRÁFICO - Sistema con Cache")
    print("="*60)
    print(f"Distribución:          {DISTRIBUTION_TYPE}")
    
    if DISTRIBUTION_TYPE == "poisson":
        print(f"Lambda (λ):            {LAMBDA} consultas/segundo")
        print(f"Intervalo esperado:    {(1/LAMBDA):.3f}s ({(1000/LAMBDA):.2f}ms)")
    else:
        print(f"Intervalo uniforme:    {MIN_INTERVAL} - {MAX_INTERVAL} ms")
    
    print(f"Total consultas:       {TOTAL_QUERIES}")
    print(f"Cache URL:             {CACHE_SERVICE_URL}")
    print("="*60 + "\n")
    
    # Esperar a que el cache esté listo
    print("⏳ Esperando a que cache esté disponible...")
    for i in range(30):
        try:
            health_check = requests.get("http://cache:5002/health", timeout=2)
            if health_check.status_code == 200:
                print("✅ Cache está listo\n")
                break
        except:
            pass
        time.sleep(1)
    else:
        print("❌ Cache no está disponible después de 30 segundos")
        return
    
    # Conectar a la base de datos
    conn = connect_db()
    print("✅ Conectado a PostgreSQL\n")
    
    stats["start_time"] = time.time()
    
    try:
        for i in range(TOTAL_QUERIES):
            try:
                # Obtener pregunta aleatoria
                question = get_random_question(conn)
                
                print(f"📤 Query [{i + 1}/{TOTAL_QUERIES}]")
                print(f"   ID: {question['id']}")
                print(f"   Título: {question['question_title'][:70]}...")
                
                # Consultar al cache
                start_time = time.time()
                result = query_cache(question)
                query_time = time.time() - start_time
                
                # Procesar resultado
                if result.get("status") == "error":
                    print(f"   ❌ Error: {result.get('error')}\n")
                    stats["failed"] += 1
                    stats["total_sent"] += 1
                    continue
                
                stats["total_sent"] += 1
                
                if result.get("status") == "hit":
                    # Cache HIT
                    stats["cache_hits"] += 1
                    stats["hit_times"].append(query_time)
                    print(f"   🎯 CACHE HIT (accesos: {result.get('access_count', 1)})")
                    print(f"   ⚡ Tiempo: {query_time:.3f}s")
                    
                elif result.get("status") == "miss":
                    # Cache MISS
                    stats["cache_misses"] += 1
                    stats["miss_times"].append(query_time)
                    score = result.get("score", 0.0)
                    stats["total_score"] += score
                    stats["score_count"] += 1
                    
                    print(f"   ❌ CACHE MISS")
                    print(f"   ⏱️ Tiempo: {query_time:.3f}s")
                    print(f"   🎯 Score: {score:.4f}")
                    print(f"   📦 Cache: {result.get('cache_size', 0)}/100")
                
                # Mostrar estadísticas cada 10 consultas
                if (i + 1) % 10 == 0:
                    print_stats()
                
                # Calcular siguiente intervalo (solo si no es la última consulta)
                if i < TOTAL_QUERIES - 1:
                    if DISTRIBUTION_TYPE == "poisson":
                        interval = generate_poisson_interval(LAMBDA)
                    else:
                        interval = generate_uniform_interval(MIN_INTERVAL, MAX_INTERVAL)
                    
                    stats["intervals"].append(interval)
                    print(f"   ⏳ Esperando {interval*1000:.0f}ms...\n")
                    time.sleep(interval)
                else:
                    print()
                
            except Exception as e:
                print(f"❌ Error en iteración {i + 1}: {e}\n")
                stats["failed"] += 1
                stats["total_sent"] += 1
        
        print("\n✅ Generación de tráfico completada\n")
        print_stats()
        
        # Obtener estadísticas finales del cache
        try:
            cache_stats = requests.get("http://cache:5002/stats", timeout=5)
            if cache_stats.status_code == 200:
                data = cache_stats.json()
                print("="*60)
                print("📊 ESTADÍSTICAS FINALES DEL CACHE")
                print("="*60)
                print(f"Tamaño actual:         {data['cache_size']}/{data['max_size']}")
                print(f"Política:              {data['policy']}")
                print(f"TTL:                   {data['ttl']}s")
                print(f"Hit rate:              {data['hit_rate']}")
                print(f"Miss rate:             {data['miss_rate']}")
                print(f"Total evictions:       {data['evictions']}")
                print("="*60 + "\n")
        except:
            pass
        
    finally:
        conn.close()
        print("🔌 Conexión a BD cerrada")


if __name__ == "__main__":
    try:
        generate_traffic()
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Generación interrumpida por el usuario")
        print_stats()
    except Exception as e:
        print(f"\n💥 Error fatal: {e}")
        import traceback
        traceback.print_exc()