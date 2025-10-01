from fastapi import FastAPI, HTTPException
import requests
import os
import time
import hashlib
from collections import OrderedDict
from datetime import datetime
import psycopg2
from typing import Optional, Dict, Any

# Configuración
CACHE_SIZE = int(os.getenv("CACHE_SIZE", "100"))
CACHE_POLICY = os.getenv("CACHE_POLICY", "LRU")  # LRU, LFU, FIFO
CACHE_TTL = int(os.getenv("CACHE_TTL", "3600"))  # segundos
LLM_SERVICE_URL = os.getenv("LLM_SERVICE_URL", "http://llm:5000/ask")
SCORE_SERVICE_URL = os.getenv("SCORE_SERVICE_URL", "http://score:6000/score")
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_NAME = os.getenv("DB_NAME", "yahoo_dataset")

app = FastAPI()

# Estadísticas
stats = {
    "hits": 0,
    "misses": 0,
    "total_queries": 0,
    "evictions": 0
}


class CacheEntry:
    """Entrada de caché con metadata"""
    def __init__(self, key: str, value: Any, original_answer: str):
        self.key = key
        self.value = value
        self.original_answer = original_answer
        self.timestamp = time.time()
        self.access_count = 1
        self.last_access = time.time()


class CacheSystem:
    """Sistema de caché con múltiples políticas"""
    
    def __init__(self, max_size: int, policy: str, ttl: int):
        self.max_size = max_size
        self.policy = policy.upper()
        self.ttl = ttl
        self.cache: Dict[str, CacheEntry] = {}
        self.access_order = OrderedDict()  # Para LRU y FIFO
        
    def _generate_key(self, question_title: str, question_content: str) -> str:
        """Genera clave única para la pregunta"""
        combined = f"{question_title}|{question_content}".strip().lower()
        return hashlib.md5(combined.encode()).hexdigest()
    
    def _is_expired(self, entry: CacheEntry) -> bool:
        """Verifica si una entrada ha expirado"""
        return (time.time() - entry.timestamp) > self.ttl
    
    def _evict(self):
        """Elimina una entrada según la política configurada"""
        if not self.cache:
            return
        
        key_to_remove = None
        
        if self.policy == "LRU":
            # Least Recently Used
            key_to_remove = next(iter(self.access_order))
            
        elif self.policy == "LFU":
            # Least Frequently Used
            key_to_remove = min(self.cache.items(), 
                              key=lambda x: x[1].access_count)[0]
            
        elif self.policy == "FIFO":
            # First In First Out
            key_to_remove = next(iter(self.access_order))
        
        if key_to_remove:
            del self.cache[key_to_remove]
            if key_to_remove in self.access_order:
                del self.access_order[key_to_remove]
            stats["evictions"] += 1
            print(f"🗑️ Evicted key: {key_to_remove[:8]}... (Policy: {self.policy})")
    
    def get(self, question_title: str, question_content: str) -> Optional[Dict]:
        """Obtiene entrada del caché"""
        key = self._generate_key(question_title, question_content)
        
        if key not in self.cache:
            return None
        
        entry = self.cache[key]
        
        # Verificar expiración
        if self._is_expired(entry):
            del self.cache[key]
            if key in self.access_order:
                del self.access_order[key]
            return None
        
        # Actualizar estadísticas de acceso
        entry.access_count += 1
        entry.last_access = time.time()
        
        # Actualizar orden de acceso para LRU
        if self.policy == "LRU":
            self.access_order.move_to_end(key)
        
        return {
            "llm_answer": entry.value,
            "original_answer": entry.original_answer,
            "access_count": entry.access_count,
            "cached_at": entry.timestamp
        }
    
    def put(self, question_title: str, question_content: str, 
            llm_answer: str, original_answer: str):
        """Agrega entrada al caché"""
        key = self._generate_key(question_title, question_content)
        
        # Si ya existe, actualizar
        if key in self.cache:
            self.cache[key].access_count += 1
            self.cache[key].last_access = time.time()
            return
        
        # Si está lleno, evict
        if len(self.cache) >= self.max_size:
            self._evict()
        
        # Agregar nueva entrada
        entry = CacheEntry(key, llm_answer, original_answer)
        self.cache[key] = entry
        self.access_order[key] = True
    
    def size(self) -> int:
        """Retorna tamaño actual del caché"""
        return len(self.cache)
    
    def clear(self):
        """Limpia el caché"""
        self.cache.clear()
        self.access_order.clear()


# Inicializar caché
cache = CacheSystem(CACHE_SIZE, CACHE_POLICY, CACHE_TTL)

print(f"💾 Cache Service configurado:")
print(f"   - Size: {CACHE_SIZE}")
print(f"   - Policy: {CACHE_POLICY}")
print(f"   - TTL: {CACHE_TTL}s")


def get_db_connection():
    """Crea conexión a PostgreSQL"""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )


def update_storage_hit(question_id: int, access_count: int):
    """Actualiza contador de accesos en la BD (cache hit)"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            UPDATE query_results 
            SET access_count = %s, 
                last_accessed = NOW()
            WHERE question_id = %s
        """, (access_count, question_id))
        
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"✅ Storage updated (HIT): question_id={question_id}, count={access_count}")
    except Exception as e:
        print(f"❌ Error updating storage: {e}")


def save_to_storage(question_id: int, question_title: str, question_content: str,
                   original_answer: str, llm_answer: str, score: float):
    """Guarda resultado en la BD (cache miss)"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Verificar si ya existe
        cur.execute("""
            SELECT id, access_count FROM query_results 
            WHERE question_id = %s
        """, (question_id,))
        
        existing = cur.fetchone()
        
        if existing:
            # Actualizar contador
            cur.execute("""
                UPDATE query_results 
                SET access_count = access_count + 1,
                    last_accessed = NOW()
                WHERE question_id = %s
            """, (question_id,))
        else:
            # Insertar nuevo registro
            cur.execute("""
                INSERT INTO query_results 
                (question_id, question_title, question_content, 
                 original_answer, llm_answer, score, access_count)
                VALUES (%s, %s, %s, %s, %s, %s, 1)
            """, (question_id, question_title, question_content, 
                  original_answer, llm_answer, score))
        
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"✅ Saved to storage: question_id={question_id}, score={score:.3f}")
    except Exception as e:
        print(f"❌ Error saving to storage: {e}")


@app.get("/health")
def health():
    """Health check"""
    return {
        "status": "ok",
        "cache_size": cache.size(),
        "max_size": CACHE_SIZE,
        "policy": CACHE_POLICY,
        "stats": stats
    }


@app.get("/query")
async def process_query(
    question_id: int,
    question_title: str,
    question_content: str = "",
    original_answer: str = ""
):
    """
    Procesa query: verifica caché, consulta LLM si es necesario, calcula score
    """
    stats["total_queries"] += 1
    
    try:
        # 1. Verificar caché
        cached = cache.get(question_title, question_content)
        
        if cached:
            # CACHE HIT
            stats["hits"] += 1
            print(f"🎯 CACHE HIT: {question_title[:50]}... (count: {cached['access_count']})")
            
            # Actualizar storage con nuevo contador
            update_storage_hit(question_id, cached['access_count'])
            
            return {
                "status": "hit",
                "question_id": question_id,
                "llm_answer": cached["llm_answer"],
                "access_count": cached["access_count"],
                "from_cache": True
            }
        
        # 2. CACHE MISS - Consultar LLM
        stats["misses"] += 1
        print(f"❌ CACHE MISS: {question_title[:50]}...")
        
        # Construir query para LLM
        title = str(question_title).strip()
        content = str(question_content).strip()
        
        if content.lower() == 'nan' or not content:
            content = ""
        
        query = f"{title} {content}".strip()
        
        if not query or query.lower() == 'nan':
            raise HTTPException(status_code=400, detail="Invalid query")
        
        # Consultar LLM
        print(f"🔄 Querying LLM...")
        llm_response = requests.get(
            LLM_SERVICE_URL,
            params={"query": query},
            timeout=60
        )
        llm_response.raise_for_status()
        llm_answer = llm_response.json().get("answer", "")
        
        # 3. Calcular Score
        print(f"📊 Calculating score...")
        score_response = requests.post(
            SCORE_SERVICE_URL,
            json={
                "llm_answer": llm_answer,
                "best_answer": original_answer,
                "method": "tfidf"  # o "combined" para usar todas las métricas
            },
            timeout=10
        )
        score_response.raise_for_status()
        score = score_response.json().get("score", 0.0)
        
        # 4. Guardar en caché
        cache.put(question_title, question_content, llm_answer, original_answer)
        
        # 5. Guardar en storage
        save_to_storage(
            question_id, question_title, question_content,
            original_answer, llm_answer, score
        )
        
        print(f"✅ MISS processed: score={score:.3f}, cached={cache.size()}/{CACHE_SIZE}")
        
        return {
            "status": "miss",
            "question_id": question_id,
            "llm_answer": llm_answer,
            "score": score,
            "access_count": 1,
            "from_cache": False,
            "cache_size": cache.size()
        }
        
    except Exception as e:
        print(f"💥 Error processing query: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats")
def get_stats():
    """Obtiene estadísticas del caché"""
    total = stats["total_queries"]
    hit_rate = (stats["hits"] / total * 100) if total > 0 else 0
    miss_rate = (stats["misses"] / total * 100) if total > 0 else 0
    
    return {
        "cache_size": cache.size(),
        "max_size": CACHE_SIZE,
        "policy": CACHE_POLICY,
        "ttl": CACHE_TTL,
        "hits": stats["hits"],
        "misses": stats["misses"],
        "total_queries": total,
        "hit_rate": f"{hit_rate:.2f}%",
        "miss_rate": f"{miss_rate:.2f}%",
        "evictions": stats["evictions"]
    }


@app.post("/clear")
def clear_cache():
    """Limpia el caché (útil para experimentos)"""
    cache.clear()
    stats["hits"] = 0
    stats["misses"] = 0
    stats["total_queries"] = 0
    stats["evictions"] = 0
    return {"status": "cleared"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5002)