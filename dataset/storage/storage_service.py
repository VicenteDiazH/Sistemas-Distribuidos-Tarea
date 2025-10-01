from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from datetime import datetime

app = FastAPI()

# Configuración de la base de datos
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_NAME = os.getenv("DB_NAME", "yahoo_dataset")


class QueryResult(BaseModel):
    question_id: int
    question_title: str
    question_content: str
    best_answer: str
    llm_answer: str
    quality_score: float


def get_db_connection():
    """Crea una conexión a la base de datos"""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME
    )


def init_database():
    """Inicializa la tabla de resultados si no existe"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS query_results (
            id SERIAL PRIMARY KEY,
            question_id INT NOT NULL,
            question_title TEXT,
            question_content TEXT,
            best_answer TEXT,
            llm_answer TEXT,
            quality_score FLOAT,
            access_count INT DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Crear índice para búsquedas rápidas por question_id
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_question_id 
        ON query_results(question_id)
    """)
    
    conn.commit()
    cur.close()
    conn.close()


@app.on_event("startup")
async def startup_event():
    """Inicializar base de datos al arrancar"""
    init_database()
    print("Base de datos inicializada")


@app.get("/health")
def health():
    """Health check endpoint"""
    return {"status": "ok", "service": "storage"}


@app.post("/store")
def store_result(result: QueryResult):
    """
    Almacena o actualiza un resultado de consulta.
    Si la pregunta ya existe, incrementa el contador de accesos.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Verificar si la pregunta ya existe
        cur.execute("""
            SELECT id, access_count FROM query_results 
            WHERE question_id = %s
        """, (result.question_id,))
        
        existing = cur.fetchone()
        
        if existing:
            # Actualizar contador de accesos y timestamps
            result_id = existing[0]
            new_count = existing[1] + 1
            
            cur.execute("""
                UPDATE query_results 
                SET access_count = %s,
                    updated_at = CURRENT_TIMESTAMP,
                    last_accessed = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (new_count, result_id))
            
            conn.commit()
            cur.close()
            conn.close()
            
            return {
                "status": "updated",
                "result_id": result_id,
                "access_count": new_count,
                "message": f"Pregunta duplicada. Contador actualizado a {new_count}"
            }
        else:
            # Insertar nuevo registro
            cur.execute("""
                INSERT INTO query_results 
                (question_id, question_title, question_content, best_answer, 
                 llm_answer, quality_score, access_count)
                VALUES (%s, %s, %s, %s, %s, %s, 1)
                RETURNING id
            """, (
                result.question_id,
                result.question_title,
                result.question_content,
                result.best_answer,
                result.llm_answer,
                result.quality_score
            ))
            
            result_id = cur.fetchone()[0]
            
            conn.commit()
            cur.close()
            conn.close()
            
            return {
                "status": "created",
                "result_id": result_id,
                "access_count": 1,
                "message": "Resultado almacenado exitosamente"
            }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al almacenar: {str(e)}")


@app.get("/stats")
def get_stats():
    """Obtiene estadísticas generales del almacenamiento"""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Total de registros únicos
        cur.execute("SELECT COUNT(*) as total FROM query_results")
        total = cur.fetchone()['total']
        
        # Score promedio
        cur.execute("SELECT AVG(quality_score) as avg_score FROM query_results WHERE quality_score > 0")
        avg_score = cur.fetchone()['avg_score']
        
        # Total de accesos (suma de todos los contadores)
        cur.execute("SELECT SUM(access_count) as total_accesses FROM query_results")
        total_accesses = cur.fetchone()['total_accesses']
        
        # Pregunta más consultada
        cur.execute("""
            SELECT question_id, question_title, access_count 
            FROM query_results 
            ORDER BY access_count DESC 
            LIMIT 1
        """)
        most_accessed = cur.fetchone()
        
        # Score más alto
        cur.execute("""
            SELECT question_id, question_title, quality_score 
            FROM query_results 
            WHERE quality_score > 0
            ORDER BY quality_score DESC 
            LIMIT 1
        """)
        highest_score = cur.fetchone()
        
        # Score más bajo
        cur.execute("""
            SELECT question_id, question_title, quality_score 
            FROM query_results 
            WHERE quality_score > 0
            ORDER BY quality_score ASC 
            LIMIT 1
        """)
        lowest_score = cur.fetchone()
        
        cur.close()
        conn.close()
        
        return {
            "total_unique_questions": total,
            "total_accesses": total_accesses or 0,
            "average_score": float(avg_score) if avg_score else 0.0,
            "most_accessed_question": dict(most_accessed) if most_accessed else None,
            "highest_score_question": dict(highest_score) if highest_score else None,
            "lowest_score_question": dict(lowest_score) if lowest_score else None
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener estadísticas: {str(e)}")


@app.get("/results")
def get_results(limit: int = 100, offset: int = 0):
    """Obtiene resultados almacenados con paginación"""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            SELECT * FROM query_results 
            ORDER BY created_at DESC 
            LIMIT %s OFFSET %s
        """, (limit, offset))
        
        results = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return {
            "results": [dict(row) for row in results],
            "count": len(results),
            "limit": limit,
            "offset": offset
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener resultados: {str(e)}")


@app.get("/result/{question_id}")
def get_result_by_question(question_id: int):
    """Obtiene un resultado específico por question_id"""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            SELECT * FROM query_results 
            WHERE question_id = %s
        """, (question_id,))
        
        result = cur.fetchone()
        
        cur.close()
        conn.close()
        
        if result:
            return dict(result)
        else:
            raise HTTPException(status_code=404, detail="Resultado no encontrado")
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener resultado: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=7000)