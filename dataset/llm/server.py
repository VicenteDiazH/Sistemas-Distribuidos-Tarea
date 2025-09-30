from fastapi import FastAPI, HTTPException
import requests
import os

# Configuración
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2:1b")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "60"))

app = FastAPI()

print(f"🦙 LLM Service configurado:")
print(f"   - Ollama URL: {OLLAMA_URL}")
print(f"   - Model: {OLLAMA_MODEL}")
print(f"   - Timeout: {REQUEST_TIMEOUT}s")

# Contador de consultas
query_count = 0


@app.get("/health")
def health():
    """Health check endpoint"""
    try:
        # Verificar que Ollama esté disponible
        response = requests.get(f"{OLLAMA_URL}/api/tags", timeout=2)
        ollama_status = "ok" if response.status_code == 200 else "error"
    except:
        ollama_status = "unreachable"
    
    return {
        "status": "ok",
        "ollama_status": ollama_status,
        "model": OLLAMA_MODEL,
        "queries": query_count
    }


@app.get("/ask")
async def ask(query: str):
    """
    Genera respuesta usando Ollama (Llama 3.2)
    
    Parámetros:
    - query: La pregunta a responder
    
    Retorna:
    - answer: La respuesta generada
    - model: Modelo usado
    - query_count: Total de consultas realizadas
    """
    global query_count
    
    try:
        # Preparar prompt optimizado para respuestas concisas
        prompt = f"Answer the following question briefly and accurately:\n\nQuestion: {query}\n\nAnswer:"
        
        # Consultar Ollama
        response = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.7,      # Creatividad moderada
                    "top_p": 0.9,
                    "num_predict": 200,      # Máximo 200 tokens (respuestas cortas)
                }
            },
            timeout=REQUEST_TIMEOUT
        )
        
        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code, 
                detail=f"Ollama error: {response.text}"
            )
        
        result = response.json()
        answer = result.get("response", "").strip()
        
        query_count += 1
        
        return {
            "answer": answer,
            "model": OLLAMA_MODEL,
            "query_count": query_count,
            "source": "ollama"
        }
        
    except requests.exceptions.Timeout:
        raise HTTPException(status_code=504, detail="Request timeout - model may be loading")
    except requests.exceptions.ConnectionError:
        raise HTTPException(status_code=503, detail="Cannot connect to Ollama service")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@app.get("/stats")
def stats():
    """Estadísticas del servicio"""
    return {
        "model": OLLAMA_MODEL,
        "ollama_url": OLLAMA_URL,
        "total_queries": query_count,
        "timeout": REQUEST_TIMEOUT
    }