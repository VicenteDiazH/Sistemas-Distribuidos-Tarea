from fastapi import FastAPI, HTTPException
import requests
import os
import traceback

# Configuración
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2:1b")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "60"))

app = FastAPI()

print(f"🦙 LLM Service configurado:")
print(f"   - Ollama URL: {OLLAMA_URL}")
print(f"   - Model: {OLLAMA_MODEL}")
print(f"   - Timeout: {REQUEST_TIMEOUT}s")

query_count = 0


@app.get("/health")
def health():
    """Health check endpoint"""
    try:
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
    """Genera respuesta usando Ollama (Llama 3.2)"""
    global query_count
    
    try:
        # Validar query
        if not query or query.strip() == "" or query.lower() == "nan":
            raise HTTPException(status_code=400, detail="Query cannot be empty or 'nan'")
        
        # Limpiar query
        query = query.strip()
        
        print(f"📥 Query recibida: {query[:100]}...")
        
        # Preparar prompt
        prompt = f"Answer the following question briefly and accurately:\n\nQuestion: {query}\n\nAnswer:"
        
        # Consultar Ollama
        print("🔄 Consultando a Ollama...")
        response = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.7,
                    "top_p": 0.9,
                    "num_predict": 200,
                }
            },
            timeout=REQUEST_TIMEOUT
        )
        
        if response.status_code != 200:
            print(f"❌ Ollama error: {response.status_code} - {response.text}")
            raise HTTPException(
                status_code=response.status_code, 
                detail=f"Ollama error: {response.text}"
            )
        
        result = response.json()
        answer = result.get("response", "").strip()
        
        query_count += 1
        
        print(f"✅ Respuesta generada: {answer[:100]}...")
        
        return {
            "answer": answer,
            "model": OLLAMA_MODEL,
            "query_count": query_count,
            "source": "ollama"
        }
        
    except HTTPException:
        raise
    except requests.exceptions.Timeout:
        print(f"⏱️ Timeout al consultar Ollama")
        raise HTTPException(status_code=504, detail="Request timeout - model may be loading")
    except requests.exceptions.ConnectionError as e:
        print(f"🔌 Error de conexión con Ollama: {e}")
        raise HTTPException(status_code=503, detail="Cannot connect to Ollama service")
    except Exception as e:
        print(f"💥 Error inesperado: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")


@app.get("/stats")
def stats():
    """Estadísticas del servicio"""
    return {
        "model": OLLAMA_MODEL,
        "ollama_url": OLLAMA_URL,
        "total_queries": query_count,
        "timeout": REQUEST_TIMEOUT
    }