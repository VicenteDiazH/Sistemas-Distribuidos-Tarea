from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import re

app = FastAPI()

# Modelo para la request
class ScoreRequest(BaseModel):
    llm_answer: str
    best_answer: str
    method: str = "tfidf"  # Opciones: "tfidf", "jaccard", "levenshtein"


def preprocess_text(text: str) -> str:
    """Limpia y normaliza el texto"""
    if not text:
        return ""
    
    # Convertir a minúsculas
    text = text.lower()
    
    # Remover puntuación extra
    text = re.sub(r'[^\w\s]', ' ', text)
    
    # Remover espacios múltiples
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()


def calculate_tfidf_similarity(text1: str, text2: str) -> float:
    """
    Calcula similitud usando TF-IDF y cosine similarity.
    Captura similitud semántica basada en términos importantes.
    """
    if not text1 or not text2:
        return 0.0
    
    # Preprocesar textos
    text1 = preprocess_text(text1)
    text2 = preprocess_text(text2)
    
    if not text1 or not text2:
        return 0.0
    
    try:
        # Vectorizar usando TF-IDF
        vectorizer = TfidfVectorizer()
        tfidf_matrix = vectorizer.fit_transform([text1, text2])
        
        # Calcular similitud de coseno
        similarity = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:2])[0][0]
        
        return float(similarity)
    
    except Exception as e:
        print(f"Error en TF-IDF: {e}")
        return 0.0


def calculate_jaccard_similarity(text1: str, text2: str) -> float:
    """
    Calcula índice de Jaccard basado en conjuntos de palabras.
    Mide superposición de vocabulario.
    """
    if not text1 or not text2:
        return 0.0
    
    # Preprocesar y tokenizar
    words1 = set(preprocess_text(text1).split())
    words2 = set(preprocess_text(text2).split())
    
    if not words1 or not words2:
        return 0.0
    
    # Calcular intersección y unión
    intersection = len(words1.intersection(words2))
    union = len(words1.union(words2))
    
    if union == 0:
        return 0.0
    
    return intersection / union


def calculate_levenshtein_similarity(text1: str, text2: str) -> float:
    """
    Calcula similitud basada en distancia de Levenshtein normalizada.
    Mide similitud a nivel de caracteres.
    """
    if not text1 or not text2:
        return 0.0
    
    text1 = preprocess_text(text1)
    text2 = preprocess_text(text2)
    
    if not text1 or not text2:
        return 0.0
    
    # Implementación de distancia de Levenshtein
    m, n = len(text1), len(text2)
    dp = [[0] * (n + 1) for _ in range(m + 1)]
    
    for i in range(m + 1):
        dp[i][0] = i
    for j in range(n + 1):
        dp[0][j] = j
    
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            if text1[i-1] == text2[j-1]:
                dp[i][j] = dp[i-1][j-1]
            else:
                dp[i][j] = 1 + min(dp[i-1][j], dp[i][j-1], dp[i-1][j-1])
    
    distance = dp[m][n]
    max_len = max(len(text1), len(text2))
    
    if max_len == 0:
        return 1.0
    
    # Convertir distancia a similitud (1 = idéntico, 0 = completamente diferente)
    similarity = 1 - (distance / max_len)
    return max(0.0, similarity)


def calculate_combined_score(text1: str, text2: str) -> dict:
    """
    Calcula múltiples métricas y retorna un score combinado.
    """
    tfidf_score = calculate_tfidf_similarity(text1, text2)
    jaccard_score = calculate_jaccard_similarity(text1, text2)
    levenshtein_score = calculate_levenshtein_similarity(text1, text2)
    
    # Score combinado (promedio ponderado)
    # TF-IDF tiene más peso porque captura mejor similitud semántica
    combined = (0.5 * tfidf_score) + (0.3 * jaccard_score) + (0.2 * levenshtein_score)
    
    return {
        "tfidf": round(tfidf_score, 4),
        "jaccard": round(jaccard_score, 4),
        "levenshtein": round(levenshtein_score, 4),
        "combined": round(combined, 4)
    }


@app.get("/health")
def health():
    """Health check endpoint"""
    return {"status": "ok", "service": "score"}


@app.post("/score")
def calculate_score(request: ScoreRequest):
    """
    Calcula el score de similitud entre dos respuestas.
    
    Métodos disponibles:
    - tfidf: Similitud semántica usando TF-IDF (recomendado)
    - jaccard: Superposición de vocabulario
    - levenshtein: Similitud a nivel de caracteres
    - combined: Combina las tres métricas
    """
    try:
        llm_answer = request.llm_answer
        best_answer = request.best_answer
        method = request.method.lower()
        
        if not llm_answer or not best_answer:
            raise HTTPException(status_code=400, detail="Ambas respuestas deben ser no vacías")
        
        if method == "tfidf":
            score = calculate_tfidf_similarity(llm_answer, best_answer)
            return {
                "score": round(score, 4),
                "method": "tfidf",
                "llm_answer_length": len(llm_answer),
                "best_answer_length": len(best_answer)
            }
        
        elif method == "jaccard":
            score = calculate_jaccard_similarity(llm_answer, best_answer)
            return {
                "score": round(score, 4),
                "method": "jaccard",
                "llm_answer_length": len(llm_answer),
                "best_answer_length": len(best_answer)
            }
        
        elif method == "levenshtein":
            score = calculate_levenshtein_similarity(llm_answer, best_answer)
            return {
                "score": round(score, 4),
                "method": "levenshtein",
                "llm_answer_length": len(llm_answer),
                "best_answer_length": len(best_answer)
            }
        
        elif method == "combined":
            scores = calculate_combined_score(llm_answer, best_answer)
            return {
                "scores": scores,
                "method": "combined",
                "recommended_score": scores["combined"],
                "llm_answer_length": len(llm_answer),
                "best_answer_length": len(best_answer)
            }
        
        else:
            raise HTTPException(status_code=400, detail=f"Método desconocido: {method}. Use: tfidf, jaccard, levenshtein, o combined")
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error calculando score: {str(e)}")


@app.get("/methods")
def get_methods():
    """Retorna información sobre los métodos disponibles"""
    return {
        "methods": {
            "tfidf": {
                "name": "TF-IDF + Cosine Similarity",
                "description": "Captura similitud semántica basada en términos importantes",
                "range": "0.0 - 1.0",
                "recommended": True
            },
            "jaccard": {
                "name": "Jaccard Index",
                "description": "Mide superposición de vocabulario entre respuestas",
                "range": "0.0 - 1.0",
                "recommended": False
            },
            "levenshtein": {
                "name": "Levenshtein Distance",
                "description": "Similitud a nivel de caracteres",
                "range": "0.0 - 1.0",
                "recommended": False
            },
            "combined": {
                "name": "Combined Score",
                "description": "Promedio ponderado de las tres métricas (50% TF-IDF, 30% Jaccard, 20% Levenshtein)",
                "range": "0.0 - 1.0",
                "recommended": True
            }
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=6000)