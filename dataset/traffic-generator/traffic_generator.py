import math

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


def query_llm(question):
    """Consulta al LLM con una pregunta"""
    try:
        # Construir la query combinando título y contenido
        title = str(question['question_title']).strip()
        content = str(question['question_content']).strip()
        
        # Limpiar "nan" strings
        if content.lower() == 'nan':
            content = ""
        
        # Combinar título y contenido
        if content:
            query = f"{title} {content}"
        else:
            query = title
        
        query = query.strip()
        
        # Validar que no esté vacía
        if not query or query.lower() == 'nan':
            print("⚠️ Query vacía, saltando...")
            return "No answer - empty query"
        
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