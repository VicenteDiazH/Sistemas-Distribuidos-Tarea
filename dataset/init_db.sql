-- Tabla para almacenar resultados de queries con LLM
CREATE TABLE IF NOT EXISTS query_results (
    id SERIAL PRIMARY KEY,
    question_id INTEGER NOT NULL,
    question_title TEXT NOT NULL,
    question_content TEXT,
    original_answer TEXT NOT NULL,
    llm_answer TEXT NOT NULL,
    score FLOAT NOT NULL,
    access_count INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índice para búsquedas rápidas por question_id
CREATE INDEX IF NOT EXISTS idx_question_id ON query_results(question_id);

-- Índice para estadísticas
CREATE INDEX IF NOT EXISTS idx_access_count ON query_results(access_count);
CREATE INDEX IF NOT EXISTS idx_created_at ON query_results(created_at);