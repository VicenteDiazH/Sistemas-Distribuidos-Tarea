FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements
COPY requirements.txt .

# Instalar dependencias Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código
COPY cache_service.py .

# Exponer puerto
EXPOSE 5002

# Comando de inicio
CMD ["uvicorn", "cache_service:app", "--host", "0.0.0.0", "--port", "5002"]