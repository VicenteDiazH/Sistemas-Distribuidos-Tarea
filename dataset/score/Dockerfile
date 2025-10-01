FROM python:3.11-slim

WORKDIR /app

# Instalar curl para health check
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Copiar requirements
COPY requirements.txt .

# Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código
COPY score_service.py .

# Exponer puerto
EXPOSE 6000

# Variable de entorno para output sin buffer
ENV PYTHONUNBUFFERED=1

# Ejecutar servidor
CMD ["uvicorn", "score_service:app", "--host", "0.0.0.0", "--port", "6000"]