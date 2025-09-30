FROM python:3.11-slim

WORKDIR /app
# instalar curl por si acaso nose pq xD
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/* 


COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade google-generativeai
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000

# arranca con uvicorn en forma exec (recomendada para FastAPI)
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "5000"]
