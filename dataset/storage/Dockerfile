FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY storage_service.py .

EXPOSE 7000

ENV PYTHONUNBUFFERED=1

CMD ["uvicorn", "storage_service:app", "--host", "0.0.0.0", "--port", "7000"]