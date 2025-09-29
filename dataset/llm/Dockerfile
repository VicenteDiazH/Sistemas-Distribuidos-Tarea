FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade google-generativeai
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000

# arranca con uvicorn en forma exec (recomendada para FastAPI)
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "5000"]
