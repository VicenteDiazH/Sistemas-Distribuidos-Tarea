from fastapi import FastAPI
import google.generativeai as genai
import os

genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))

print("📌 Modelos disponibles:")
for m in genai.list_models():
    print(f"- {m.name} -> {m.supported_generation_methods}")

app = FastAPI()

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/ask")
async def ask(query: str):
    try:
        model = genai.GenerativeModel("gemini-2.5-flash-lite")
        response = model.generate_content(query)
        return {"answer": response.text}
    except Exception as e:
        return {"error": str(e)}
