from fastapi import FastAPI
from groq import Groq
import os

app = FastAPI()
client = Groq(api_key=os.getenv("GROQ_API_KEY"))

@app.post("/query")
def query(question: str):
    completion = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": question}],
        max_tokens=1024
    )
    return {"answer": completion.choices[0].message.content}