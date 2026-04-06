from fastapi import FastAPI
from anthropic import Anthropic
import os

app = FastAPI()
client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

@app.post("/query")
def query(question: str):
    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1024,
        messages=[{"role": "user", "content": question}]
    )
    return {"answer": message.content[0].text}