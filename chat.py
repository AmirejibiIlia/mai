import requests

API_URL = "https://mai-production-cff7.up.railway.app/query"

def ask(company_id: str, question: str):
    response = requests.post(
        API_URL,
        params={
            "company_id": company_id,
            "question": question
        }
    )
    response.raise_for_status()
    data = response.json()
    return data

# Usage
result = ask("Amadeo", "What was the main cost for the company?")
print(result["answer"])
print(f"SQL: {result['sql']}")
print(f"Rows: {result['rows']}")