# mai — AI SQL Agent

Multi-tenant agentic system that answers natural language questions by querying your database.

## How it works

1. **Schema selection** — picks only the relevant tables/columns for the question
2. **SQL generation** — writes an efficient SELECT query
3. **Validation** — verifies the query answers what was asked
4. **Execution** — runs the query with company-level data isolation
5. **Answer** — returns a concise natural language response

## Setup

### 1. Environment variables

Create a `.env` file:
```
DATABASE_URL=postgresql://user:password@host:port/dbname
GROQ_API_KEY=your_groq_api_key
```

### 2. Build metadata

Run once after connecting your database (or after schema changes):
```bash
python metadata_builder.py
```

### 3. Upload sample data (optional)

```bash
python data/upload_data.py
```

## Deploy (Railway)

```bash
git push
```

## Usage

```bash
curl -X POST "https://your-app.up.railway.app/query" \
  -G \
  --data-urlencode "company_id=Amadeo" \
  --data-urlencode "question=What is total revenue?"
```

Response:
```json
{
  "company_id": "Amadeo",
  "answer": "The total revenue is $245,000.",
  "sql": "SELECT SUM(amount) FROM mastertable WHERE type = 'income'",
  "valid": true,
  "rows": 1
}
```

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Health check |
| GET | `/companies` | List all companies |
| POST | `/query` | Ask a question |
