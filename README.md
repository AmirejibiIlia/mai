# AI SQL Agent - Production

Multi-tenant agentic SQL system with LangGraph.

## Features
- Multi-agent SQL generation & validation
- Company-level data isolation
- Configurable timeouts & limits
- Error handling & logging
- Production-ready architecture

## Deploy
```bash
git push && railway up
```

## Usage
```bash
curl -G -X POST "https://api.railway.app/query" \
  --data-urlencode "company_id=Amadeo" \
  --data-urlencode "question=Total revenue?"
```

## Environment
See `.env.example`