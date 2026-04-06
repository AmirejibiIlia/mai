from fastapi import HTTPException

class CompanyNotFoundError(HTTPException):
    def __init__(self, company_id: str):
        super().__init__(status_code=404, detail=f"Company not found: {company_id}")

class InvalidSQLError(HTTPException):
    def __init__(self, details: str = "Generated SQL is invalid"):
        super().__init__(status_code=400, detail=details)

class QueryExecutionError(HTTPException):
    def __init__(self, error: str):
        super().__init__(status_code=500, detail=f"Query execution failed: {error}")