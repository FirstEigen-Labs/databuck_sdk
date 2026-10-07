from pydantic import BaseModel

class LogAnalyzeRequest(BaseModel):
    log_content: str
    
class LogAnalyzeResponse(BaseModel):
    status: str
    message: dict  # Changed to dict for better flexibility

class ValidateTableRequest(BaseModel):
    content: str

class ValidateTableResponse(BaseModel):
    content: str
