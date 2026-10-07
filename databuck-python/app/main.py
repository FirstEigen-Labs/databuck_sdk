from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import Json
import uvicorn
import json


from app.db_connection import fetch_api_key_from_db
from app.models import LogAnalyzeRequest, LogAnalyzeResponse, ValidateTableRequest, ValidateTableResponse
from app.services.validate_table import validateTable,reset_conversation
from app.services.log_summarizer import analyze_log
import logging
from app.services.session import SessionState

logger = logging.getLogger('uvicorn.error')
logger.setLevel(logging.DEBUG)

session_state = SessionState()
app = FastAPI(title="Log Analyzer API",
              description="API for analyzing and summarizing log files",
              version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/analyzeLog", response_model=LogAnalyzeResponse)
async def analyze_log_endpoint(request: LogAnalyzeRequest):
    """
    Analyzes a log file and returns a summarized explanation.
    """
    try:
        # Call the log analyzer service
        summary = analyze_log(request.log_content)
        
        return LogAnalyzeResponse(
            status="success",
            message={"summary": summary}
        )
    except Exception as e:
        return LogAnalyzeResponse(
            status="failed",
            message={"error": str(e)}
        )
    
@app.post("/validateTable", response_model=ValidateTableResponse)
async def validateTable_endpoint(request: Request):
    auth_header = request.headers.get("Authorization")
    activeProjectId = request.headers.get("activeProjectId")
    activeDomainId = request.headers.get("activeDomainId")
    session_id = request.headers.get("sessionId")
    token_header = request.headers.get("Token")
    user_details  = request.headers.get("userdetails")
    if not session_state.get_session(session_id):
        reset_conversation()

    print(f"Received session_id: {session_id}, auth_header: {auth_header}, token_header: {token_header}")
    print(f" session_id: {activeProjectId}, activeDomainId: {activeDomainId})")
    session_state.set_value(session_id,'auth', auth_header)
    session_state.set_value(session_id,'token',token_header)
    session_state.set_value(session_id,'user_details',user_details)
    
    
    session_state.set_value(session_id,'activeProjectId',activeProjectId)
    session_state.set_value(session_id,'activeDomainId',activeDomainId)

    session_state.set_value(session_id,'sessionId', session_id)
    session_state.set_value(session_id,'api', None)
    payload = await request.json()
    
    try:
        #print(f"Received request content: {request.content}")
        api_key = session_state.get_value(session_id,'api')
        print(f"retrived API key: {api_key}")
        if not api_key:
            print(f"Fetching API key from database for session {session_id}")
            ai_details = fetch_api_key_from_db()
            session_state.set_value(session_id,'api',ai_details['api_key'])
            session_state.set_value(session_id,'ai_model',ai_details['default_ai_option'])
            api_key=ai_details['api_key']
            model_name=ai_details['default_ai_option']

            
        else:
            print(f"Using cached API key")
        # Call the log analyzer service
        summary = validateTable(payload['content'],api_key,model_name, session_state, session_id)
        
        return ValidateTableResponse(
            content=summary
        )
    except Exception as e:
        logging.error(f"Error in validateTable endpoint: {e}")
        return LogAnalyzeResponse(
            status="failed",
            message=str(e)
        )


if __name__ == "__main__":
    ai_details = fetch_api_key_from_db()
    print("OpenAI API Key:", ai_details['api_key'])
    uvicorn.run("app.main:app", host="127.0.0.1", port=8000, reload=True)