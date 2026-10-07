import os
from typing import Optional
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.prompts import ChatPromptTemplate
from langchain.schema import StrOutputParser
from langchain.schema.runnable import RunnablePassthrough
from langchain.llms.base import BaseLLM

load_dotenv()

def get_llm() -> BaseLLM:
    api_key = os.environ.get('LLM_API_KEY')
    model_name = os.environ.get('LLM_MODEL')
    
    if not api_key or not model_name:
        raise EnvironmentError("LLM_API_KEY and LLM_MODEL must be set")
    
    if api_key.startswith('sk-ant-'):
        return ChatAnthropic(model=model_name, api_key=api_key)
    elif api_key.startswith('sk-') and 40 < len(api_key) < 60:
        return ChatOpenAI(model=model_name, api_key=api_key)
    elif len(api_key) == 39:
        return ChatGoogleGenerativeAI(model=model_name, google_api_key=api_key)
    else:
        raise ValueError(f"Unknown API key format. Length: {len(api_key)}, Prefix: {api_key[:10]}...")

def analyze_log(log_content: str) -> str:
    """
    Analyzes a log using the configured LLM provider and returns a summarized explanation.
    
    Args:
        log_content: The log content to be analyzed
        
    Returns:
        A summarized explanation of the log
    """

    prompt = ChatPromptTemplate.from_messages([
        ("system", "You are an expert error log analyzer."),
        ("human", """
        You are an expert at analyzing error logs. Below is a log from a system:

        {log_content}

        Please provide a concise summary of this log in simple, business-friendly language. Focus on:
        1. The main events recorded in the log
        2. If there are any issues, clearly explain what specific problem occurred
        3. Use plain English without technical jargon

        DO:
        - Maintain a neutral tone when describing events
        - Identify the specific error or issue if one exists
        - Explain what happened in simple terms a non-technical person can understand
        - Keep your explanation brief and to the point

        DON'T:
        - Use phrases like "what went wrong" or "what went right"
        - Include technical error codes without explanation
        - Suggest solutions or actions for the user to take
        - Use complex technical terminology without explanation

        The summary should give users a clear understanding of what the log shows without requiring technical expertise.
        """)
    ])

    chain = (
        {"log_content": RunnablePassthrough()}
        | prompt
        | get_llm()
        | StrOutputParser()
    )

    return chain.invoke(log_content)