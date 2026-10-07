import os
import json
import logging
import requests
import openai
from sqlalchemy import create_engine, text
from db_connection import fetch_api_key_from_db
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.prompts import ChatPromptTemplate
from langchain.schema import StrOutputParser
from langchain.schema.runnable import RunnablePassthrough
from langchain.llms.base import BaseLLM
import re

logger = logging.getLogger('uvicorn.error')
logger.setLevel(logging.DEBUG)

# Session to track user inputs
user_session = {
    "datasource_name": None,
    "table_name": None
}

# === PostgreSQL ===
def get_postgres_engine(schema: str = None):
    options = f"?options=-csearch_path%3D{schema}" if schema else ""
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}{options}"
    )

def find_datasource(datasource_name: str):
    try:
        engine = get_postgres_engine()
        with engine.connect() as conn:
            query = text("""
                SELECT iddataschema, schemaname 
                FROM databuck_app_db.listdataschema 
                WHERE schemaname = :ds_name
            """)
            result = conn.execute(query, {"ds_name": datasource_name}).fetchone()
            return dict(result._mapping) if result else None
    except Exception as e:
        logger.error(f"Error querying datasource: {e}")
        return None

# === External API ===
def get_table_names(schema_id: int, datasource_name: str):
    print(schema_id, datasource_name)
    try:
        headers = {
            "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIwYjAyOTlhMi02YzdjLTRhOTktODI3YS1mMGMzYzA4MjU1MDkiLCJpYXQiOjE3NTAyMTE4NDEsImV4cCI6MTc1MDI0MDY0MSwianRpIjoiMjYxY2MxNDQtZjk2Zi00ZWE0LWFlNjgtOGQ1NDY2Y2ViNWU4IiwiaXNzIjoiZGF0YWJ1Y2sifQ.1HcThBHcv0fr9Gtc_i4kgo9yNIevXdwnE-mZ5ao6Xdo",
            "Token":"0b0299a2-6c7c-4a99-827a-f0c3c0825509",
            "Content-Type" :"application/json"
        }
        payload = {
            "schemaId": schema_id,
            "dataConnectionName": datasource_name
        }
        print(payload)
        url = "http://localhost:8080/databuck_war/dbconsole/getTableNamesByDataConnNameNSchemaID"
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json().get("result", [])
    except Exception as e:
        logger.error(f"Failed to fetch table list: {e}")
        return []


def get_llm(api_key: str):
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
# === OpenAI ===
def suggest_nickname_and_description(datasource_name: str, table_name: str, api_key: str ):
    try:
        content = f"Datasource: {datasource_name}\nTable: {table_name}"
        prompt = (
            "Given the following datasource and table names, suggest a table nickname and a concise description for it:\n"
            f"Request: {content}\n\n"
            "Return only a raw JSON object. Example:\n"
            '{\n'
            '  "Datasource Type": "Your datasource type name",\n'
            '  "Datasource": "Your datasource name",\n'
            '  "Table name": "Actual table name",\n'
            '  "Nickname": "ExampleNickname",\n'
            '  "Description": "Brief description of the table."\n'
            '}'
        )
        llm = get_llm(api_key)
        response = llm.invoke(prompt)
        content = response.content 

        '''
        response = openai.ChatCompletion.create(
            model=os.getenv("OPENAI_MODEL"),
            messages=[
                {"role": "system", "content": "You are an expert in suggesting table nicknames and descriptions."},
                {"role": "user", "content": prompt}
            ]
        )

        result_text = response.choices[0].message.content.strip()
        result_text = result_text.replace("```json", "").replace("```", "")
        return json.loads(result_text)
        '''
        try:
            parsed = clean_and_parse_json(content)
            print("LLM raw response:", parsed)
            return parsed
        except json.JSONDecodeError:
            logger.error(f"error: {e}")
            return {"error": str(e)}

    except Exception as e:
        logger.error(f"OpenAI error: {e}")
        return {"error": str(e)}

def extract_content_from_response(response):
    """Extract content from either OpenAI or Google AI response"""
    try:
        # Handle OpenAI response format
        if isinstance(response, dict) and 'choices' in response:
            return response['choices'][0]['message']['content']
            
        # Handle Google AI response format
        if hasattr(response, 'text'):
            return response.text
            
        # Handle LangChain AIMessage format
        if hasattr(response, 'content'):
            return response.content
            
        raise ValueError(f"Unsupported response format: {type(response)}")
        
    except Exception as e:
        logger.error(f"Error extracting content from response: {e}")
        raise ValueError(f"Failed to extract content: {str(e)}")


def clean_and_parse_json(raw_response: str) -> dict:
    # Extract content between ```json and ```
    match = re.search(r"```json\s*(\{.*?\})\s*```", raw_response, re.DOTALL)
    if match:
        json_str = match.group(1)
    else:
        json_str = raw_response.strip()

    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        return {
            "table": None,
            "error": "Invalid JSON format"
        }
    
def extract_info_and_suggest(user_input: str, table_list: str, api_key: str) -> dict:
    openai.api_key = api_key
    llm = get_llm(api_key)
    prompt = f"""
You are an assistant that checks if a given table name exists in a list.

You are given the following JSON:
{table_list}

Your task:
1. Look inside the `result.tableList` array.
2. Check if any object's `tableName` matches the user input (case-insensitive).
3. If a match is found, return the exact `tableName` as it appears in the list.
4. If no match is found, return null.

User input: "{user_input}"

Respond only in this JSON format:
{{
  "table": "<matched_table_name>"   // If found
}}

Or if not found:
{{
  "table": null
}}
"""


    response = llm.invoke(prompt)
    content = response.content  # ✅ Correct extraction

   
    try:
        parsed = clean_and_parse_json(content)
        print(parsed)
        user_session["table_name"] = parsed["table"]
        print("LLM raw response:", parsed)
        return parsed
    except json.JSONDecodeError:
        return {
            "datasource": None,
            "table": None,
            "next_message": "Sorry, I couldn't understand that. Can you try again with datasource and table?"
        }


# === Chatbot Handler ===
def chatbot_conversation(user_message: str, api_key: str):
    # Step 1: Collect datasource name
    if not user_session["datasource_name"]:
        user_session["datasource_name"] = user_message.strip()
        ds_info = find_datasource(user_session["datasource_name"])
        if not ds_info:
            user_session["datasource_name"] = None
            return "❌ Datasource not found. Please enter a valid datasource name:"
        return f"✅ Got it. Now, please enter the table name from '{user_session['datasource_name']}':"

    # Step 2: Collect table name
    if not user_session["table_name"]:
        user_session["table_name"] = user_message.strip()
        print(user_session["datasource_name"])
        ds_info = find_datasource(user_session["datasource_name"])
        table_list = get_table_names(ds_info["iddataschema"], user_session["datasource_name"])
        print(table_list)
        extract_info_and_suggest(user_session["table_name"] , table_list, api_key=api_key)

        if not user_session["table_name"]:
            return f"❌ Table not found in '{user_session['datasource_name']}'. Available tables:\n- " + \
                   "\n- ".join(table_list) + "\n\nPlease enter a valid table name:"

        # Step 3: All valid, generate result
        suggestion = suggest_nickname_and_description(
            user_session["datasource_name"],
            user_session["table_name"],
            api_key
        )

        # Reset session
        #user_session["datasource_name"] = None
        #user_session["table_name"] = None

        return f"✅ Table recognized! Here's the generated metadata:\n\n```json\n{json.dumps(suggestion, indent=2)}\n```"

    return "Unexpected state. Please restart."


# === Simulated usage ===
if __name__ == "__main__":
    api_key = fetch_api_key_from_db()
    while True:
        user_input = input("You: ")
        response = chatbot_conversation(user_input,api_key)
        print(f"Bot: {response}\n")
s