# === chatbot_refactored.py ===

import os
import json
import logging
import re
from dotenv import load_dotenv
import requests
from app.services.session import SessionState
from sqlalchemy import create_engine, text
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.prompts import ChatPromptTemplate
from langchain.schema import StrOutputParser
from langchain.memory import ConversationBufferMemory

# === Configuration ===
logger = logging.getLogger("uvicorn.error")
logger.setLevel(logging.DEBUG)
conversation_memory = ConversationBufferMemory(memory_key="chat_history", return_messages=True)
load_dotenv()

BASE_URL=os.getenv("BASE_URL")
EXISTING_TABLE_LIST_URL=BASE_URL + os.getenv("EXISTING_TABLE_LIST_URL")
ADD_TABLE=BASE_URL + os.getenv("ADD_TABLE")
SEARCH_BY_TABLE_NAME=BASE_URL + os.getenv("SEARCH_BY_TABLE_NAME")
TABLE_LIST_URL=BASE_URL + os.getenv("TABLE_LIST_URL")

# === PostgreSQL ===

def find_datasource_session(ds_name: str, session_state: SessionState, session_id: str):
    datasource_list = session_state.get_value(session_id, 'datasource_list')
    schemaType = "Unknown"
    for ds in datasource_list:
        if ds.get("schemaName") == ds_name:  # Use == instead of is
            schemaType = ds.get("schemaType", "Unknown")
            session_state.set_value(session_id, 'idSchema', ds.get("idDataSchema", "0"))
            session_state.set_value(session_id, 'schemaType', ds.get("schemaType", "0"))
            return schemaType  # Return the value
    return schemaType
        
    
# === External API ===
def get_datasource_by_table_names(tableName: str, session_state: SessionState, session_id: str):
    try:
        auth = session_state.get_value(session_id, 'auth') or ''
        token = session_state.get_value(session_id, 'token') or ''

        headers = {
            "Authorization": auth,
            "Token": token,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0"
        }

        response = requests.get(
            SEARCH_BY_TABLE_NAME,
            headers=headers
        )
        response.raise_for_status()
        data = response.json()

        result = data.get("result", {})
        datasource_list = data.get("datasource", {}).get("datasource", [])
        matched_sources = []

        for schema_name, table_list in result.items():
            if tableName in table_list:
                # Find the matching datasource by schemaName
                matching_ds = next(
                    (ds for ds in datasource_list if ds.get("schemaName") == schema_name),
                    {}
                )
                matched_sources.append({
                    "DatasourceConnection": matching_ds.get("schemaType", "Unknown"),
                    "DatasourceName": schema_name,
                    "Table name": tableName,
                    "Schema": matching_ds.get("databaseSchema", "Unknown")
                })

        return matched_sources

    except Exception as e:
        logger.error(f"Failed to fetch datasource for table '{tableName}': {e}")
        return []
    
def get_datasource_by_table_names_session(tableName: str, session_state: SessionState, session_id: str):
    try:
        datasource_list = session_state.get_value(session_id, 'datasource_list') or []
        table_list_session = session_state.get_value(session_id, 'table_list') or []
        matched_sources = []

        for schema_name, table_list in table_list_session:
            if tableName in table_list:
                # Find the matching datasource by schemaName
                matching_ds = next(
                    (ds for ds in datasource_list if ds.get("schemaName") == schema_name),
                    {}
                )
                matched_sources.append({
                    "DatasourceConnection": matching_ds.get("schemaType", "Unknown"),
                    "DatasourceName": schema_name,
                    "Table name": tableName,
                    "Schema": matching_ds.get("databaseSchema", "Unknown")
                })

        return matched_sources

    except Exception as e:
        logger.error(f"Failed to fetch datasource for table '{tableName}': {e}")
        return []

def load_all_datasource_tables(session_state: SessionState, session_id: str):
    try:
        auth = session_state.get_value(session_id, 'auth') or ''
        token = session_state.get_value(session_id, 'token') or ''

        headers = {
            "Authorization": auth,
            "Token": token,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0"
        }

        response = requests.get(
            SEARCH_BY_TABLE_NAME,
            headers=headers
        )
        response.raise_for_status()
        data = response.json()

        result = data.get("result", {})
        datasource_list = data.get("datasource", {}).get("datasource", [])
        session_state.set_value(session_id, 'datasource_list', datasource_list)
        
        session_state.set_value(session_id, 'table_list', result.items())          

    except Exception as e:
        logger.error(f"Failed to fetch datasource for table : {e}")
        return []
    
    
# === External API ===
def add_table(payload, session_state: SessionState, session_id: str):
    try:
        headers = {
            "Authorization": session_state.get_value(session_id, 'auth'),
            "Token": session_state.get_value(session_id, 'token'),
            "Content-Type": "application/json",
            "Accept": "application/json",
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:39.0)'
        }
        response = requests.post(
            ADD_TABLE, json=payload, headers=headers, timeout=10
        )
        response.raise_for_status()
        if response.status_code != 200:
            logger.error(f"error to add table: {response.text}")
            return f"Error: {response.text}"
        return response.json().get("templateId")
    except Exception as e:
        logger.error(f"Failed to fetch table list: {e}")
        return []
    
def get_existing_table_names( session_state: SessionState, session_id: str):
    try:
        headers = {
            "Authorization": session_state.get_value(session_id, 'auth'),
            "Token": session_state.get_value(session_id, 'token'),
            "Content-Type": "application/json",
            "Accept": "application/json",
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:39.0)'
        }
        payload = {
            "SearchByOption": "1",
            "FromDate": "2021-01-01",
            "ToDate": "2025-06-30",
            "ProjectIds": session_state.get_value(session_id, 'activeProjectId', 0),
            "SearchText": "",
            "domainId": session_state.get_value(session_id, 'activeDomainId', 1),
            "filterByDerived": False
        }
        response = requests.post(
            EXISTING_TABLE_LIST_URL, json=payload, headers=headers, timeout=10
        )
        #response.raise_for_status()
        return response.json().get("result", []).get("ViewPageDataList", [])
    except Exception as e:
        logger.error(f"Failed to fetch table list: {e}")
        return []


# === LLM ===
def get_llm(api_key: str,model_name:str):
    open_ai_model = os.environ.get("OPENAI_MODEL")
    gemeni_model = os.environ.get("GEMINI_MODEL")
    if not api_key:
        raise EnvironmentError("API KEY MISSING")
    if model_name=="OpenAi":
        return ChatOpenAI(model=open_ai_model, api_key=api_key)
    elif model_name=="Gemini":
        return ChatGoogleGenerativeAI(model=gemeni_model, google_api_key=api_key)
    else:
        raise ValueError("Unsupported API key format")

# === Prompts ===
def create_intent_chain(api_key: str,model_name:str):
    prompt = ChatPromptTemplate.from_template("""
    You are an intelligent AI assistant designed to help users validate a table based on their message and the current session context.

    Your core objective is to:
    - Understand if the user intends to validate a table.
    - Extract table name from the user input. Typically the table name is a noun or noun phrase.
    - if table name is not provided, ask the user to provide a table name.
    - if table name exists, check against the list of existing tables. if table exists in multiple datasources, ask the user to choose a datasource.
    - if the user selects the proper datasource, prompt the user to validate all the details.
    - upon getting confirmation, trigger the table validation process.
                                              
    ### Core Behaviors:

    1. Always assume the default intent is `"validate_table"` with 'validate table table_name' as input unless the user explicitly asks to `"list tables"` or `"list datasources"`.

    2. If the table name is present but the datasource is missing, set:
    - `ask_datasource: true`
    - `validate_table: false`
    - `ask_confirmation: false`
        

    3. If both table and datasource are present: Users may say:"Validate table X from Y datasource"
    - `ask_datasource: false`
    - `validate_table: false`
    - `ask_confirmation: true`
    
    upon confirmation, set:
    - `validate_table: true` 
    - `ask_confirmation: false`
    - `ask_datasource: false`

    4. If table name is invalid or unrecognized (based on internal check), prompt the user to provide a valid table name or the datasource.

    5. If user asks to "list datasources", change intent to `list_datasources`.

    6. If user asks to "list tables" but no datasource is available in session, set:
    - `intent: list_tables`
    - `needs_datasource: true`

    7. If the message is ambiguous or in the form of a question (e.g., “Can you check a table for me?”), classify `message_type` as `"question"`.
                                   
    8. If the user wants to update or change the **Nick Name** (e.g., says "change the Nick Name to X" or "update Nick Name"), then:
    - Retain current table and datasource context.
    - Update the field `"Nick Name"` with the new value.
    - Set:
        - `"ask_validation": true`
        - `"ask_confirmation": true`
        - `"validate_table": false`
    - Respond accordingly by showing updated Nick Name and asking for confirmation, call generate_table_confirmation function again.
    9. Always generate a Nick Name and Description, even if not explicitly asked, using the table name as context.
                                        
    ### Input Parameters:

   - Chat History: {chat_history}
   - User Message: {user_message}
   - Datasource: {current_datasource}
   - Table: {current_table}

    Respond in this structured JSON format:
                                              
    Output JSON:
    {{
      "intent": "validate_table | list_datasources | list_tables",
      "datasource_name": "...",
      "table_name": "...",
      "ask_datasource": true | false,
      "validate_table": true | false,
      "ask_validation": true | false,
      "message_type": "complete | partial | question",
      "Nick Name": "<A short 1–3 word title summarizing the table's purpose, often based on the table name.",
      "Description": "<A concise 5–10 words explanation describing what the table contains or what the user is trying to do with it.>"
    }}
    """)
    return prompt | get_llm(api_key,model_name) | StrOutputParser()


def clean_and_parse_json(raw_response: str) -> dict:
    match = re.search(r"```json\s*(\{.*?\})\s*```", raw_response, re.DOTALL)
    json_str = match.group(1) if match else raw_response.strip()
    try:
        return json.loads(json_str)
    except:
        return {
            "intent": "unknown",
            "datasource_name": None,
            "table_name": None,
            "needs_datasource": True,
            "needs_table": True,
            "message_type": "question"
        }
    
def get_table_info(json_array, table_name):
    """
    Given a JSON array (list of dicts) and a table name,
    return the dict for that table name, or None if not found.
    """
    listmatch =[]
    for entry in json_array:
        if entry.get("Tablename") == table_name:
            listmatch.append(entry)
    return listmatch

def get_payload(intent, session_state, session_id):
    user_info = session_state.get_value(session_id, 'user_details')
    user_info = json.loads(user_info)
    template_json = {
        "idUser": user_info.get("idUser"),
        "dataTemplateName": intent.get("Nick Name", ""),
        "dataLocation": session_state.get_value(session_id, 'schemaType'),
        "createdByUser":user_info.get("userName"),
        "hostName": "",
        "schemaName": "",
        "srcBrokerUri": "",
        "srcTopicName": "",
        "dataFormat": "",
        "userName": "",
        "pwd": "",
        "tarBrokerUri": "",
        "tarTopicName": "",
        "description": intent.get("Description", ""),
        "idDataSchema": session_state.get_value(session_id, 'idSchema'),
        "headerId": "",
        "rowsId": "",
        "tableName": intent.get("table_name"),
        "tableNameList": "",
        "whereId": "",
        "queryCheckboxId": "N",
        "historicDateTable": "",
        "selectedTables": "",
        "queryTextboxId": "",
        "fileQueryCheckboxid": "",
        "fileQueryTextboxid": "",
        "incrementalSourceId": "N",
        "dateFormatId": "",
        "sliceStartId": "",
        "sliceEndId": "",
        "profilingEnabled": "Y",
        "domainfunction": "",
        "projectId": session_state.get_value(session_id, 'activeProjectId'),
        "domainId": session_state.get_value(session_id, 'activeDomainId'),
        "advancedRulesEnabled": "Y",
        "rollingHeaderPresent": "N",
        "rollingColumn": "",
        "deepInsightEnabled": "N",
        "kafkaTopicStructureFilePath": "",
        "historicAnalysisEnabled": "N",
        "historicDataDays": "",
        "historicDateColumn": "",
        "taggIds": []
    }
    print(template_json)
    return template_json


def generate_table_confirmation(session_state: SessionState, session_id: str, intent: dict) -> str:
    ds_name = intent.get("datasource_name")
    table_name = intent.get("table_name")
    if not ds_name or not table_name:
        return None

    ds_schema =  find_datasource_session(ds_name, session_state, session_id)
    if ds_schema:
        formatted_message = (
                f"<br>📄<b> Table Name:</b>      {table_name}<br>"
                f"🔗 <b>Datasource:</b>       {ds_name}<br>"
                f"📂 <b>Schema:</b>           {ds_schema}<br>"
                f"🏷️ <b>Nick Name:</b>          {intent.get('Nick Name', '')}<br>"
                f"📝 <b>Description:</b>       {intent.get('Description', '')}<br>"
            )
        return formatted_message
    return None


def validateTable(user_message: str, api_key: str,model_name:str, session_state: SessionState, session_id: str):
    intent_chain = create_intent_chain(api_key,model_name)
    chat_history = conversation_memory.chat_memory.messages
    print("-----------")
    print(f"Chat history: {chat_history}")
    print("-----------")
    intent_result = intent_chain.invoke({
        "user_message": user_message,
        "chat_history": chat_history,
        "current_datasource": session_state.get_value(session_id, 'datasource_name'),
        "current_table": session_state.get_value(session_id, 'table_name')
    })

    intent = clean_and_parse_json(intent_result)
    lower_msg = user_message.strip().lower()

    
    # Step 1: Cancel
    if lower_msg in {"no", "cancel", "never mind", "don’t add", "not now", "stop", "exit", "quit"}:
        if session_state.get_value(session_id, 'prompt_validate') or session_state.get_value(session_id, 'confirm_validation'):
            session_state.set_value(session_id, 'prompt_validate', False)
            session_state.set_value(session_id, 'confirm_validation', False)
            response = "❌ Validation cancelled. If you'd like to validate a different table or perform another action, just let me know!"
            conversation_memory.chat_memory.add_user_message(user_message)
            conversation_memory.chat_memory.add_ai_message(response)
            session_state.reset()
            reset_conversation()
            return response

    # get all datasource and tables and store it in session state
    if session_state.get_value(session_id, 'datasource_list') is None or session_state.get_value(session_id, 'table_list') is None:
        load_all_datasource_tables(session_state, session_id)

    print("==================")
    print(intent)
    print("==================")
    if intent.get("intent") == "validate_table":

        # Check if table name is provided
        if not intent.get("table_name"):
            response = "❓ Please provide the name of the table you want to validate."
            conversation_memory.chat_memory.add_user_message(user_message)
            conversation_memory.chat_memory.add_ai_message(response)
            return response
        # Check if both table name and datasource is provided
        else:
            table_name= intent.get("table_name")
            datasource_name=intent.get("datasource_name")
            session_state.set_value(session_id, 'datasource_name',datasource_name)
            session_state.set_value(session_id, 'table_name',table_name)
            validated = intent.get("validate_table")
            ask_confirm = session_state.get_value(session_id, "ask_confirmation")

            if table_name is not None and datasource_name is not None:
                # Check if table is already validated
                if ask_confirm:
                    payload = get_payload(intent,session_state, session_id)
                    add_table_response = add_table(payload, session_state, session_id)
                    print(add_table_response)
                    if "error" in add_table_response.lower():
                        response = f"❌ Error adding table: {add_table_response}"
                    else:
                         user_reply = user_message.lower().strip()
                         if user_reply in ["yes", "y", "sure", "ok", "confirm", "proceed"]:
                            session_state.set_value(session_id, "ask_confirmation", False)
                            response = (
                                "✅ Thank you for confirming. The table has been successfully added for validation.<br>"
                                "📊 Profiling and rule creation are in progress.<br>"
                                "📬 You will be notified via your registered email once the process is complete.<br>"
                                f"🔍 To monitor the status, please visit: "
                                f"<a href='http://localhost:4200/dbckangui/validation-check/view-quality-check' target='_blank'>Validation Page</a> "
                                f"Validation Id: {add_table_response}"
)
                         elif user_reply in ["no", "n", "cancel", "stop"]:
                            session_state.set_value(session_id, "ask_confirmation", False)
                            response = "❌ Validation cancelled. Let me know if you want to start over."

                    session_state.set_value(session_id, 'datasource_name', None)
                    session_state.set_value(session_id, 'table_name', None)
                    session_state.set_value(session_id, "intent", {})
                    conversation_memory.chat_memory.add_user_message(user_message)
                    conversation_memory.chat_memory.add_ai_message(response)
                    return response
                if not validated:
                    response= "unable to get the details"
                    entire_list = get_existing_table_names(session_state, session_id)
                    if entire_list:
                        table_list = [t["Tablename"] if isinstance(t, dict) else t for t in entire_list]    
                        table_name = intent.get("table_name")  # ✅ FIX
                        session_state.set_value(session_id, 'awaiting_table_validation_confirmation', True)

                        if table_name in table_list:
                            matched_table_info = get_table_info(entire_list, table_name)
                            table_info_json = [
                                {
                                    "🔌 Connection": item.get("DataLocation", "Unknown"),
                                    "🌐 Database Name": item.get("databaseName", "Unknown"),
                                    "🧾 Table Name": table_name,
                                    
                                }
                                for item in matched_table_info
                            ]
                            response = (
                                f"✅ Table <b>'{table_name}'</b> has already been validated <b> {len(matched_table_info)} times </b> in the datasources listed below:.<br>"
                                f"{json.dumps(table_info_json)}<br><br>❓Would you like to continue with validation? (yes/no)"
                            )
                        else:
                            details = generate_table_confirmation(session_state, session_id, intent)
                            response = f"🔍 Please confirm the details below:<br><br> {details}.<br>🛠️ Proceed with validation?"
                            session_state.set_value(session_id, "ask_confirmation", True)
                            
                    conversation_memory.chat_memory.add_user_message(user_message)
                    conversation_memory.chat_memory.add_ai_message(response)
                    return response
                else:
                    details = generate_table_confirmation(session_state, session_id, intent)
                    response = f"Confirm the following details: {details}. Do you want to proceed ?"
                    session_state.set_value(session_id, "ask_confirmation", True)
                    conversation_memory.chat_memory.add_user_message(user_message)
                    conversation_memory.chat_memory.add_ai_message(response)
                    return response

          
            # Get datasource from table name 
            elif table_name is not None:
                table_name= intent.get("table_name")
                # check if table already exists in the datasources.
                matching_datasources = get_datasource_by_table_names_session(intent.get("table_name"), session_state, session_id)
                if matching_datasources:
                    if len(matching_datasources) == 1:
                        # Only one datasource found, use it directly
                        datasource_name = matching_datasources[0]                        
                        # Get datasource info and proceed to validation
                        ds_info =  session_state.get_value(session_id, 'datasource_list')
                        if ds_info:
                            details = json.dumps({
                                "🔌 Connection": ds_info.get('schematype', 'Unknown'),
                                "🌐 Datasource Name": datasource_name,
                                "🧾 Table Name": intent.get("table_name"),
                                "🏷️ Nick Name": intent.get("Nick Name", ""),
                                "📄 Description": intent.get("Description", "")
                            }, indent=2)
                            
                            response = f"✅ Found table '{table_name}' in datasource '{datasource_name}'!<br><br>Please validate the following details and confirm:<br>{details}"
                            conversation_memory.chat_memory.add_user_message(user_message)
                            conversation_memory.chat_memory.add_ai_message(response)
                            return response
                        else:
                            response = f"❌ Found table '{table_name}' in datasource '{datasource_name}', but couldn't retrieve datasource details. Please try again."
                            conversation_memory.chat_memory.add_user_message(user_message)
                            conversation_memory.chat_memory.add_ai_message(response)
                            return response
                    else:
                        # Multiple datasources found, let user choose
                        datasources = []
                        for ds in matching_datasources:
                            datasource_entry = {
                                "🔌 Connection": ds.get("DatasourceConnection", "Unknown"),
                                "🌐 Datasource Name": ds.get("DatasourceName", "Unknown"),
                                "🧾  Table name": table_name,
                                "🗂️ Schema": ds.get("Schema", "Unknown")
                            }
                            datasources.append(datasource_entry)  

                        response = f"📋  Found multiple datasources with a table named <b>'{table_name}'<b>.<br>"
                        response += json.dumps(datasources)
                        response += "<br>❓ Can you tell me which datasource you'd like to use?"
                        session_state.set_value(session_id,"ask_datasource", True)
                        conversation_memory.chat_memory.add_user_message(user_message)
                        conversation_memory.chat_memory.add_ai_message(response)
                        return response
                else:
                    # No datasource found with this table
                    response = f"❌ Table '{table_name}' not found in any available datasources.<br><br>Please check the table name or provide a valid datasource name."
                    conversation_memory.chat_memory.add_user_message(user_message)
                    conversation_memory.chat_memory.add_ai_message(response)
                    return response
    
    
    elif intent.get("intent") == "list_datasources":
        datasource_list = session_state.get_value(session_id, 'datasource_list')
        simplified_list = []
        for ds in datasource_list:
            simplified_list.append({
                "🔌 Connection": ds.get("schemaType", "Unknown"),
                "🌐 Datasource Name": ds.get("schemaName", "Unknown")
            })

        response = json.dumps(simplified_list)

    elif intent.get("intent") == "list_tables":
        response = "Intent is list_tables"
        print(intent)
    else:
        response = "Intent is unknown or not handled"
        print(intent)

    conversation_memory.chat_memory.add_user_message(user_message)
    conversation_memory.chat_memory.add_ai_message(response)
    return response


# === API Support ===
def reset_conversation():
    conversation_memory.clear()
    return "Conversation has been reset."

def get_conversation_history():
    return conversation_memory.chat_memory.messages