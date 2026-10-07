import os
from fastapi import logger
import psycopg2
from dotenv import load_dotenv

from app.services.session import SessionState

load_dotenv()

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

def fetch_api_key_from_db():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # Step 1: Get the value of 'default_ai_option'
                cursor.execute("""
                    SELECT property_value FROM databuck_app_db.databuck_property_details 
                    WHERE property_category_id = 16 AND property_name = 'default_ai_option'
                     """)
                default_ai_result = cursor.fetchone()
                
                if not default_ai_result:
                    raise ValueError("No value found for 'default_ai_option'.")
                
                default_ai_option = default_ai_result[0].strip()
                print(f"default_ai_option: {default_ai_option}")
                
                # Step 2: Get the API key for the specified option
                cursor.execute("""
                    SELECT property_value FROM databuck_app_db.databuck_property_details 
                    WHERE property_category_id = 16 AND property_name = %s""", 
                    (default_ai_option,))
                result = cursor.fetchone()
                
                if not result:
                    raise ValueError(f"No API key found for default_ai_option = '{default_ai_option}'")
                
                api_key = result[0].strip()
                print("Fetched OpenAI key from database:", api_key)
                
                # Return both values
                return {
                    "default_ai_option": default_ai_option,
                    "api_key": api_key
                }

    except Exception as e:
        raise RuntimeError(f"Failed to fetch OpenAI key and default_ai_option: {e}")
    
