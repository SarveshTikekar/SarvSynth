from supabase import create_client, Client, ClientOptions
import os
from dotenv import load_dotenv

load_dotenv()

def get_supabase_client() -> Client:
    """
    Initialize and return a synchronous Supabase client anchored to the 'healthcare' schema.
    """
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("SUPABASE_URL or SUPABASE_KEY environment variables are missing")
        
    return create_client(
        SUPABASE_URL, 
        SUPABASE_KEY, 
        options=ClientOptions(schema="healthcare")
    )
