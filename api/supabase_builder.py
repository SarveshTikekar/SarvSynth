from supabase import acreate_client, AsyncClient, ClientOptions
import os
from dotenv import load_dotenv

load_dotenv()

async def get_supabase_client() -> AsyncClient:
    """
    Initialize and return an asynchronous Supabase client anchored to the 'healthcare' schema.
    Since raw data and the metrics store are both in 'healthcare', this unified approach
    prevents cross-schema cache errors (PGRST205).
    """
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("SUPABASE_URL or SUPABASE_KEY environment variables are missing")
        
    # Unified schema for both raw data and metrics
    return await acreate_client(
        SUPABASE_URL, 
        SUPABASE_KEY, 
        options=ClientOptions(schema="healthcare")
    )
