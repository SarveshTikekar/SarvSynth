from supabase import create_async_client, AsyncClient, ClientOptions
import os
from dotenv import load_dotenv
from contextlib import asynccontextmanager

load_dotenv()

@asynccontextmanager
async def get_supabase_client():
    """
    Provides an async Supabase client. 
    Credentials are pulled from environment variables.
    """
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    
    if not url or not key:
        raise ValueError("SUPABASE_URL or SUPABASE_KEY environment variables are missing.")

    client: AsyncClient = await create_async_client(supabase_url=url, supabase_key=key, options=ClientOptions(schema="healthcare"))
    
    try:
        yield client
    finally:
        pass
