import asyncio
from workflows.spark_session_builder import get_spark_session
from workflows.supabase_builder import get_supabase_client
from workflows.etl_pipeline.patients import etl as patients_etl
from workflows.etl_pipeline.conditions import etl as conditions_etl
from workflows.etl_pipeline.encounters import etl as encounters_etl

async def etl_runner():
    """
    Orchestrates the ETL process by providing a Spark session 
    and an async Supabase client to the workers.
    """
    # 1. Open the Spark Session (Sync context manager)
    with get_spark_session() as spark:
        # 2. Open the Supabase Client (Async context manager)
        async with get_supabase_client() as supabase:
            print(" Starting Patient ETL...")
            await patients_etl(spark, supabase)
            print("Starting Conditions ETL...")
            await conditions_etl(spark, supabase)
            print("Starting Encounters ETL...")
            await encounters_etl(spark, supabase)
            print("ETL Sync Complete.")

if __name__ == "__main__":
    asyncio.run(etl_runner())