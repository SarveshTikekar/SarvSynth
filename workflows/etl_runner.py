import asyncio
from datetime import datetime
from workflows.etl_pipeline.allergies import etl as allergies_etl
from workflows.etl_pipeline.conditions import etl as conditions_etl
from workflows.etl_pipeline.encounters import etl as encounters_etl
from workflows.etl_pipeline.patients import etl as patients_etl
from workflows.spark_session_builder import get_spark_session
from workflows.supabase_builder import get_supabase_client


async def etl_runner():
    """
    Orchestrates the ETL process by providing a Spark session
    and an async Supabase client to the workers.
    """

    # 1. Open the Spark Session (Sync context manager)
    with get_spark_session() as spark:
        # 2. Open the Supabase Client (Async context manager)

        async with get_supabase_client() as supabase:
            print(
                " Starting Patient ETL pipeline at: ",
                datetime.now().strftime("%Y-%m-%d %H:%M"),
            )
            await patients_etl(spark, supabase)

            print(
                "Starting Conditions ETL pipeline at: ",
                datetime.now().strftime("%Y-%m-%d %H:%M"),
            )
            await conditions_etl(spark, supabase)

            print(
                "Starting Encounters ETL pipeline at: ",
                datetime.now().strftime("%Y-%m-%d %H:%M"),
            )
            await encounters_etl(spark, supabase)

            print(
                "Starting the Allergies ETL Pipeline at: ",
                datetime.now().strftime("%Y-%m-%d %H:%M"),
            )
            await allergies_etl(spark, supabase)
            print("All the ETL Pipelines have been ran successfully!")


if __name__ == "__main__":
    asyncio.run(etl_runner())