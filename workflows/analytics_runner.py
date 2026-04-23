from workflows.spark_session_builder import get_spark_session;
from workflows.analytics_pipeline.patient_metrics import run_patient_analytics
from workflows.supabase_builder import get_supabase_client
import asyncio

async def analytics_runner():
    with get_spark_session() as spark:
        async with get_supabase_client() as supabase:
            await run_patient_analytics(spark=spark, supabase=supabase)

if __name__ == "__main__":
    asyncio.run(analytics_runner())
