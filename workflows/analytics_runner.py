from workflows.spark_session_builder import get_spark_session
from workflows.analytics_pipeline.patient_metrics import run_patient_analytics
from workflows.analytics_pipeline.conditions_metrics import run_conditions_analytics
from workflows.analytics_pipeline.encounter_metrics import run_encounter_analytics
from workflows.supabase_builder import get_supabase_client
import asyncio

async def analytics_runner():
    with get_spark_session() as spark:
        async with get_supabase_client() as supabase:
            print("Running Patient Analytics...")
            await run_patient_analytics(spark=spark, supabase=supabase)
            
            print("Running Conditions Analytics...")
            await run_conditions_analytics(spark=spark, supabase=supabase)
            
            print("Running Encounter Analytics...")
            await run_encounter_analytics(spark=spark, supabase=supabase)

if __name__ == "__main__":
    asyncio.run(analytics_runner())
