from pyspark.sql.functions import col, to_date, regexp_extract, isnull, mean, countDistinct, sum, coalesce, current_date, datediff, avg, when, date_sub, count, trim, floor, lit, round, current_date, month, create_map, desc, lag, year, concat 
from pyspark.sql import Window
import itertools
from itertools import chain
from workflows.etl_pipeline.master import Master
import os
import math
from workflows.etl_pipeline.models.conditions import conditionKPIS, conditionMetrics, conditionAdvancedMetrics
import builtins


async def etl(spark, supabase):
    """
    Load the transformed conditions DataFrame from CSV if not already loaded.
    """

    path=os.path.join(os.path.dirname(__file__), "..", "..", "Datasets", "csv", "conditions.csv")
        
    df = spark.read.csv(path, header=True, inferSchema=True)
    new_cols = ['condition_record_date', 'date_of_abetment', 'uuid', 'encounter_uuid', '_', 'fsn_id', 'fsn']

    for old_col, new_col in zip(df.columns, new_cols):
        df = df.withColumnRenamed(old_col, new_col)

    # Drop placeholder columns
    df = df.drop(*[col for col in df.columns if col.startswith('_')])

    # Convert date columns to proper date format
    df = df.withColumn("condition_record_date", to_date(col("condition_record_date"), "yyyy-MM-dd").cast("string")) \
               .withColumn("date_of_abetment", to_date(col("date_of_abetment"), "yyyy-MM-dd").cast("string"))

    # Extract event type from description and drop the fsn column
    df = df.withColumn("medical_concepts", regexp_extract(col("fsn"), r"^(.*?)\(", 1)) \
               .withColumn("associated_semantics", regexp_extract(col("fsn"), r"\((.*?)\)$", 1)).drop("fsn") 
               
    df = df.withColumn("medical_concepts", trim(col("medical_concepts"))) \
               .withColumn("associated_semantics", trim(col("associated_semantics")))

    df = df.withColumn("created_at", current_date().cast("string"))

    data = [row.asDict() for row in df.collect()]

    batch_size = 500
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]

        try:
            response = await supabase.table("conditions").insert(batch).execute()
        
        except Exception as e:
            raise Exception(f"Error inserting batch into Supabase: {e}") 