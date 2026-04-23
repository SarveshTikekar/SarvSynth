""" ETL Pipeline for the patients"""

from supabase import AsyncClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, to_date, isnull, mean, percentile_approx, current_date, datediff, coalesce, sum, dense_rank, count, lit, concat, ceil, floor, avg, try_divide, max, min, current_timestamp
from math import log2
from pyspark.sql import Window
from pyspark.sql.types import NumericType
import re
import os
import math
from workflows.etl_pipeline.models.patients import patientKPIS, patientMetrics, patientAdvancedMetrics
from workflows.spark_session_builder import get_spark_session
from workflows.supabase_builder import get_supabase_client

async def etl(spark: SparkSession, supabase: AsyncClient):

    """
    Load the transformed patients DataFrame from CSV if not already loaded.
    """

    path=os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "Datasets", "csv", "patients.csv"))
    df = spark.read.csv(path, header=True, inferSchema=True)

    # Rename columns    
    new_cols_list = ["uuid", "birth_date", "death_date", "social_security_number", "driver_license_number", 
                         "passport_number", "salutation", "first_name", "middle_name", "last_name", "doctorate", "_", "marital_status",
                         "race", "ethnicity", "gender", "patient_birthplace", "current_address", "geolocated_city", "geolocated_state",
                         "geolocated_county", "_", "postal_code", "latitude", "longitude", "_", "_", "family_income"]

    for old_col, new_col in zip(df.columns, new_cols_list):
        df = df.withColumnRenamed(old_col, new_col)
        
    df = df.drop(*[col for col in df.columns if col.startswith("_")])

    # Drop numeric-looking column names
    re_patterns = [r'_', r'[0-9]+$']
    dropped_cols = [col for col in df.columns if re.match(re_patterns[0], col)]
    df = df.drop(*dropped_cols)
       
    #Format dates

    df = df.withColumn("death_date", to_date(col("death_date"), "yyyy-MM-dd"))
        
    df = df.withColumn("birth_date", to_date(col("birth_date"), "yyyy-MM-dd"))

    # Clean first, middle, last, maiden names
    df = df.withColumn("first_name", split(col("first_name"), re_patterns[1]).getItem(0)) \
               .withColumn("middle_name", split(col("middle_name"), re_patterns[1]).getItem(0)) \
               .withColumn("last_name", split(col("last_name"), re_patterns[1]).getItem(0)) \

    # Fill missing values
    df = df.withColumn("salutation", when(col("gender") == "M", "Mr.").otherwise("Ms.")) \
                .fillna({"passport_number": "N/A", "middle_name": "N/A", "doctorate": "No doctorate", "marital_status": "Unknown"})
        
    #Modify the postal code 
    df = df.withColumn("postal_code", col("postal_code").cast("string")).withColumn("postal_code",concat(lit("0"),col("postal_code")))

    #We add the created date column to keep track of when the record was created in the database

    df = df.withColumn("created_at", current_timestamp())
    """ We now push everything to the database """

    #Converting the date col to strings
    df = df.withColumn("birth_date", col("birth_date").cast("string")).withColumn("death_date", col("death_date").cast("string")).withColumn("created_at", col("created_at").cast("string"))

    data = [row.asDict() for row in df.collect()]

    batch_size = 250
    for i in range(0, len(data), batch_size):

        batch = data[i:i+batch_size]

        try:
            response = await supabase.table("patients").upsert(batch).execute()

        except Exception as e:
            raise Exception(f"Error inserting batch into Supabase: {e}")
