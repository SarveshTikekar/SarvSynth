from pyspark.sql.functions import col, to_timestamp, coalesce, current_date, lit
import os
import asyncio

async def etl(spark, supabase):
    """
    Load transformed encounters data into Supabase, ensuring referential integrity.
    """
    print("Starting Encounters ETL...")
    
    # 1. Fetch valid patient UUIDs to avoid Foreign Key violations
    print("Fetching valid patient UUIDs...")
    response = await supabase.table("patients").select("uuid").execute()
    valid_uuids = [row['uuid'] for row in response.data]
    
    if not valid_uuids:
        print("No patients found in database. Skipping encounters ETL.")
        return

    # 2. Load and Transform Data
    path = os.path.join(os.path.dirname(__file__), "..", "..", "Datasets", "csv", "encounters.csv")
    if not os.path.exists(path):
        print(f"File not found: {path}")
        return
        
    df = spark.read.csv(path, header=True, inferSchema=True)
    
    # Rename columns to match schema
    new_cols = [
        'encounter_id', 'visit_start', 'visit_end', 'uuid', 'hospital_id', 
        'practioner_id', '_', 'encounter_type', 'encounter_reason_id', 
        'encounter_reason', 'visiting_base_fees', 'visting_total_fees', 
        'coverage', 'diagnosis_id', 'diagnosis'
    ]
    
    for old_col, new_col in zip(df.columns, new_cols):
        df = df.withColumnRenamed(old_col, new_col)

    # Drop placeholder and ensure types
    df = df.drop("_")
    
    # Convert dates to proper ISO format for Supabase (cast to string)
    df = df.withColumn("visit_start", to_timestamp(col("visit_start")).cast("string"))
    df = df.withColumn("visit_end", coalesce(to_timestamp(col("visit_end")), current_date()).cast("string"))

    # Typecast numerical fields
    df = df.withColumn("visiting_base_fees", col("visiting_base_fees").cast("float")) \
           .withColumn("visting_total_fees", col("visting_total_fees").cast("float")) \
           .withColumn("coverage", col("coverage").cast("float"))

    # 3. Filter for Referential Integrity
    df_filtered = df.filter(col("uuid").isin(valid_uuids))
    
    dropped_count = df.count() - df_filtered.count()
    if dropped_count > 0:
        print(f"Dropped {dropped_count} orphan encounter records (UUID not in patients table).")

    # 4. Prepare for Upsert
    data = [row.asDict() for row in df_filtered.collect()]
    
    print(f"Upserting {len(data)} records to 'encounters' table...")
    batch_size = 500
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        try:
            # Using upsert based on encounter_id
            await supabase.table("encounters").upsert(
                batch, 
                on_conflict="encounter_id"
            ).execute()
        except Exception as e:
            print(f"Error in batch {i//batch_size}: {e}")
            continue

    print("Encounters ETL Complete.")
