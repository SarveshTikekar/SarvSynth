from pyspark.sql.functions import col, to_date, regexp_extract, trim, current_date, lit
import os
import asyncio

async def etl(spark, supabase):
    """
    Load transformed conditions data into Supabase, ensuring referential integrity.
    """
    print("Starting Conditions ETL...")
    
    # 1. Fetch valid UUIDs from patients table to avoid Foreign Key violations
    print("Fetching valid patient UUIDs...")
    response = await supabase.table("patients").select("uuid").execute()
    valid_uuids = [row['uuid'] for row in response.data]
    
    if not valid_uuids:
        print("No patients found in database. Skipping conditions ETL.")
        return

    # 2. Load and Transform Data
    path = os.path.join(os.path.dirname(__file__), "..", "..", "Datasets", "csv", "conditions.csv")
    if not os.path.exists(path):
        print(f"File not found: {path}")
        return
        
    df = spark.read.csv(path, header=True, inferSchema=True)
    
    # Rename columns to match schema
    new_cols = ['condition_record_date', 'date_of_abetment', 'uuid', 'encounter_uuid', '_', 'fsn_id', 'fsn']
    for old_col, new_col in zip(df.columns, new_cols):
        df = df.withColumnRenamed(old_col, new_col)

    # Drop placeholder and raw fsn column after extraction
    df = df.drop(*[c for c in df.columns if c.startswith('_')])

    # Extract clean concepts
    df = df.withColumn("medical_concepts", trim(regexp_extract(col("fsn"), r"^(.*?)\(", 1))) \
           .withColumn("associated_semantics", trim(regexp_extract(col("fsn"), r"\((.*?)\)$", 1))) \
           .drop("fsn")

    # Standardize dates for JSON serialization (cast to string)
    df = df.withColumn("condition_record_date", to_date(col("condition_record_date"), "yyyy-MM-dd").cast("string")) \
           .withColumn("date_of_abetment", to_date(col("date_of_abetment"), "yyyy-MM-dd").cast("string"))

    # 3. Filter for Referential Integrity
    # Only keep conditions for patients that exist in our database
    df_filtered = df.filter(col("uuid").isin(valid_uuids))
    
    dropped_count = df.count() - df_filtered.count()
    if dropped_count > 0:
        print(f"Dropped {dropped_count} orphan condition records (UUID not in patients table).")

    # 4. Prepare for Upsert
    data = [row.asDict() for row in df_filtered.collect()]
    
    print(f"Upserting {len(data)} records to 'conditions' table...")
    batch_size = 500
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        try:
            # Using upsert with on_conflict to avoid duplicates on re-runs
            await supabase.table("conditions").upsert(
                batch, 
                on_conflict="uuid,encounter_uuid,fsn_id,condition_record_date"
            ).execute()
        except Exception as e:
            print(f"Error in batch {i//batch_size}: {e}")
            # We continue other batches even if one fails
            continue

    print("Conditions ETL Complete.")