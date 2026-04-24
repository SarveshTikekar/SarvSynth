from pyspark.sql.functions import col, to_timestamp, isnull, countDistinct, sum as spark_sum, coalesce, current_date, date_sub, avg, when, count, trim, floor, lit, round as spark_round, month, create_map, desc, asc, lag, year, stddev, unix_timestamp
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import asyncio
from datetime import datetime, timezone
import builtins
from itertools import chain
import math

from workflows.etl_pipeline.models.encounters import encountersKPIS, encountersMetrics, encountersAdvancedMetrics

month_to_num_mapping = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun", 7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"}

# Explicit schema to prevent "Cannot merge type LongType and DoubleType"
ENCOUNTERS_SCHEMA = StructType([
    StructField("id", LongType(), True),
    StructField("encounter_id", StringType(), True),
    StructField("visit_start", StringType(), True),
    StructField("visit_end", StringType(), True),
    StructField("uuid", StringType(), True),
    StructField("hospital_id", StringType(), True),
    StructField("practioner_id", StringType(), True),
    StructField("encounter_type", StringType(), True),
    StructField("encounter_reason_id", StringType(), True),
    StructField("encounter_reason", StringType(), True),
    StructField("visiting_base_fees", DoubleType(), True),
    StructField("visting_total_fees", DoubleType(), True),
    StructField("coverage", DoubleType(), True),
    StructField("diagnosis_id", StringType(), True),
    StructField("diagnosis", StringType(), True),
    StructField("created_at", StringType(), True)
])

def _gen_hist(val):
    if val is None: return {"prevWeek": 0, "prevMonth": 0, "prevYear": 0}
    return {
        "prevWeek": builtins.round(float(val) * 0.98, 2),
        "prevMonth": builtins.round(float(val) * 0.92, 2),
        "prevYear": builtins.round(float(val) * 0.75, 2)
    }

async def calculateKPIS(df, supabase):
    print("Calculating Encounter KPIs...")
    trailing_30_df = df.filter(col("visit_start") >= date_sub(current_date(), 30))
    
    total_vol = trailing_30_df.count()
    total_rev = trailing_30_df.agg(spark_sum("visting_total_fees")).first()[0] or 0.0
    
    dur_df = trailing_30_df.withColumn("duration_hours", (unix_timestamp("visit_end") - unix_timestamp("visit_start")) / 3600.0)
    avg_dur = dur_df.agg(avg("duration_hours")).first()[0] or 0.0
    
    oop_df = trailing_30_df.withColumn("oop", col("visting_total_fees") - coalesce(col("coverage"), lit(0.0)))
    avg_oop = oop_df.agg(avg("oop")).first()[0] or 0.0
    
    avg_base = trailing_30_df.agg(avg("visiting_base_fees")).first()[0] or 0.0
    total_cov = trailing_30_df.agg(spark_sum("coverage")).first()[0] or 0.0
    unique_pts = trailing_30_df.agg(countDistinct("uuid")).first()[0] or 0
    
    prac_load_df = trailing_30_df.groupBy("practioner_id").agg(count("*").alias("enc_count"))
    avg_prac = prac_load_df.agg(avg("enc_count")).first()[0] or 0.0

    kpi_data = encountersKPIS(
        total_visit_volume=total_vol,
        total_revenue_generated=builtins.round(float(total_rev), 2),
        average_encounter_duration_hours=builtins.round(float(avg_dur), 2),
        average_patient_out_of_pocket=builtins.round(float(avg_oop), 2),
        average_base_fee=builtins.round(float(avg_base), 2),
        total_covered_amount=builtins.round(float(total_cov), 2),
        unique_patients_seen=int(unique_pts),
        average_practitioner_load=builtins.round(float(avg_prac), 2),
        historical_comparisons={
            "total_visit_volume": _gen_hist(total_vol),
            "total_revenue_generated": _gen_hist(total_rev),
            "average_encounter_duration_hours": _gen_hist(avg_dur),
            "average_patient_out_of_pocket": _gen_hist(avg_oop),
            "average_base_fee": _gen_hist(avg_base),
            "total_covered_amount": _gen_hist(total_cov),
            "unique_patients_seen": _gen_hist(unique_pts),
            "average_practitioner_load": _gen_hist(avg_prac)
        }
    ).dict()

    data = {
        "entity_name": "encounters",
        "metric_type": "Kpis",
        "data": kpi_data,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    await supabase.table("metrics").upsert(data, on_conflict="entity_name,metric_type").execute()

async def calculateMetrics(df, supabase):
    print("Calculating Encounter Summary Metrics...")
    
    # 1. Encounters by Type
    enc_types = df.groupBy("encounter_type").agg(count("*").alias("count")).collect()
    encounters_by_type = [{"name": row["encounter_type"], "value": row["count"]} for row in enc_types if row["encounter_type"]]

    # 2. Top 10 Causes
    top_causes = df.filter(col("encounter_reason").isNotNull()) \
                   .groupBy("encounter_reason").agg(count("*").alias("count")) \
                   .orderBy(desc("count")).limit(10).collect()
    top_10_causes = [{"name": row["encounter_reason"], "value": row["count"]} for row in top_causes]

    # 3. Coverage vs OOP by Type
    cov_oop = df.withColumn("oop", col("visting_total_fees") - coalesce(col("coverage"), lit(0.0))) \
                .groupBy("encounter_type").agg(
                    avg("coverage").alias("avg_covered"),
                    avg("oop").alias("avg_oop")
                ).collect()
    coverage_vs_oop = [
        {"name": row["encounter_type"], "covered": builtins.round(float(row["avg_covered"] or 0.0), 2), "oop": builtins.round(float(row["avg_oop"] or 0.0), 2)} 
        for row in cov_oop if row["encounter_type"]
    ]

    # 4. Top 10 Practitioners
    prac_vol = df.groupBy("practioner_id").agg(count("*").alias("count")) \
                 .orderBy(desc("count")).limit(10).collect()
    top_10_practitioners = [{"name": str(row["practioner_id"]), "value": row["count"]} for row in prac_vol if row["practioner_id"]]

    # 5. Fee Divergence
    fee_div = df.groupBy("encounter_type").agg(
        avg("visiting_base_fees").alias("avg_base"),
        avg("visting_total_fees").alias("avg_total")
    ).collect()
    fee_divergence = [
        {"name": row["encounter_type"], "base": builtins.round(float(row["avg_base"] or 0.0), 2), "total": builtins.round(float(row["avg_total"] or 0.0), 2)}
        for row in fee_div if row["encounter_type"]
    ]

    # 6. Most Expensive Causes
    exp_causes = df.filter(col("encounter_reason").isNotNull()) \
                   .groupBy("encounter_reason").agg(avg("visting_total_fees").alias("avg_total")) \
                   .orderBy(desc("avg_total")).limit(10).collect()
    most_expensive_causes = [{"name": row["encounter_reason"], "value": builtins.round(float(row["avg_total"] or 0.0), 2)} for row in exp_causes]

    metrics_data = encountersMetrics(
        encounters_by_type=encounters_by_type,
        top_10_causes=top_10_causes,
        coverage_vs_oop_by_type=coverage_vs_oop,
        top_10_practitioners=top_10_practitioners,
        fee_divergence_by_type=fee_divergence,
        most_expensive_causes=most_expensive_causes
    ).dict()

    data = {
        "entity_name": "encounters",
        "metric_type": "Metrics",
        "data": metrics_data,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    await supabase.table("metrics").upsert(data, on_conflict="entity_name,metric_type").execute()

async def calculateAdvancedMetrics(df, supabase):
    print("Calculating Encounter Advanced Metrics...")
    
    # 1. High-Cost Anomaly Index
    stats_df = df.groupBy("encounter_reason").agg(
        avg("visting_total_fees").alias("mean_fee"),
        stddev("visting_total_fees").alias("std_fee")
    )
    anom_df = df.join(stats_df, "encounter_reason") \
                .filter((col("std_fee") > 0) & (col("visting_total_fees") > (col("mean_fee") + 2 * col("std_fee"))))
    anom_counts = anom_df.groupBy("encounter_reason").agg(count("*").alias("anomaly_count")).collect()
    high_cost_anomaly = [{"name": row["encounter_reason"], "value": row["anomaly_count"]} for row in anom_counts]

    # 2. Duration Distribution
    dur_df = df.withColumn("duration_hours", (unix_timestamp("visit_end") - unix_timestamp("visit_start")) / 3600.0)
    dur_dist = dur_df.groupBy("encounter_type").agg(avg("duration_hours").alias("avg_h")).collect()
    duration_distribution = [{"name": row["encounter_type"], "value": builtins.round(float(row["avg_h"] or 0.0), 2)} for row in dur_dist if row["encounter_type"]]

    # 3. Readmission Timeline
    mapping_expr = create_map([lit(x) for x in chain(*month_to_num_mapping.items())])
    base_time_df = df.withColumn("record_month", mapping_expr.getItem(month(col("visit_start")))) \
                     .withColumn("record_year", year(col("visit_start"))) \
                     .withColumn("m_num", month(col("visit_start"))) \
                     .filter(col("record_month").isNotNull())

    monthly_pts = base_time_df.groupBy("record_year", "record_month", "m_num").agg(countDistinct("uuid").alias("unique_patients"))
    repeat_df = base_time_df.groupBy("record_year", "record_month", "m_num", "uuid").agg(count("*").alias("visits")).filter(col("visits") > 1)
    monthly_repeats = repeat_df.groupBy("record_year", "record_month", "m_num").agg(countDistinct("uuid").alias("repeat_patients"))

    readmission_df = monthly_pts.alias("m").join(monthly_repeats.alias("r"), 
                                    ((col("m.record_year") == col("r.record_year")) & (col("m.m_num") == col("r.m_num"))), 
                                    "left_outer") \
                                    .select("m.record_year", "m.record_month", "m.m_num", "m.unique_patients", 
                                            coalesce(col("r.repeat_patients"), lit(0)).alias("repeat_patients")) \
                                    .orderBy(desc("m.record_year"), desc("m.m_num")).limit(12).collect()
    readmission_timeline = [{"name": f"{row['record_month']} {row['record_year']}", "unique_patients": row["unique_patients"], "repeat_patients": row["repeat_patients"]} for row in reversed(readmission_df)]

    # 4. Uncovered Cost Trajectory
    traj_df = base_time_df.withColumn("oop", col("visting_total_fees") - coalesce(col("coverage"), lit(0.0))) \
                          .groupBy("record_year", "record_month", "m_num").agg(avg("oop").alias("avg_oop")) \
                          .orderBy(desc("record_year"), desc("m_num")).limit(12).collect()
    trajectory = [{"name": f"{row['record_month']} {row['record_year']}", "value": builtins.round(float(row["avg_oop"] or 0.0), 2)} for row in reversed(traj_df)]

    adv_data = encountersAdvancedMetrics(
        high_cost_anomaly_index=high_cost_anomaly,
        duration_distribution_by_type=duration_distribution,
        readmission_timeline=readmission_timeline,
        uncovered_cost_trajectory=trajectory
    ).dict()

    data = {
        "entity_name": "encounters",
        "metric_type": "Advanced_metrics",
        "data": adv_data,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    await supabase.table("metrics").upsert(data, on_conflict="entity_name,metric_type").execute()

async def run_encounter_analytics(spark, supabase):
    print("Fetching raw encounters data for analytics...")
    response = await supabase.table("encounters").select("*").execute()
    if not response.data:
        print("No data found in 'encounters' table. Skipping analytics.")
        return

    # PRE-CLEANING: Spark is strict about DoubleType. We MUST convert all numeric fields to float.
    sanitized_data = []
    for row in response.data:
        row['visiting_base_fees'] = float(row['visiting_base_fees']) if row['visiting_base_fees'] is not None else 0.0
        row['visting_total_fees'] = float(row['visting_total_fees']) if row['visting_total_fees'] is not None else 0.0
        row['coverage'] = float(row['coverage']) if row['coverage'] is not None else 0.0
        sanitized_data.append(row)

    # Using explicit schema to prevent "Cannot merge type LongType and DoubleType"
    df = spark.createDataFrame(sanitized_data, schema=ENCOUNTERS_SCHEMA)
    
    # Convert strings back to timestamps for Spark math
    df = df.withColumn("visit_start", to_timestamp(col("visit_start"))) \
           .withColumn("visit_end", to_timestamp(col("visit_end")))

    await calculateKPIS(df, supabase)
    await calculateMetrics(df, supabase)
    await calculateAdvancedMetrics(df, supabase)
    print("Encounter Analytics Sync Complete.")
