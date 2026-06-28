from pyspark.sql.functions import col, to_timestamp, isnull, countDistinct, sum as spark_sum, coalesce, current_date, date_sub, avg, when, count, trim, floor, lit, round as spark_round, month, create_map, desc, asc, lag, year, stddev, unix_timestamp, datediff, concat
from pyspark.sql import Window
from pyspark.sql.functions.builtin import to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import asyncio
from datetime import datetime, timezone
import builtins
from itertools import chain
import math

from workflows.etl_pipeline.models.allergies import *

month_to_num_mapping = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun", 7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"}

ALLERGIES_SCHEMA = StructType([
    StructField("allergy_detection_date", StringType(), True),
    StructField("allergy_cure_date", StringType(), True),
    StructField("uuid", StringType(), True),
    StructField("encounter_id", StringType(), True),
    StructField("allergy_code", LongType(), True),
    StructField("coding_system", StringType(), True),
    StructField("allergy_description", StringType(), True),
    StructField("allergy_type", StringType(), True),
    StructField("category", StringType(), True),
    StructField("primary_symptom_code", LongType(), True),
    StructField("primary_symptom_description", StringType(), True),
    StructField("primary_symptom_severity", StringType(), True),
    StructField("secondary_symptom_code", LongType(), True),
    StructField("secondary_symptom_description", StringType(), True),
    StructField("secondary_symptom_severity", StringType(), True),
    StructField("primary_symptom_nature", StringType(), True),
    StructField("secondary_symptom_nature", StringType(), True),
    StructField("allergen_nature", StringType(), True),
    StructField("created_at", StringType(), True)
])

def calculateHistoricalTrends(df, patients_df, encounters_df):
    """ Calculate allergy KPI snapshots for historical periods (last week/month/year). """
    mapping = {"last_week": 7, "last_month": 30, "last_year": 365}
    trends = {}

    for period_name, num_days in mapping.items():
        ref_date = date_sub(current_date(), num_days)
        
        # Filter allergies detected on or before ref_date
        df_filtered = df.filter(col("allergy_detection_date") <= ref_date)
        
        # Filter patients alive/registered as of ref_date
        patients_filtered = patients_df.filter(
            (to_date(col("birth_date")) <= ref_date) &
            ((col("death_date").isNull()) | (to_date(col("death_date")) >= ref_date))
        )
        
        total_patients_count = patients_filtered.count()
        if total_patients_count == 0 or df_filtered.count() == 0:
            continue
            
        total_allrg_count = df_filtered.select("uuid").distinct().count()
        
        # Active allergies as of ref_date
        active_count = df_filtered.filter(
            (col("allergy_cure_date").isNull()) | (col("allergy_cure_date") > ref_date)
        ).count()
        active_all_percentage = (active_count / float(df_filtered.count()) * 100.0)
        
        severe_count = df_filtered.filter(col("primary_symptom_severity") == "SEVERE").count()
        severe_allergy_rt = (severe_count / float(df_filtered.count()) * 100.0)
        
        allergic_patient_rt = (total_allrg_count / float(total_patients_count) * 100.0)
        
        # Penicillin de-labeling rate
        penicillin_df = df_filtered.filter(col("allergy_description").rlike(r"^[pP]penicillin$"))
        penicillin_eligible = penicillin_df.filter(
            (col("primary_symptom_severity") != "SEVERE") & 
            (col("secondary_symptom_severity") != "SEVERE") & 
            (col("primary_symptom_description") != "Anaphylaxis")
        ).count()
        penicillin_delabeling_rt = (penicillin_eligible / float(penicillin_df.count()) * 100.0) if penicillin_df.count() > 0 else 0.0
        
        # Poly allergen rate
        poly_count = df_filtered.groupBy("uuid").agg(countDistinct("allergy_code").alias("cnt")).filter(col("cnt") >= 2).count()
        poly_allergen_patient_rt = (poly_count / float(total_allrg_count) * 100.0) if total_allrg_count > 0 else 0.0
        
        # Drug hypersensitivity rate
        drug_count = df_filtered.filter((col("category") == "medication") | (col("allergen_nature") == "drug")).select("uuid").distinct().count()
        drug_hypersensitivity_rt = (drug_count / float(total_allrg_count) * 100.0) if total_allrg_count > 0 else 0.0
        
        # Readmissions up to ref_date
        enc_filtered = encounters_df.filter(col("visit_start") <= ref_date)
        allergic_uuids = df_filtered.select("uuid").distinct()
        allergy_encounters = enc_filtered.join(allergic_uuids, "uuid")
        w = Window.partitionBy("uuid").orderBy("visit_start")
        enc_with_prev = allergy_encounters.withColumn("prev_visit_start", lag("visit_start").over(w))
        enc_with_diff = enc_with_prev.withColumn("diff_days", datediff(col("visit_start"), col("prev_visit_start")))
        readmit_patients_count = enc_with_diff.filter((col("diff_days") > 0) & (col("diff_days") <= 30)).select("uuid").distinct().count()
        readmission_rt = (readmit_patients_count / float(total_allrg_count) * 100.0) if total_allrg_count > 0 else 0.0

        trends[period_name] = {
            "total_allergic_population": int(total_allrg_count),
            "active_allergy_percentage": builtins.round(float(active_all_percentage), 2),
            "severe_allergy_incidence_rate": builtins.round(float(severe_allergy_rt), 2),
            "allergic_patient_rate": builtins.round(float(allergic_patient_rt), 2),
            "penicillin_allergy_delabeling_eligibility_rate": builtins.round(float(penicillin_delabeling_rt), 2),
            "allergy_related_readmission_rate": builtins.round(float(readmission_rt), 2),
            "poly_allergen_patient_rate": builtins.round(float(poly_allergen_patient_rt), 2),
            "drug_hypersensitivity_rate": builtins.round(float(drug_hypersensitivity_rt), 2)
        }

    return trends

async def calculateKPIS(df, supabase):
    print("Calculation of Allergy based KPIs at: ", datetime.now().strftime("%T-%m-%d %H:%M"))

    # Fetch patients for total patient count and historical trends
    patients_resp = await supabase.table("patients").select("uuid, birth_date, death_date").execute()
    patients_df = df.sparkSession.createDataFrame(patients_resp.data)
    total_patients_count = patients_df.count()

    # Fetch encounters for readmission rate
    encounters_resp = await supabase.table("encounters").select("uuid, visit_start").execute()
    encounters_df = df.sparkSession.createDataFrame(encounters_resp.data)
    encounters_df = encounters_df.withColumn("visit_start", to_date(col("visit_start")))

    # KPI-1: Total allergic population (Already computed)
    total_allrg_count: int = df.select("uuid").distinct().count()
    if total_allrg_count == 0:
        return
        
    # KPI-2: Active allergy percentage
    active_count = df.filter(col("allergy_cure_date").isNull()).count()
    active_all_percentage: float = (active_count / float(df.count()) * 100.0) if df.count() > 0 else 0.0

    # KPI-3: Severe allergy incidence rate
    severe_count = df.filter(col("primary_symptom_severity") == "SEVERE").count()
    severe_allergy_rt: float = (severe_count / float(df.count()) * 100.0) if df.count() > 0 else 0.0

    # KPI-4: Allergic patient rate
    allergic_patient_rt: float = (total_allrg_count / float(total_patients_count) * 100.0) if total_patients_count > 0 else 0.0

    # KPI-5: Penicillin de-labeling eligibility rate
    penicillin_df = df.filter(col("allergy_description").rlike("^[pP]enicillin$"))
    penicillin_eligible = penicillin_df.filter(
        (col("primary_symptom_severity") != "SEVERE") & 
        (col("secondary_symptom_severity") != "SEVERE") & 
        (col("primary_symptom_description") != "Anaphylaxis")
    ).count()
    penicillin_delabeling_rt: float = (penicillin_eligible / float(penicillin_df.count()) * 100.0) if penicillin_df.count() > 0 else 0.0

    # KPI-6: Allergy related 30-day readmission rate
    allergic_uuids = df.select("uuid").distinct()
    allergy_encounters = encounters_df.join(allergic_uuids, "uuid")
    w = Window.partitionBy("uuid").orderBy("visit_start")
    enc_with_prev = allergy_encounters.withColumn("prev_visit_start", lag("visit_start").over(w))
    enc_with_diff = enc_with_prev.withColumn("diff_days", datediff(col("visit_start"), col("prev_visit_start")))
    readmit_patients_count = enc_with_diff.filter((col("diff_days") > 0) & (col("diff_days") <= 30)).select("uuid").distinct().count()
    readmission_rt: float = (readmit_patients_count / float(total_allrg_count) * 100.0) if total_allrg_count > 0 else 0.0

    # KPI-7: Poly allergen patient rate
    poly_count = df.groupBy("uuid").agg(countDistinct("allergy_code").alias("cnt")).filter(col("cnt") >= 2).count()
    poly_allergen_patient_rt: float = (poly_count / float(total_allrg_count) * 100.0) if total_allrg_count > 0 else 0.0

    # KPI-8: Drug hypersensitivity rate
    drug_count = df.filter((col("category") == "medication") | (col("allergen_nature") == "drug")).select("uuid").distinct().count()
    drug_hypersensitivity_rt: float = (drug_count / float(total_allrg_count) * 100.0) if total_allrg_count > 0 else 0.0

    # Original field: allergy_risk_stratification
    strat = df.groupBy("allergen_nature").agg(countDistinct("uuid").alias("cnt"))
    risk_strat = [
        (
            row["allergen_nature"] if row["allergen_nature"] else "Unknown",
            int(row["cnt"]),
            builtins.round((row["cnt"] / float(total_allrg_count) * 100.0), 2) if total_allrg_count else 0.0
        )
        for row in strat.collect()
    ]

    # Historical trends
    historical_trends = calculateHistoricalTrends(df, patients_df, encounters_df)

    kpi_data = allergyKPIS(
        total_allergic_population=total_allrg_count,
        active_allergy_percentage=builtins.round(active_all_percentage, 2),
        severe_allergy_incidence_rate=builtins.round(severe_allergy_rt, 2),
        allergic_patient_rate=builtins.round(allergic_patient_rt, 2),
        penicillin_allergy_delabeling_eligibility_rate=builtins.round(penicillin_delabeling_rt, 2),
        allergy_related_readmission_rate=builtins.round(readmission_rt, 2),
        poly_allergen_patient_rate=builtins.round(poly_allergen_patient_rt, 2),
        drug_hypersensitivity_rate=builtins.round(drug_hypersensitivity_rt, 2),
        allergy_risk_stratification=risk_strat,
        historical_data=historical_trends
    ).dict()

    data = {
        "entity_name": "allergies",
        "metric_type": "Kpis",
        "data": kpi_data,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    await supabase.table("metrics").upsert(data, on_conflict="entity_name,metric_type").execute()

async def calculateMetrics(df, supabase):
    print("Calculation of Allergy based Metrics started at: ", datetime.now().strftime("%T-%m-%d %H:%M"))
    
    total_allrg_count: int = df.select("uuid").distinct().count()
    if total_allrg_count == 0:
        return

    # Metric 1: Top 10 causative agents
    agents = df.groupBy("allergy_description").agg(count("*").alias("count")) \
               .orderBy(desc("count")).limit(10)
    top_10 = [{"agent": row["allergy_description"], "count": int(row["count"])} for row in agents.collect()]

    # Metric 2: Severity distribution
    severities = df.groupBy("primary_symptom_severity").agg(count("*").alias("count")).orderBy("primary_symptom_severity")
    severity_dist = [{"severity": row["primary_symptom_severity"], "count": int(row["count"])} for row in severities.collect()]

    # Metric 3: Allergy discovery trends
    trends = df.withColumn("year_val", year("allergy_detection_date")) \
               .groupBy("year_val", "category").agg(count("*").alias("count")) \
               .orderBy("year_val", "category")
    discovery_trends = [
        {
            "year": int(row["year_val"]) if row["year_val"] is not None else 0,
            "category": row["category"],
            "count": int(row["count"])
        }
        for row in trends.collect()
    ]

    # Metric 4: Allergy distribution by category
    categories = df.groupBy("category").agg(count("*").alias("count")).orderBy("category")
    cat_dist = [{"category": row["category"], "count": int(row["count"])} for row in categories.collect()]

    metrics_data = allergyMetrics(
        top_10_causative_agents=top_10,
        severity_distribution=severity_dist,
        allergy_discovery_trends=discovery_trends,
        allergy_distribution_by_category=cat_dist
    ).dict()

    data = {
        "entity_name": "allergies",
        "metric_type": "Metrics",
        "data": metrics_data,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    await supabase.table("metrics").upsert(data, on_conflict="entity_name,metric_type").execute()

async def calculateAdvancedMetrics(df, supabase):
    print("Calculation of Allergy based Advanced Metrics at: ", datetime.now().strftime("%T-%m-%d %H:%M"))
    
    total_allrg_count: int = df.select("uuid").distinct().count()
    if total_allrg_count == 0:
        return

    # Fetch patients for birth_date in age onset calculation
    patients_resp = await supabase.table("patients").select("uuid, birth_date").execute()
    patients_df = df.sparkSession.createDataFrame(patients_resp.data)
    patients_clean = patients_df.withColumn("birth_date", to_date(col("birth_date")))

    # Advanced Metric 1: Cross reactivity risk matrix (drug allergy co-occurrences)
    df_a = df.select("uuid", col("allergy_description").alias("allergy_A"))
    df_b = df.select("uuid", col("allergy_description").alias("allergy_B"))
    df_self = df_a.join(df_b, "uuid").filter(col("allergy_A") < col("allergy_B"))
    co_occurrences = df_self.groupBy("allergy_A", "allergy_B") \
                            .agg(count("*").alias("co_occurrence_count")) \
                            .orderBy(desc("co_occurrence_count")).limit(20)
    cross_matrix = [
        {
            "allergy_A": row["allergy_A"],
            "allergy_B": row["allergy_B"],
            "co_occurrence_count": int(row["co_occurrence_count"])
        }
        for row in co_occurrences.collect()
    ]

    # Advanced Metric 2: Allergen nature severity matrix
    nature_severity = df.groupBy("allergen_nature", "primary_symptom_severity") \
                        .agg(count("*").alias("count")) \
                        .orderBy("allergen_nature", "primary_symptom_severity")
    nat_sev_matrix = [
        {
            "allergen_nature": row["allergen_nature"],
            "severity": row["primary_symptom_severity"],
            "count": int(row["count"])
        }
        for row in nature_severity.collect()
    ]

    # Advanced Metric 3: Age at onset distribution
    onset_df = df.join(patients_clean.select("uuid", "birth_date"), "uuid") \
                 .withColumn("age_at_onset", (datediff(col("allergy_detection_date"), col("birth_date")) / 365.25)) \
                 .withColumn("age_group_floor", floor(col("age_at_onset") / 10) * 10)
    onset_df = onset_df.withColumn("age_group", concat(col("age_group_floor").cast("string"), lit("-"), (col("age_group_floor") + 9).cast("string")))
    age_onset = onset_df.groupBy("age_group").agg(count("*").alias("count")).orderBy("age_group")
    age_dist = [{"age_group": row["age_group"], "count": int(row["count"])} for row in age_onset.collect() if row["age_group"] is not None]

    # Advanced Metric 4: Pollen seasonality acceleration (MoM growth rate for environment allergies)
    env_df = df.filter((col("category") == "environment") | (col("allergen_nature") == "environment")) \
               .withColumn("month_num", month("allergy_detection_date"))
    month_counts = env_df.groupBy("month_num").agg(count("*").alias("count")).orderBy("month_num")
    w = Window.orderBy("month_num")
    month_growth = month_counts.withColumn("prev_count", lag("count").over(w)) \
                               .withColumn("mom_growth_rate", 
                                           when(col("prev_count").isNotNull() & (col("prev_count") > 0),
                                                spark_round(((col("count") - col("prev_count")) / col("prev_count")) * 100.0, 2))
                                           .otherwise(lit(0.0)))
    pollen_seasonality = [
        {
            "month": month_to_num_mapping.get(row["month_num"], str(row["month_num"])),
            "count": int(row["count"]),
            "mom_growth_rate": float(row["mom_growth_rate"])
        }
        for row in month_growth.collect()
    ]

    adv_data = allergyAdvancedMetrics(
        cross_reactivity_risk_matrix=cross_matrix,
        allergen_nature_severity_matrix=nat_sev_matrix,
        age_at_onset_distribution=age_dist,
        pollen_seasonality_acceleration=pollen_seasonality
    ).dict()

    data = {
        "entity_name": "allergies",
        "metric_type": "Advanced_metrics",
        "data": adv_data,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    await supabase.table("metrics").upsert(data, on_conflict="entity_name,metric_type").execute()

async def run_allergy_analytics(spark, supabase):
    print("Fetching Allergy data at: ", datetime.now().strftime("%T-%m-%d %H:%M"))
    
    # 1. Fetch total count first
    response = await supabase.table("allergies").select("*", count="exact").limit(0).execute()
    row_count = response.count
    
    if row_count == 0:
        print("No data found in 'allergies' table. Skipping analytics.")
        return

    all_data = []
    batch_size = 1000
    for i in range(0, row_count, batch_size):
        response = await supabase.table("allergies").select("*").range(i, i + batch_size - 1).execute()
        if response.data:
            all_data.extend(response.data)

    df = spark.createDataFrame(all_data, schema=ALLERGIES_SCHEMA)
    
    # Convert strings back to timestamps/dates for Spark math
    df = df.withColumn("allergy_detection_date", to_date(col("allergy_detection_date"))) \
           .withColumn("allergy_cure_date", to_date(col("allergy_cure_date")))
    
    await calculateKPIS(df, supabase)
    await calculateMetrics(df, supabase)
    await calculateAdvancedMetrics(df, supabase)
    print("Allergy Analytics Sync Complete.")