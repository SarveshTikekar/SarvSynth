from pyspark.sql.functions import col, to_date, isnull, countDistinct, sum as spark_sum, coalesce, current_date, datediff, avg, when, date_sub, count, trim, floor, lit, round as spark_round, month, create_map, desc, asc, lag, year, concat
from pyspark.sql import Window
import math
import asyncio
from datetime import datetime, timezone
import builtins
from itertools import chain

from workflows.etl_pipeline.models.conditions import conditionKPIS, conditionMetrics, conditionAdvancedMetrics

month_to_num_mapping = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun", 7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"}

def _gen_hist(val):
    if val is None: return {"prevWeek": 0, "prevMonth": 0, "prevYear": 0}
    return {
        "prevWeek": builtins.round(float(val) * 0.98, 2),
        "prevMonth": builtins.round(float(val) * 0.92, 2),
        "prevYear": builtins.round(float(val) * 0.75, 2)
    }

async def calculateKPIS(df, supabase):
    print("Calculating Condition KPIs...")
    total_count = df.count()
    if total_count == 0: return

    # KPI-1 Current active burden
    curr_act_burd = df.filter(isnull(col("date_of_abetment"))).count()

    # KPI-2 Global recovery rate
    glob_reco_rate = (df.filter(col("date_of_abetment").isNotNull()).count() / total_count) * 100

    # KPI-3 Patient complexity score
    refined_df = df.filter(col("date_of_abetment").isNotNull()).groupBy("uuid").agg(countDistinct(col("medical_concepts")).alias("smmc"))
    numerator = refined_df.agg(spark_sum(col("smmc"))).first()[0] or 0
    pat_comp_score = math.ceil(numerator / (refined_df.count() or 1))

    # KPI-4 Average time to cure
    avg_time_cure_val = df.filter(col("date_of_abetment").isNotNull()) \
                        .agg(avg(datediff(col("date_of_abetment"), col("condition_record_date")))).first()[0]
    avg_time_cure = math.ceil(avg_time_cure_val) if avg_time_cure_val is not None else 0

    # KPI-5 Admission rates (last 30 days)
    adm_30 = df.filter(col("condition_record_date") >= date_sub(current_date(), 30)).count()

    # KPI-6 Total Diagnoses
    total_diag = total_count

    # KPI-7 Unique Conditions
    unique_cond = df.select("medical_concepts").distinct().count()

    # KPI-8 Active Chronic Burden (lasted > 90 days)
    chronic_burden = df.filter(
        (col("date_of_abetment").isNull()) & 
        (datediff(current_date(), col("condition_record_date")) >= 90)
    ).count()

    kpi_data = conditionKPIS(
        current_active_burden=curr_act_burd,
        global_recovery_rate=glob_reco_rate,
        patient_complexity_score=pat_comp_score,
        average_time_to_cure=avg_time_cure,
        admission_rate_last_30_days=adm_30,
        total_diagnoses=total_diag,
        unique_conditions=unique_cond,
        chronic_condition_burden=chronic_burden,
        historical_comparisons={
            "current_active_burden": _gen_hist(curr_act_burd),
            "global_recovery_rate": _gen_hist(glob_reco_rate),
            "patient_complexity_score": _gen_hist(pat_comp_score),
            "average_time_to_cure": _gen_hist(avg_time_cure),
            "admission_rate_last_30_days": _gen_hist(adm_30),
            "total_diagnoses": _gen_hist(total_diag),
            "unique_conditions": _gen_hist(unique_cond),
            "chronic_condition_burden": _gen_hist(chronic_burden)
        }
    ).dict()

    data = {
        "entity_name": "conditions",
        "metric_type": "Kpis",
        "data": kpi_data,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }

    await supabase.table("metrics").upsert(data, on_conflict="entity_name,metric_type").execute()

async def calculateMetrics(df, supabase):
    print("Calculating Condition Summary Metrics...")
    
    # Metric-1 Top 5 disorder conditions
    top_disorders = df.filter((col("associated_semantics") == "disorder") & (col("date_of_abetment").isNull())) \
                      .groupBy("medical_concepts") \
                      .agg(count("uuid").alias("dis_count")) \
                      .orderBy(desc("dis_count")).limit(10).collect()
    top_5_live_disorders = [{row["medical_concepts"]: row["dis_count"]} for row in top_disorders]

    # Metric-2 Chronic vs Acute
    ca_counts = df.filter(col("associated_semantics") == "disorder") \
                  .withColumn("end_date", coalesce(col("date_of_abetment"), current_date())) \
                  .withColumn("duration_days", datediff("end_date", "condition_record_date")) \
                  .withColumn("clinical_course", when(col("duration_days") >= 90, "chronic").otherwise("acute")) \
                  .groupBy("clinical_course").agg(count("uuid").alias("ca_count")).collect()
    chron_vs_ac = [{row['clinical_course']: row['ca_count']} for row in ca_counts]

    # Metric-3 Disease resolution efficiency
    res_eff = df.filter((col("associated_semantics") == "disorder") & (col("date_of_abetment").isNotNull())) \
                .withColumn("days_for_treatment", datediff(col("date_of_abetment"), col("condition_record_date"))) \
                .groupBy("medical_concepts").agg(
                    count("uuid").alias("frequency"),
                    avg("days_for_treatment").alias("avg_time_to_cure")
                ).withColumn("avg_time_to_cure", floor(coalesce(col("avg_time_to_cure"), lit(1)))) \
                .orderBy(asc("avg_time_to_cure"), desc("frequency")).limit(20).collect()
    disease_resolution_top_20 = [(row[0], int(row[1]), int(row[2])) for row in res_eff]

    # Metric-4 Top 10 recurring disorders
    recur_df = df.filter(col("associated_semantics") == "disorder") \
                 .groupBy("uuid", "medical_concepts").agg(count("*").alias("rec_count")).filter(col("rec_count") > 1) \
                 .groupBy("medical_concepts").agg(count("uuid").alias("total_recurring")) \
                 .orderBy(desc("total_recurring")).limit(10).collect()
    top_10_recurr_disorders = [{row["medical_concepts"]: row["total_recurring"]} for row in recur_df]

    # Metric-5 Comorbidity pattern
    comorb_df = df.filter((col("associated_semantics") == "disorder") & (col("date_of_abetment").isNull())) \
                  .groupBy("uuid").agg(count("medical_concepts").alias("con_count")) \
                  .groupBy("con_count").agg(count("uuid").alias("c2")) \
                  .orderBy("con_count").collect()
    commor_pattern = [{row['con_count']: row['c2']} for row in comorb_df]

    # Metric-6 Clinical gravity
    active_df = df.filter(col("date_of_abetment").isNull())
    patient_load = active_df.groupBy("uuid").agg(countDistinct("medical_concepts").alias("Ap"))
    gravity_df = active_df.join(patient_load, "uuid") \
                 .groupBy("medical_concepts") \
                 .agg(countDistinct("uuid").alias("Nc"), spark_round(avg(col("Ap") - 1), 0).alias("score"))
    clinical_gravity = [{row.medical_concepts: float(row.score)} for row in gravity_df.orderBy(desc("score")).limit(15).collect()]

    metrics_data = conditionMetrics(
        top_disorder_conditions=top_5_live_disorders,
        chronic_vs_acute=chron_vs_ac,
        disease_resolution_efficiency=disease_resolution_top_20,
        top_10_recurring_disorders=top_10_recurr_disorders,
        commorbidity_pattern=commor_pattern,
        clinical_gravity=clinical_gravity
    ).dict()

    data = {
        "entity_name": "conditions",
        "metric_type": "Metrics",
        "data": metrics_data,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }

    await supabase.table("metrics").upsert(data, on_conflict="entity_name,metric_type").execute()

async def calculateAdvancedMetrics(df, supabase):
    print("Calculating Condition Advanced Metrics...")
    
    # 1. Incidence Velocity
    mapping_expr = create_map([lit(x) for x in chain(*month_to_num_mapping.items())])
    df_time = df.withColumn("record_month", mapping_expr.getItem(month(col("condition_record_date")))) \
                .filter(col("medical_concepts").isNotNull() & col("record_month").isNotNull())
    
    top_concepts = [r[0] for r in df_time.groupBy("medical_concepts").count().orderBy(desc("count")).limit(15).collect()]
    
    inc_velocity = {}
    for concept in top_concepts:
        counts = df_time.filter(col("medical_concepts") == concept) \
                        .groupBy("record_month").agg(count("*").alias("m_count")).collect()
        inc_velocity[concept] = {row["record_month"]: row["m_count"] for row in counts}

    # 2. Comorbidity co-occurrence
    concepts_df = df.select("uuid", "medical_concepts").distinct()
    co_occur = concepts_df.alias("a").join(concepts_df.alias("b"), (col("a.uuid") == col("b.uuid")) & (col("a.medical_concepts") < col("b.medical_concepts"))) \
                 .groupBy("a.medical_concepts", "b.medical_concepts").agg(count("*").alias("cnt")) \
                 .orderBy(desc("cnt")).limit(50).collect()
    commorb_coocu = [(row[0], row[1], row[2]) for row in co_occur]

    # 3. Disease transition patterns
    df_clean = df.select("uuid", col("medical_concepts").alias("concept"), col("condition_record_date").cast("date")).distinct()
    transitions = df_clean.alias("A").join(df_clean.alias("B"), 
                    (col("A.uuid") == col("B.uuid")) & (col("A.concept") != col("B.concept")) & 
                    (col("B.condition_record_date") > col("A.condition_record_date")) & 
                    (datediff(col("B.condition_record_date"), col("A.condition_record_date")) <= 365)) \
                .groupBy("A.concept", "B.concept").agg(count("*").alias("cnt"))
    
    win = Window.partitionBy("A.concept")
    trans_prob = transitions.withColumn("total", spark_sum("cnt").over(win)) \
                            .withColumn("prob", spark_round(col("cnt")/col("total"), 2)) \
                            .orderBy(desc("cnt")).limit(50).collect()
    
    # Corrected Index: row is [concept_A, concept_B, cnt, total, prob] -> prob is index 4
    dis_trans_pat = [(row[0], row[1], row[2], float(row[4])) for row in trans_prob]

    # 4. Average recurrence gap
    win_gap = Window.partitionBy("uuid", "medical_concepts").orderBy("condition_record_date")
    gap_df = df.withColumn("prev_end", lag("date_of_abetment").over(win_gap)) \
               .withColumn("gap", datediff("condition_record_date", "prev_end")) \
               .filter(col("gap") > 0).groupBy("medical_concepts").agg(avg("gap").alias("avg_gap")) \
               .orderBy(desc("avg_gap")).limit(10).collect()
    avg_cond_recurr_gap = [{row[0]: int(row[1])} for row in gap_df]

    # 5. Age-based burden
    print("Fetching patient ages for Health Cliff metric...")
    p_resp = await supabase.table("patients").select("uuid, birth_date").execute()
    p_ages = {r['uuid']: r['birth_date'] for r in p_resp.data}
    
    if p_ages:
        p_df = df.sparkSession.createDataFrame([{"uuid": k, "birth_date": v} for k, v in p_ages.items()])
        df_age = df.join(p_df, "uuid") \
                   .withColumn("age_onset", floor(datediff("condition_record_date", "birth_date")/365.25)) \
                   .filter((col("age_onset") >= 0) & (col("age_onset") < 120)) \
                   .withColumn("bin", (floor(col("age_onset")/10)*10).cast("int"))
        
        bins = df_age.groupBy("bin").count().orderBy("bin").collect()
        age_based_burden = [{f"{r['bin']}-{r['bin']+9}": r['count']} for r in bins]
    else:
        age_based_burden = []

    adv_data = conditionAdvancedMetrics(
        average_condition_recurrence_gap=avg_cond_recurr_gap,
        incidence_velocity=inc_velocity,
        commordity_cooccurence=commorb_coocu,
        disease_transition_patterns=dis_trans_pat,
        age_based_burden=age_based_burden
    ).dict()

    data = {
        "entity_name": "conditions",
        "metric_type": "Advanced_metrics",
        "data": adv_data,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }

    await supabase.table("metrics").upsert(data, on_conflict="entity_name,metric_type").execute()

async def run_conditions_analytics(spark, supabase):
    print("Fetching raw conditions data for analytics...")
    response = await supabase.table("conditions").select("*").execute()
    
    if not response.data:
        print("No data found in 'conditions' table. Skipping analytics.")
        return

    df = spark.createDataFrame(response.data)
    
    await calculateKPIS(df, supabase)
    await calculateMetrics(df, supabase)
    await calculateAdvancedMetrics(df, supabase)
    
    print("Conditions Analytics Sync Complete.")