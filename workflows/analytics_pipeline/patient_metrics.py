from pyspark.sql.functions import col, when, split, to_date, isnull, mean, percentile_approx, current_date, datediff, coalesce, sum as spark_sum, dense_rank, count, lit, concat, ceil, floor, avg, try_divide, max, min, year, isnotnull
from math import log2
from pyspark.sql import Window
import re
import os
import math
from workflows.etl_pipeline.models.patients import patientKPIS, patientMetrics, patientAdvancedMetrics
import asyncio
from datetime import datetime, timezone
import builtins
from functools import reduce
from pyspark.sql import DataFrame

async def calculateKPIS(df, supabase): 
    print("Calculating Patient KPIs...")
    patient_count = df.count()
    if patient_count == 0: return
        
    active_patient_rt = int((df.filter(isnull(col("death_date"))).count()/patient_count) * 100)
    gender_ratio = int((df.filter(col("gender") == "M").count()/(df.filter(col("gender") == "F").count() or 1)) * 100)

    stats = df.filter(isnull(col("death_date"))).agg(
                    mean("family_income").alias("mean"),
                    percentile_approx("family_income", 0.5).alias("median")
                ).first()
    mean_fi, median_fi = stats["mean"] or 0, stats["median"] or 0

    end_date = coalesce(col("death_date"), current_date())
    df_with_age = df.withColumn("age", (datediff(end_date, col("birth_date"))/365.25))
    avg_age = df_with_age.filter(isnull(col("death_date"))).select(mean("age")).first()[0]
    avg_age = builtins.round(float(avg_age), 1) if avg_age else 0.0

    married_count = df.filter(col("marital_status") == "M").count()
    married_rt = builtins.round((married_count / patient_count) * 100, 1) if patient_count > 0 else 0.0

    doc_count = df.filter(col("doctorate") != "No doctorate").count()
    doc_rt = builtins.round((doc_count / patient_count) * 100, 1) if patient_count > 0 else 0.0

    data = {
        "entity_name": "patients",
        "metric_type": "Kpis",
        "data": patientKPIS(
            total_patients = patient_count, 
            active_patient_rate = active_patient_rt, 
            gender_balance_ratio = gender_ratio, 
            mean_family_income = int(mean_fi), 
            median_family_income = int(median_fi),
            avg_patient_age = avg_age,
            married_rate = married_rt,
            higher_education_rate = doc_rt,
        ).dict(),
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    await supabase.table("metrics").upsert(data, on_conflict="entity_name,metric_type").execute()
    
async def calculateMetrics(df, supabase):    
    print("Calculating Patient Summary Metrics...")
    end_date = coalesce(col("death_date"), current_date())
    df_age = df.withColumn("age", (datediff(end_date, col("birth_date"))/365.25).cast("int"))

    working_class = df_age.filter((col("age") >= 20) & (col("age") <= 64)).count()
    non_working_class = df_age.filter((col("age") < 20) | (col("age") > 64)).count()
    econ_dep_ratio = int(((non_working_class) / (working_class if working_class > 0 else 1)) * 100)

    race_df = df.groupBy("race").count()
    race_df = race_df.withColumn("enhanced_count", (col("count") / df.count()) ** 2)
    cult_div_score = int((1 - race_df.agg(spark_sum("enhanced_count")).first()[0]) * 100)

    deaths = df.filter(col("death_date").isNotNull()).count()
    mort_rate = builtins.round((deaths / df.count() * 100), 2) if df.count() > 0 else 0.0
        
    averages = df_age.agg(mean("age").alias("mean_age"), mean("family_income").alias("mean_fi")).first()
    x_mean = averages.mean_age or 0
    y_mean = averages.mean_fi or 0
        
    denom_term_1 = df_age.agg(spark_sum((col("age") - x_mean) ** 2)).first()[0]
    denom_term_2 = df_age.agg(spark_sum((col("family_income") - y_mean) ** 2)).first()[0] 
    numerator = df_age.agg(spark_sum((col("age") - x_mean) * (col("family_income") - y_mean))).first()[0]
    corr_coeff = numerator / ((denom_term_1 * denom_term_2) ** 0.5) if (denom_term_1 * denom_term_2) > 0 else 0.0

    df_gini = df.withColumn("income_ranking", dense_rank().over(window=Window.orderBy(col("family_income"))))
    sum_income_rank = df_gini.agg(spark_sum(col("family_income") * col("income_ranking"))).first()[0]
    sum_income = df_gini.agg(spark_sum(col("family_income"))).first()[0]
    gini_coeff = float((2 * sum_income_rank / (sum_income * df.count())) - ((df.count() + 1)/df.count())) if sum_income > 0 else 0.0

    data = {
        "entity_name": "patients",
        "metric_type": "Metrics",
        "data": patientMetrics(
            economic_dependence_ratio = econ_dep_ratio, 
            cultural_diversity_score = cult_div_score, 
            mortality_rate = mort_rate, 
            age_wealth_correlation = builtins.round(float(corr_coeff), 3), 
            income_inequality_index = builtins.round(float(gini_coeff), 3)
        ).dict(),
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    await supabase.table("metrics").upsert(data, on_conflict="entity_name,metric_type").execute()

async def calculateAdvancedMetrics(df, supabase):
    print("Calculating Patient Advanced Metrics...")
    
    # --- 1. 10-Year Trends (Normalized for Recharts) ---
    current_yr = datetime.now().year
    econ_trend, cult_trend, mort_trend = [], [], []

    for i in range(1, 11):
        target_year = current_yr - i
        ref_date = lit(f"{target_year}-12-31").cast("date")
        df_yearly = df.filter((year(col("birth_date")) <= target_year) & ((col("death_date").isNull()) | (year(col("death_date")) > target_year)))
        pop_count = df_yearly.count()
        
        if pop_count > 0:
            df_age_yr = df_yearly.withColumn("age_yr", (datediff(ref_date, col("birth_date")) / 356.25).cast("int"))
            working = df_age_yr.filter((col("age_yr") >= 20) & (col("age_yr") <= 64)).count()
            econ_ratio = int(((pop_count - working) / (working if working > 0 else 1)) * 100)
            econ_trend.append({"name": str(target_year), "value": econ_ratio})
            
            race_yr = df_yearly.groupBy("race").count()
            race_yr = race_yr.withColumn("enh", (col("count") / pop_count) ** 2)
            div_score = int((1 - race_yr.agg(spark_sum("enh")).first()[0]) * 100)
            cult_trend.append({"name": str(target_year), "value": div_score})
        else:
            econ_trend.append({"name": str(target_year), "value": 0})
            cult_trend.append({"name": str(target_year), "value": 0})
            
        dead_count = df.filter((isnotnull(col("death_date"))) & (year(col("death_date")) == target_year)).count()
        alive_count = df.filter(((isnull(col("death_date"))) | (year(col("death_date")) >= target_year)) & (year(col("birth_date")) <= target_year)).count()
        m_rate = builtins.round((dead_count / (alive_count if alive_count > 0 else 1)) * 100, 2)
        mort_trend.append({"name": str(target_year), "value": m_rate})

    # --- 2. Survival Trend ---
    bins = [x * 5 for x in range(1, 18)]
    df_age_calc = df.withColumn("age", when((col("death_date").isNotNull()), (datediff(col("death_date"), col("birth_date"))/365.25).cast("int")) \
                               .otherwise((datediff(current_date(), col("birth_date"))/365.25)).cast("int"))
    males, females = [], []
    curr_m, curr_f = 1, 1
    for req_age in bins:
        alive_m = df_age_calc.filter((col("gender") == "M") & (col("age") >= req_age)).count()
        alive_f = df_age_calc.filter((col("gender") == "F") & (col("age") >= req_age)).count()
        dead_m = df_age_calc.filter((col("gender") == "M") & ((col("age") > (req_age - 5)) & (col("age") <= req_age) & (col("death_date").isNotNull()))).count()
        dead_f = df_age_calc.filter((col("gender") == "F") & ((col("age") > (req_age - 5)) & (col("age") <= req_age) & (col("death_date").isNotNull()))).count()
        curr_m = builtins.round(curr_m * (1 - (dead_m/(alive_m or 1))), 3)
        curr_f = builtins.round(curr_f * (1 - (dead_f/(alive_f or 1))), 3)
        males.append({req_age: curr_m}); females.append({req_age: curr_f})
    
    # --- 3. Demographic Entropy ---
    city_list = [str(row[0]) for row in df.select("geolocated_city").distinct().collect()]
    dem_entro = []
    for city in city_list:
        group = [{row[1]: row[2]} for row in df.filter(col("geolocated_city") == city).groupBy("geolocated_city", "race").agg(count("*")).collect()]
        total = builtins.sum(v for d in group for v in d.values())
        div_index = builtins.round(-builtins.sum((v/total)*log2(v/total) for d in group for v in d.values() if v > 0), 3) if total > 0 else 0.0
        dem_entro.append((city, div_index, group))

    # --- 4. Wealth Trajectory ---
    df_wealth = df_age_calc.withColumn("wealth_velocity", ceil(try_divide(col("family_income"), col("age"))))  
    age_bins = {i + (1 if i > 0 else 0): i + 5 for i in range(0, 85, 5)}
    weal_traj = []
    for lower, upper in age_bins.items():
        stats = df_wealth.filter((col("age") >= lower) & (col("age") <= upper)).agg(avg("family_income"), avg("wealth_velocity")).first()
        weal_traj.append((f"{lower}-{upper}", int(stats[0] or 0), int(stats[1] or 0)))

    # --- 5. Mortality Hazard ---
    max_fi = df.select(max("family_income")).first()[0] or 0
    min_fi = df.select(min("family_income")).first()[0] or 0
    interval = math.ceil((max_fi - min_fi)/10) if max_fi > min_fi else 1000
    quint_range = {i + (1 if i > 0 else 0): i+interval for i in range(0, max_fi + 1, interval)}
    races = [r[0] for r in df.select("race").distinct().collect() if r[0] != 'other']
    mort_haz = {}
    for r in races:
        q_list = []
        for idx, (low, high) in enumerate(quint_range.items()):
            num = df.filter((col("race") == r) & (col("family_income") >= low) & (col("family_income") <= high) & (col("death_date").isNotNull())).count()
            denom = df.filter((col("race") == r) & (col("family_income") >= low) & (col("family_income") <= high)).count()
            q_list.append((f"Q{idx}", [low, high], builtins.round(num/(denom or 1), 3)))
        mort_haz[r] = q_list

    data = {
        "entity_name": "patients",
        "metric_type": "Advanced_metrics",
        "data": patientAdvancedMetrics(
            actural_survival_trend=[{"males": males}, {"females": females}], 
            demographic_entropy=dem_entro,
            wealth_trajectory=weal_traj,
            mortality_hazard_by_quintiles=mort_haz,
            economic_dependence_trend=econ_trend,
            cultural_diversity_trend=cult_trend,
            mortality_rate_trend=mort_trend
        ).dict(),
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    await supabase.table("metrics").upsert(data, on_conflict="entity_name,metric_type").execute()
    
async def run_patient_analytics(spark, supabase):
    print("Fetching raw data for patient analytics...")

    # 1. Fetch total count first (limit 0 is efficient)
    response = await supabase.table("patients").select("*", count="exact").limit(0).execute()
    row_count = response.count
    
    if row_count == 0:
        print("No patient data found to analyze.")
        return

    all_data = []
    batch_size = 1000
    for i in range(0, row_count, batch_size):
        # Inclusive range: e.g. 0 to 999, 1000 to 1999
        response = await supabase.table("patients").select("*").range(i, i + batch_size - 1).execute()
        if response.data:
            all_data.extend(response.data)
    
    # Create ONE DataFrame from the full list
    df = spark.createDataFrame(all_data)
    
    await calculateKPIS(df, supabase)
    await calculateMetrics(df, supabase)
    await calculateAdvancedMetrics(df, supabase)
    print("Patient Analytics Sync Complete.")