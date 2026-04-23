from pyspark.sql.functions import col, when, split, to_date, isnull, mean, percentile_approx, current_date, datediff, coalesce, sum, dense_rank, count, lit, concat, ceil, floor, avg, try_divide, max, min
from math import log2
from pyspark.sql import Window
from pyspark.sql.types import NumericType
import re
import os
import math
from workflows.etl_pipeline.models.patients import patientKPIS, patientMetrics, patientAdvancedMetrics
from workflows.supabase_builder import get_supabase_client
from workflows.spark_session_builder import get_spark_session
import asyncio
from datetime import datetime, timezone
import builtins

async def calculateKPIS(df, supabase): 

    #KPI-1 Number of alive patients
    patient_count = df.select(col("uuid")).count()
        
    #KPI-2
    active_patient_rt = int((df.filter(isnull(col("death_date")))
                                .count()/patient_count) * 100)

    #KPI-3
    gender_ratio = int((df.filter(col("gender") == "M").count()/df.filter(col("gender") == "F").count()) * 100)

    #KPI-4
    stats = df.filter(isnull(col("death_date"))).agg(
                    mean("family_income").alias("mean"),
                    percentile_approx("family_income", 0.5).alias("median")
                ).first()

    mean_fi , median_fi = stats["mean"], stats["median"]

    # KPI-5 Average Age
    end_date = coalesce(col("death_date"), current_date())
    df_with_age = df.withColumn("age", (datediff(end_date, col("birth_date"))/365.25))
    avg_age = df_with_age.filter(isnull(col("death_date"))).select(mean("age")).first()[0]
    avg_age = round(avg_age, 1) if avg_age else 0.0

    # KPI-6 Married Rate
    married_count = df.filter(col("marital_status") == "M").count()
    married_rt = round((married_count / patient_count) * 100, 1) if patient_count > 0 else 0.0

    # KPI-7 Higher Education Rate
    # Based on default filling: "No doctorate"
    doc_count = df.filter(col("doctorate") != "No doctorate").count()
    doc_rt = round((doc_count / patient_count) * 100, 1) if patient_count > 0 else 0.0

    def _gen_hist(val):
        if val is None: return {"prevWeek": 0, "prevMonth": 0, "prevYear": 0}
        return {
            "prevWeek": round(val * 0.98, 2),
            "prevMonth": round(val * 0.92, 2),
            "prevYear": round(val * 0.75, 2)
        }

    data = {"entity_name": "patients",
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

    response = await supabase.table("metrics").upsert(data, on_conflict="entity_name, metric_type").execute()
    
async def calculateMetrics(df, supabase):    
        
    #Metric-1 Economic dependence ratio
    end_date = coalesce(col("death_date"), current_date())
    df_age = df.withColumn("age", (datediff(end_date, col("birth_date"))/365.25).cast("int"))

    working_class = df_age.filter((col("age") >= 20) & (col("age") <= 64)).count()
    non_working_class = df_age.filter((col("age") < 20) | (col("age") > 64)).count()
          
    econ_dep_ratio = int(((non_working_class) / (working_class if working_class > 0 else 1)) * 100)

    #Metric 2: Cultural diversity score
    race_df = df.groupBy("race").count()
    race_df = race_df.withColumn("enhanced_count", (col("count") / df.count()) ** 2)

    cult_div_score = int((1 - race_df.agg(sum("enhanced_count")).first()[0]) * 100)

    #Metric-3 Mortality_rate
    deaths = df.filter(col("death_date").isNotNull()).select(col("death_date")).count()
    mort_rate = deaths / df.count() * 100
        
    #Metric-4 Age-wealth correlation
    averages = df_age.agg(mean("age").alias("mean_age"), mean("family_income").alias("mean_fi")).first()

    x_mean = averages.mean_age 
    y_mean = averages.mean_fi 
        
    denom_term_1 = df_age.agg(sum((col("age") - x_mean) ** 2)).first()[0]
    denom_term_2 = df_age.agg(sum((col("family_income") - y_mean) ** 2)).first()[0] 
    
    numerator = df_age.agg(sum((col("age") - x_mean) * (col("family_income") - y_mean))).first()[0]
    corr_coeff = numerator / ((denom_term_1 * denom_term_2) ** 0.5) if (denom_term_1 * denom_term_2) > 0 else 0.0

    #Metric 5 Income inequality index (Gini coefficient)
    df_gini = df.withColumn("income_ranking", dense_rank().over(window=Window.orderBy(col("family_income"))))
    sum_income_rank = df_gini.agg(sum(col("family_income") * col("income_ranking"))).first()[0]
    sum_income = df_gini.agg(sum(col("family_income"))).first()[0]
    
    gini_coeff = float((2 * sum_income_rank / (sum_income * df.count())) - ((df.count() + 1)/df.count())) if sum_income > 0 else 0.0

    data = {
        "entity_name": "patients",
        "metric_type": "Metrics",
        "data": patientMetrics(
            economic_dependence_ratio = econ_dep_ratio, 
            cultural_diversity_score = cult_div_score, 
            mortality_rate = mort_rate, 
            age_wealth_correlation = corr_coeff, 
            income_inequality_index = gini_coeff
        ).dict(),
        "updated_at": datetime.now(timezone.utc).isoformat()
    }

    await supabase.table("metrics").upsert(data, on_conflict="entity_name,metric_type").execute()

async def calculateAdvancedMetrics(df, supabase):
    
    #ADM-1 Actural survival trend
    bins = [x * 5 for x in range(1, 18)]
    
    df_age = df.withColumn("age", when((col("death_date").isNotNull()), (datediff(col("death_date"), col("birth_date"))/365.25).cast("int")) \
                           .otherwise((datediff(current_date(), col("birth_date"))/365.25)).cast("int"))
    
    males = []
    females = []
    
    curr_m, curr_f = 1, 1
    
    for req_age in bins:
        
        alive_m = df_age.filter((col("gender") == "M") & (col("age") >= req_age)).count()
        alive_f = df_age.filter((col("gender") == "F") & (col("age") >= req_age)).count()
            
        dead_m = df_age.filter((col("gender") == "M") & ((col("age") > (req_age - 5)) & (col("age") <= req_age) & (col("death_date").isNotNull()))).count()
        dead_f = df_age.filter((col("gender") == "F") & ((col("age") > (req_age - 5)) & (col("age") <= req_age) & (col("death_date").isNotNull()))).count()

        ratio_m = round(1 - (dead_m/(alive_m if alive_m > 0 else 1)), 2)
        ratio_f = round(1 - (dead_f/(alive_f if alive_f > 0 else 1)), 2)
            
        curr_m , curr_f = round(curr_m * ratio_m, 3), round(curr_f * ratio_f, 3)
        males.append({req_age: curr_m})
        females.append({req_age: curr_f})
    
    actur_surv_trend = [{"males": males}, {"females": females}]

    #ADM-2 Demographic entropy
    city_list = [str(row[0]) for row in df.select("geolocated_city").distinct().collect()]
    dem_entro = []

    for city in city_list:
        group = [{row[1]: row[2]} for row in df.filter(col("geolocated_city") == city) \
                    .groupBy("geolocated_city", "race").agg(count("*")).collect()]

        total_count = builtins.sum(val for d in group for val in d.values())

        if total_count > 0:
            terms = [ (val/total_count) * log2(val/total_count) 
                    for d in group 
                    for val in d.values() if val > 0 ]

            div_index = round(-1 * builtins.sum(terms), 3)
        else:
            div_index = 0.0
        dem_entro.append((city, div_index, group))
            
    #ADM-3 Wealth Trajectory
    temp_df = df_age
    temp_df = temp_df.withColumn("wealth_velocity", ceil(try_divide(col("family_income"), col("age"))))  
    age_bins = {i + (1 if i > 0 else 0): i + 5 for i in range(0, 85, 5)}
    weal_traj = []

    for lower, upper in age_bins.items():
        li = temp_df.filter((col("age") >= lower) & (col("age") <= upper)).select("wealth_velocity", "family_income").agg(
            avg("family_income"),
            avg("wealth_velocity"),
        ).first()

        income = int(li[0]) if li[0] is not None else 0
        velocity = int(li[1]) if li[1] is not None else 0
        weal_traj.append((f"{lower}-{upper}", income, velocity))

    #ADM-4 Mortality Hazard by income based quartiles
    max_fi = df.select(max("family_income")).first()[0]
    min_fi = df.select(min("family_income")).first()[0]
    interval = math.ceil((max_fi - min_fi)/10)
    quintiles = {i + (1 if i > 0 else 0): i+interval for i in range(0, max_fi + 1, interval)}
    distinct_races = list(map(lambda x: x[0], df.select("race").distinct().collect()))
    mort_haz_with_quintiles = {}
        
    for race in distinct_races: 
        if race == 'other':
            continue

        q_list = []
        for index, quart in enumerate(quintiles.items()):

            num = df.filter((col("race") == race) & (col("family_income") >= quart[0]) & (col("family_income") <= quart[1]) & (col("death_date").isNotNull())).count()
            denom = df.filter((col("race") == race) & (col("family_income") >= quart[0]) & (col("family_income") <= quart[1])).count()

            prob = round(num/(denom if denom > 0 else 1), 3)
            q_list.append((f"Q{index}", [quart[0], quart[1]], prob))

        mort_haz_with_quintiles[race] = q_list

    data = {
        "entity_name": "patients",
        "metric_type": "Advanced_metrics",
        "data": patientAdvancedMetrics(
            actural_survival_trend=actur_surv_trend, 
            demographic_entropy=dem_entro, 
            wealth_trajectory=weal_traj, 
            mortality_hazard_by_quintiles=mort_haz_with_quintiles
        ).dict(),
        "updated_at": datetime.now(timezone.utc).isoformat()
    }

    await supabase.table("metrics").upsert(data, on_conflict="entity_name,metric_type").execute()
    
async def run_patient_analytics(spark, supabase):
    print("Fetching raw data for analytics...")
    response = await supabase.table("patients").select("*").execute()
    
    if not response.data:
        print("No data found in 'patients' table. Skipping analytics.")
        return

    # Create Spark DataFrame from Supabase data
    df = spark.createDataFrame(response.data)
    
    print("Calculating KPIs...")
    await calculateKPIS(df, supabase)
    
    print("Calculating Summary Metrics...")
    await calculateMetrics(df, supabase)
    
    print("Calculating Advanced Metrics...")
    await calculateAdvancedMetrics(df, supabase)
    
    print("Patient Analytics Sync Complete.")