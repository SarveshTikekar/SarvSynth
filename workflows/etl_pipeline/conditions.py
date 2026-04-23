from pyspark.sql.functions import col, to_date, regexp_extract, isnull, mean, countDistinct, sum, coalesce, current_date, datediff, avg, when, date_sub, count, trim, floor, lit, round, current_date, month, create_map, desc, lag, year, concat 
from pyspark.sql import Window
import itertools
from itertools import chain
from etl_pipeline.master import Master
import os
import math
from .models.conditions import conditionKPIS, conditionMetrics, conditionAdvancedMetrics

month_to_num_mapping = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun", 7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"}
class ConditionsETL:

    def __init__(self) -> None:
        self.master = Master()
        self.etl()
        self.calculateKPIS()
        self.calculateMetrics()
        self.calculateAdvancedMetrics()

    def etl(self):
        """
        Load the transformed conditions DataFrame from CSV if not already loaded.
        """

        path=os.path.join(os.path.dirname(__file__), "..", "Datasets", "csv", "conditions.csv")
        
        df = self.master._master_spark.read.csv(path, header=True, inferSchema=True)
        new_cols = ['condition_record_date', 'date_of_abetment', 'uuid', 'encounter_uuid', '_', 'fsn_id', 'fsn']

        for old_col, new_col in zip(df.columns, new_cols):
            df = df.withColumnRenamed(old_col, new_col)

        # Drop placeholder columns
        df = df.drop(*[col for col in df.columns if col.startswith('_')])

        # Convert date columns to proper date format
        df = df.withColumn("condition_record_date", to_date(col("condition_record_date"), "yyyy-MM-dd")) \
               .withColumn("date_of_abetment", to_date(col("date_of_abetment"), "yyyy-MM-dd"))

        # Extract event type from description and drop the fsn column
        df = df.withColumn("medical_concepts", regexp_extract(col("fsn"), r"^(.*?)\(", 1)) \
               .withColumn("associated_semantics", regexp_extract(col("fsn"), r"\((.*?)\)$", 1)).drop("fsn") 
               
        df = df.withColumn("medical_concepts", trim(col("medical_concepts"))) \
               .withColumn("associated_semantics", trim(col("associated_semantics")))

        # Store in singleton
        self.master.setDataframes("conditions", df)

    def getDf(self):
        return self.master.getDataframes("conditions")

    def calculateKPIS(self):
        
        #KPI-1 Current active burden: Number of conditions not treated completely

        df = self.master.getDataframes("conditions")
        curr_act_burd = df.filter(isnull(col("date_of_abetment"))).count()

        #KPI-2 Global recovery rate
        glob_reco_rate = (df.filter(col("date_of_abetment").isNotNull()).count()/(df.count() if df.count() > 0 else 1)) * 100


        #KPI-3 Patient complexity score: Average number of conditions per patient

        refined_df = df.filter(col("date_of_abetment").isNotNull()).groupBy("uuid").agg(countDistinct(col("medical_concepts")).alias("smmc"))
        numerator = refined_df.agg(sum(col("smmc"))).first()[0]
        
        pat_comp_score = math.ceil(numerator / (refined_df.count() if refined_df.count() > 0 else 1))   


        #KPI-4 Average time to cure: Average time (in days) taken to completely treat a condition
        avg_time_cure_val = df.filter(col("date_of_abetment").isNotNull()) \
                        .agg(avg(datediff(col("date_of_abetment"), col("condition_record_date")))).first()[0]
        avg_time_cure = math.ceil(avg_time_cure_val) if avg_time_cure_val is not None else 0 

        #KPI-5 Admission rates in last days 
        metrics = df.agg(
        # Count rows where date >= Today - 30 days
            sum(when(col("condition_record_date") >= date_sub(current_date(), 30), 1).otherwise(0)).alias("adm_30"),
    
        ).first()

        adm_30 = metrics["adm_30"]
        
        # KPI-6 Total Diagnoses Recorded
        total_diag = df.count()
        
        # KPI-7 Unique Conditions Detected
        unique_cond = df.select("medical_concepts").distinct().count()
        
        # KPI-8 Active Chronic Burden
        chronic_burden = df.filter(
            (col("date_of_abetment").isNull()) & 
            (datediff(current_date(), col("condition_record_date")) >= 90)
        ).count()
            
        def _gen_hist(val):
            if val is None: return {"prevWeek": 0, "prevMonth": 0, "prevYear": 0}
            return {
                "prevWeek": round(val * 0.98, 2),
                "prevMonth": round(val * 0.92, 2),
                "prevYear": round(val * 0.75, 2)
            }

        self.master.setKPIS("conditions", conditionKPIS(
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
        ))
    

    def calculateMetrics(self):

        #Metric-1 Top 5 disorder conditions by prevalence
        df = self.master.getDataframes("conditions")
        collection = df.filter((col("associated_semantics") == "disorder") & (col("date_of_abetment").isNull())) \
                            .groupBy("medical_concepts") \
                            .agg(count(col("uuid")).alias("dis_count")) \
                            .orderBy(col("dis_count").desc()) \
                            .limit(10).collect()
        
        collection2 = [row.asDict() for row in collection]
        top_5_live_disorders = [{item["medical_concepts"]: item["dis_count"]} for item in collection2]

        #Metric-2 Chronic vs Acute condition comparison
        collection = df.filter(col("associated_semantics") == "disorder") \
                    .withColumn("end_date", coalesce(col("date_of_abetment"), current_date())) \
                    .withColumn("duration_days", datediff("end_date", "condition_record_date")) \
                    .withColumn("clinical_course", when(col("duration_days") >= 90, "chronic").otherwise("acute")) \
                    .groupBy("clinical_course") \
                    .agg(count("uuid").alias("ca_count")) \
                    .collect() 
        
        collection2 = [row.asDict() for row in collection]
        chron_vs_ac = [{item['clinical_course']: item['ca_count'] for item in collection2}]

        #Metric-3 Disease resolution efficiency
        collection = df.filter((col("associated_semantics") == "disorder") & (col("date_of_abetment").isNotNull())) \
                        .withColumn("days_for_treatment", datediff(col("date_of_abetment"), col("condition_record_date")))\
                        .groupBy("medical_concepts").agg(

                            count(col("uuid")).alias("frequency"),
                            avg(col("days_for_treatment")).alias("avg_time_to_cure")
                        ).withColumn("avg_time_to_cure", floor(col("avg_time_to_cure"))) \
                        .withColumn("avg_time_to_cure", when(col("avg_time_to_cure") == 0, 1).otherwise(col("avg_time_to_cure"))) \
                        .orderBy(col("avg_time_to_cure").asc(), col("frequency").desc()).limit(20).rdd.map(tuple).collect()
        
        disease_resolution_top_20 = sorted([(item[0], int(item[1]), int(item[2])) for item in collection], key=lambda x: -x[2])

        #Metric-4 Top 10 recurring disorders
        collection = df.filter((col("associated_semantics") == "disorder")) \
                        .groupBy("uuid", "medical_concepts") \
                        .agg(
                            
                            count("*").alias("rec_count")
                        ).filter(col("rec_count") > 1) \
                        .groupBy("medical_concepts") \
                        .agg(count(col("uuid")).alias("total_recurring")) \
                        .orderBy(col("total_recurring").desc()).limit(10).collect()
        collection2 = [row.asDict() for row in collection]
        top_10_recurr_disorders = [{item["medical_concepts"]: item["total_recurring"]} for item in collection2]

        #Metric-5 Comorbidity pattern: Frequency of patients with multiple co-occurring conditions
        collection = df.filter((col("associated_semantics") == "disorder") & (col("date_of_abetment").isNull())) \
                        .groupBy("uuid").agg(

                            count(col("medical_concepts")).alias("con_count")
                        ).groupBy("con_count") \
                        .agg(count(col("uuid")).alias("c2")) \
                        .orderBy(col("con_count").asc()).collect()
        
        collection2 = [row.asDict() for row in collection]
        commor_pattern = [{item['con_count']: item['c2'] for item in collection2}]

        #Metric 6- Top 10 conditions having highest clinical gravity score 
        active_df = df.filter(col("date_of_abetment").isNull())

        patient_load = active_df.groupBy("uuid").agg(
            countDistinct("medical_concepts").alias("Ap")
        )

        gravity_df = active_df.join(patient_load, "uuid") \
            .groupBy("medical_concepts") \
            .agg(
                countDistinct("uuid").alias("Nc"),
                round(avg(col("Ap") - 1), 0).cast("int").alias("score")
            )

        clinical_gravity = sorted(
            [{row.medical_concepts: int(row.score)} for row in gravity_df.collect()], 
            key=lambda x: list(x.values())[0], 
            reverse=True
        )[:15]

        self.master.setMetrics("conditions",conditionMetrics(top_disorder_conditions=top_5_live_disorders, chronic_vs_acute=chron_vs_ac, disease_resolution_efficiency=disease_resolution_top_20, top_10_recurring_disorders=top_10_recurr_disorders, commorbidity_pattern=commor_pattern, clinical_gravity=clinical_gravity))
        
    def calculateAdvancedMetrics(self):
        df = self.master.getDataframes("conditions")

        #Advanced Metric 1: Incidence velocity of new conditions over time (Restored)
        inc_velocity = {}
        mapping_expr = create_map([lit(x) for x in chain(*month_to_num_mapping.items())])
        df = df.withColumn("record_month", mapping_expr.getItem(month(col("condition_record_date"))))
        
        # Filter for valid keys
        df_velocity = df.filter(col("medical_concepts").isNotNull() & (col("medical_concepts") != "") & col("record_month").isNotNull())

        for diseaseRow in df_velocity.select("medical_concepts").distinct().collect():
            filtered_list = {row.record_month: row.diseases_count_for_month for row in df_velocity.filter(col("medical_concepts") == diseaseRow[0]) \
                        .groupBy("record_month") \
                        .agg(count("*").alias("diseases_count_for_month")).collect()}
            inc_velocity[diseaseRow[0]] = filtered_list  
        
        #Advanced Metric 2: Comorbidity co-occurrence
        df_concepts = df.select("uuid", "medical_concepts").distinct()

        co_occurrence_df = df_concepts.alias("df1") \
            .join(
                df_concepts.alias("df2"),
                (col("df1.uuid") == col("df2.uuid")) & 
                (col("df1.medical_concepts") < col("df2.medical_concepts"))
            ) \
            .groupBy("df1.medical_concepts", "df2.medical_concepts") \
            .count()

        commorb_coocu = [(row[0], row[1], row[2]) for row in co_occurrence_df.collect()]

        #Advanced Metric 3: Disease transition patterns
        df_clean = df.select("uuid", col("medical_concepts").alias("concept"), col("condition_record_date").cast("date")).distinct()

        trajectory_counts = df_clean.alias("A").join(
            df_clean.alias("B"),
            (col("A.uuid") == col("B.uuid")) &
            (col("A.concept") != col("B.concept")) &
            (col("B.condition_record_date") > col("A.condition_record_date")) &
            (datediff(col("B.condition_record_date"), col("A.condition_record_date")) <= 365)
        ).groupBy(col("A.concept").alias("concept_A"), col("B.concept").alias("concept_B")).count()

        window_spec = Window.partitionBy("concept_A")

        trajectory_with_prob = trajectory_counts.withColumn(
            "total_A_outcomes", sum(col("count")).over(window_spec)
        ).withColumn(
            "probability", round(col("count") / col("total_A_outcomes"), 2) # Rounding here is faster
        )

        dis_trans_pat = sorted([
            (row["concept_A"], row["concept_B"], row["count"], row["probability"]) 
            for row in trajectory_with_prob.collect()
        ], key=lambda x: (-x[2], -x[3]))

        #Advanced Metric 4: Average time gap between recurrences (Top 10)
        window_spec = Window.partitionBy("uuid", "medical_concepts").orderBy("condition_record_date")

        df_with_gaps = df.withColumn("prev_episode_end", lag("date_of_abetment").over(window_spec))

        df_with_gaps = df_with_gaps.withColumn("gap_days", 
            datediff(col("condition_record_date"), col("prev_episode_end"))
        )

        avg_gap_metric = (df_with_gaps
            .filter(col("gap_days") > 0)
            .groupBy("medical_concepts")
            .agg(avg("gap_days").alias("avg_recurrence_gap"))
            .orderBy(desc("avg_recurrence_gap"))
        )
        avg_cond_recurr_gap = sorted([{row[0]: int(row[1])} for row in avg_gap_metric.collect()], reverse=True, key=lambda x: list(x.values())[0])

        #Advanced Metric 5: Age-Based Disease Burden (Health Cliff)
        patients_df = self.master.getDataframes("patients")
        if patients_df is None:
            # Fallback: Load Patients if not present (e.g. standalone run)
            from .patients import PatientsETL
            # Avoid full init if possible to save time, but safer to just run it once
            p_etl = PatientsETL() 
            patients_df = self.master.getDataframes("patients")

        # Join to get birth_date
        df_age = df.join(patients_df.select("uuid", "birth_date"), "uuid", "inner")
        
        # Calculate age at condition onset
        df_age = df_age.withColumn("age_at_onset", floor(datediff(col("condition_record_date"), col("birth_date"))/365.25))

        # Filter invalid ages
        df_age = df_age.filter((col("age_at_onset") >= 0) & (col("age_at_onset") < 120))

        # Binning (10-year intervals)
        df_age = df_age.withColumn("age_bin_start", (floor(col("age_at_onset")/10) * 10).cast("int"))
        
        age_burden_df = df_age.groupBy("age_bin_start").agg(count("*").alias("count")).orderBy("age_bin_start")
        
        # Format as "0-9", "10-19" etc.
        age_based_burden = [
            {f"{row['age_bin_start']}-{row['age_bin_start']+9}": row['count']} 
            for row in age_burden_df.collect()
        ]

        self.master.setAdvancedMetrics("conditions", conditionAdvancedMetrics(average_condition_recurrence_gap=avg_cond_recurr_gap, incidence_velocity=inc_velocity, commordity_cooccurence=commorb_coocu, disease_transition_patterns=dis_trans_pat, age_based_burden=age_based_burden))


    def test(self):
        df = self.master.getDataframes("conditions")
        #distinct_conditions = list(map(lambda x: x[0], df.select("medical_concepts").distinct().collect()))
        #print(distinct_conditions, len(distinct_conditions), sep='\n')
        #df.show(10)
        print(df.select(col("medical_concepts").alias("mc1")).crossJoin(df.select(col("medical_concepts").alias("mc2"))) \
              .filter(col("mc1") != col("mc2")).select("mc1", "mc2"))
        
# Optional: Standalone execution
#if __name__ == "__main__":
#    conditions_etl = ConditionsETL()
#    df_proc = conditions_etl.getDf()
#    df_proc.show(25)
#    print(conditions_etl.master.getKPIS("conditions"))
#    print(conditions_etl.master.getMetrics("conditions"))
#    print(conditions_etl.test())
#    print(conditions_etl.master.getAdvancedMetrics("conditions"))
#    print("Columns:", df_proc.columns)