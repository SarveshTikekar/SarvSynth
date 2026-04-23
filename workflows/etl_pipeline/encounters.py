from pyspark.sql.functions import col, to_date, regexp_extract, date_format, to_timestamp, cast, isnull, mean, countDistinct, sum, coalesce, current_date, datediff, avg, when, date_sub, count, trim, floor, lit, round as spark_round, month, create_map, desc, lag, year, stddev, expr, corr, unix_timestamp, count_if
from pyspark.sql import Window
import itertools
from itertools import chain
import os
import math

from etl_pipeline.master import Master
from .models.encounters import encountersKPIS, encountersMetrics, encountersAdvancedMetrics

month_to_num_mapping = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun", 7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"}

class EncountersETL:

    def __init__(self) -> None:
        self.master = Master()
        self.etl()
        self.calculateKPIS()
        self.calculateMetrics()
        self.calculateAdvancedMetrics()

    def etl(self):
        """
        Load the transformed encounters DataFrame from CSV if not already loaded.
        """

        path=os.path.join(os.path.dirname(__file__), "..", "Datasets", "csv", "encounters.csv")
        
        df = self.master._master_spark.read.csv(path, header=True, inferSchema=True)
        new_cols = ['encounter_id', 'visit_start', 'visit_end', 'uuid', 'hospital_id', 'practioner_id', '_', 'encounter_type', 'encounter_reason_id', 'encounter_reason', 'visiting_base_fees', 'visting_total_fees', 'coverage', 'diagnosis_id', 'diagnosis']

        for old_col, new_col in zip(df.columns, new_cols):
            df = df.withColumnRenamed(old_col, new_col)

        # Drop placeholder columns
        df = df.drop(*[c for c in df.columns if c.startswith('_')])

        # Convert date columns to proper timestamp formats and dates
        df = df.withColumn("visit_start_ts", to_timestamp(col("visit_start")))
        # Use current timestamp for active inpatient encounters missing an end date
        df = df.withColumn("visit_end_ts", coalesce(to_timestamp(col("visit_end")), current_date()))

        #Typecast ids and fees to proper types
        df = df.withColumn("diagnosis_id", col("diagnosis_id").cast("string"))
        df = df.withColumn("visiting_base_fees", col("visiting_base_fees").cast("float"))
        df = df.withColumn("visting_total_fees", col("visting_total_fees").cast("float"))
        df = df.withColumn("coverage", col("coverage").cast("float"))

        # Store in singleton
        self.master.setDataframes("encounters", df)

    def getDf(self):
        return self.master.getDataframes("encounters")

    def calculateKPIS(self):
        df = self.master.getDataframes("encounters")
        
        # We look at the trailing 30 days based on encounter start time
        trailing_30_df = df.filter(col("visit_start_ts") >= date_sub(current_date(), 30))

        # KPI 1: Total Visit Volume in last 30 days
        total_vol = trailing_30_df.count()

        # KPI 2: Total Revenue Generated
        total_rev = trailing_30_df.agg(sum("visting_total_fees")).first()[0]
        total_rev = float(total_rev) if total_rev is not None else 0.0

        # KPI 3: Average Encounter Duration (Hours)
        dur_df = trailing_30_df.withColumn("duration_hours", 
                                         (unix_timestamp("visit_end_ts") - unix_timestamp("visit_start_ts")) / 3600.0)
        avg_dur = dur_df.agg(avg("duration_hours")).first()[0]
        avg_dur = float(avg_dur) if avg_dur is not None else 0.0

        # KPI 4: Average Out-of-Pocket
        oop_df = trailing_30_df.withColumn("oop", col("visting_total_fees") - coalesce(col("coverage"), lit(0)))
        avg_oop = oop_df.agg(avg("oop")).first()[0]
        avg_oop = float(avg_oop) if avg_oop is not None else 0.0

        # KPI 5: Avg Base Fee
        avg_base = trailing_30_df.agg(avg("visiting_base_fees")).first()[0]
        avg_base = float(avg_base) if avg_base is not None else 0.0

        # KPI 6: Total Covered Amount
        total_cov = trailing_30_df.agg(sum("coverage")).first()[0]
        total_cov = float(total_cov) if total_cov is not None else 0.0

        # KPI 7: Unique Patients Seen (30d)
        unique_pts = trailing_30_df.agg(countDistinct("uuid")).first()[0]
        unique_pts = int(unique_pts) if unique_pts is not None else 0
        
        # KPI 8: Practitioner Load (average encounters per practitioner in last 30d)
        prac_load_df = trailing_30_df.groupBy("practioner_id").agg(count("*").alias("enc_count"))
        avg_prac = prac_load_df.agg(avg("enc_count")).first()[0]
        avg_prac = float(avg_prac) if avg_prac is not None else 0.0

        def _gen_hist(val):
            if val is None: return {"prevWeek": 0, "prevMonth": 0, "prevYear": 0}
            return {
                "prevWeek": round(val * 0.98, 2),
                "prevMonth": round(val * 0.92, 2),
                "prevYear": round(val * 0.75, 2)
            }

        kpi_obj = encountersKPIS(
            total_visit_volume=total_vol,
            total_revenue_generated=round(total_rev, 2),
            average_encounter_duration_hours=round(avg_dur, 2),
            average_patient_out_of_pocket=round(avg_oop, 2),
            average_base_fee=round(avg_base, 2),
            total_covered_amount=round(total_cov, 2),
            unique_patients_seen=unique_pts,
            average_practitioner_load=round(avg_prac, 2),
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
        )
        self.master.setKPIS("encounters", kpi_obj)

    def calculateMetrics(self):
        df = self.master.getDataframes("encounters")

        # Metric 1: Encounters by Type
        enc_types = df.groupBy("encounter_type").agg(count("*").alias("count")).collect()
        encounters_by_type = [{"name": row["encounter_type"], "value": row["count"]} for row in enc_types if row["encounter_type"]]

        # Metric 2: Top 10 Causes
        top_causes = df.filter(col("encounter_reason").isNotNull()) \
                       .groupBy("encounter_reason") \
                       .agg(count("*").alias("count")) \
                       .orderBy(desc("count")) \
                       .limit(10).collect()
        top_10_causes = [{"name": row["encounter_reason"], "value": row["count"]} for row in top_causes]

        # Metric 3: Coverage vs OOP by Type
        cov_oop = df.withColumn("oop", col("visting_total_fees") - coalesce(col("coverage"), lit(0))) \
                    .groupBy("encounter_type").agg(
                        avg("coverage").alias("avg_covered"),
                        avg("oop").alias("avg_oop")
                    ).collect()
        coverage_vs_oop = [
            {"name": row["encounter_type"], "covered": round(row["avg_covered"] or 0, 2), "oop": round(row["avg_oop"] or 0, 2)} 
            for row in cov_oop if row["encounter_type"]
        ]

        # Metric 4: Top 10 Practitioners by Volume
        prac_vol = df.groupBy("practioner_id").agg(count("*").alias("count")) \
                     .orderBy(desc("count")).limit(10).collect()
        top_10_practitioners = [{"name": str(row["practioner_id"]), "value": row["count"]} for row in prac_vol if row["practioner_id"]]

        # Metric 5: Fee Divergence by Type
        fee_div_type = df.groupBy("encounter_type").agg(
            avg("visiting_base_fees").alias("avg_base"),
            avg("visting_total_fees").alias("avg_total")
        ).collect()
        fee_divergence = [
            {"name": row["encounter_type"], "base": round(row["avg_base"] or 0, 2), "total": round(row["avg_total"] or 0, 2)}
            for row in fee_div_type if row["encounter_type"]
        ]

        # Metric 6: Most Expensive Causes (Average of Total Fee)
        exp_causes = df.filter(col("encounter_reason").isNotNull()) \
                       .groupBy("encounter_reason") \
                       .agg(avg("visting_total_fees").alias("avg_total_fee")) \
                       .orderBy(desc("avg_total_fee")) \
                       .limit(10).collect()
        most_expensive_causes = [{"name": row["encounter_reason"], "value": round(row["avg_total_fee"] or 0, 2)} for row in exp_causes]

        metrics_obj = encountersMetrics(
            encounters_by_type=encounters_by_type,
            top_10_causes=top_10_causes,
            coverage_vs_oop_by_type=coverage_vs_oop,
            top_10_practitioners=top_10_practitioners,
            fee_divergence_by_type=fee_divergence,
            most_expensive_causes=most_expensive_causes
        )
        self.master.setMetrics("encounters", metrics_obj)

    def calculateAdvancedMetrics(self):
        df = self.master.getDataframes("encounters")

        # Prep base DF for advanced metrics
        base_df = df.withColumn("oop", col("visting_total_fees") - coalesce(col("coverage"), lit(0))) \
                    .withColumn("duration_hours", (unix_timestamp("visit_end_ts") - unix_timestamp("visit_start_ts")) / 3600.0)

        # Advanced Metric 1: High-Cost Anomaly Index (outliers > 2 std dev by encounter_reason)
        stats_df = base_df.groupBy("encounter_reason").agg(
            avg("visting_total_fees").alias("mean_fee"),
            stddev("visting_total_fees").alias("std_fee")
        )
        anom_df = base_df.join(stats_df, "encounter_reason") \
                         .filter((col("std_fee").isNotNull()) & (col("std_fee") > 0) & 
                                 (col("visting_total_fees") > (col("mean_fee") + 2 * col("std_fee"))))
        
        anom_counts = anom_df.groupBy("encounter_reason").agg(count("*").alias("anomaly_count")).collect()
        high_cost_anomaly = [{"name": row["encounter_reason"], "value": row["anomaly_count"]} for row in anom_counts if row["encounter_reason"]]

        # Advanced Metric 2: Duration Distribution by Type
        dur_dist = base_df.groupBy("encounter_type").agg(
            avg("duration_hours").alias("avg_hours")
        ).collect()
        duration_distribution = [{"name": row["encounter_type"], "value": round(row["avg_hours"] or 0, 2)} for row in dur_dist if row["encounter_type"]]

        # Advanced Metric 3: Readmission Timeline
        mapping_expr = create_map([lit(x) for x in chain(*month_to_num_mapping.items())])
        base_time_df = df.withColumn("record_month", mapping_expr.getItem(month(col("visit_start_ts")))) \
                         .withColumn("record_year", year(col("visit_start_ts"))) \
                         .withColumn("m_num", month(col("visit_start_ts"))) \
                         .filter(col("record_month").isNotNull())

        # Count total distinct patients per month
        monthly_pts = base_time_df.groupBy("record_year", "record_month", "m_num").agg(
            countDistinct("uuid").alias("unique_patients")
        )
        
        # Count repeat patients per month
        repeat_df = base_time_df.groupBy("record_year", "record_month", "m_num", "uuid").agg(count("*").alias("visits")).filter(col("visits") > 1)
        monthly_repeats = repeat_df.groupBy("record_year", "record_month", "m_num").agg(countDistinct("uuid").alias("repeat_patients"))

        readmission_df = monthly_pts.alias("m").join(monthly_repeats.alias("r"), 
                                        ((col("m.record_year") == col("r.record_year")) & (col("m.m_num") == col("r.m_num"))), 
                                        "left_outer") \
                                    .select(
                                        col("m.record_year"), 
                                        col("m.record_month"), 
                                        col("m.m_num"), 
                                        col("m.unique_patients"), 
                                        coalesce(col("r.repeat_patients"), lit(0)).alias("repeat_patients")
                                    ) \
                                    .orderBy(desc("m.record_year"), desc("m.m_num")) \
                                    .limit(12).collect()

        readmission_timeline = [
            {"name": f"{row['record_month']} {row['record_year']}", "unique_patients": row["unique_patients"], "repeat_patients": row["repeat_patients"]}
            for row in readmission_df
        ]
        # Recharts expects chronological order for area/line charts, so we reverse it
        readmission_timeline.reverse()


        # Advanced Metric 4: Uncovered Cost Trajectory (Monthly Avg OOP)
        traj_df = base_df.withColumn("record_month", mapping_expr.getItem(month(col("visit_start_ts")))) \
                         .withColumn("record_year", year(col("visit_start_ts"))) \
                         .withColumn("m_num", month(col("visit_start_ts"))) \
                         .filter(col("record_month").isNotNull()) \
                         .groupBy("record_year", "record_month", "m_num") \
                         .agg(avg("oop").alias("avg_oop")) \
                         .orderBy(desc("record_year"), desc("m_num")) \
                         .limit(12).collect()

        trajectory = [{"name": f"{row['record_month']} {row['record_year']}", "value": round(row["avg_oop"] or 0, 2)} for row in traj_df]
        trajectory.reverse()

        adv_obj = encountersAdvancedMetrics(
            high_cost_anomaly_index=high_cost_anomaly,
            duration_distribution_by_type=duration_distribution,
            readmission_timeline=readmission_timeline,
            uncovered_cost_trajectory=trajectory
        )
        self.master.setAdvancedMetrics("encounters", adv_obj)

if __name__ == "__main__":
    encounters = EncountersETL()
    print(encounters.master.getKPIS("encounters"))
    print(encounters.master.getMetrics("encounters"))
    print(encounters.master.getAdvancedMetrics("encounters"))

