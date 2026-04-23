from pyspark.sql.functions import col, to_date, regexp_extract, initcap, count, sum, avg, isnull, isnotnull, transform
from pyspark.sql import Window
from etl_pipeline.master import Master
import os
from .models.allergies import allergyKPIS, allergyMetrics
class AllergiesETL:

    def __init__(self) -> None:
        self.master = Master()
        self.etl()
        self.calculateKPIS()
        self.calculateMetrics()

    def etl(self):
        """
        Load the transformed conditions DataFrame from CSV if not already loaded.
        """

        path=os.path.join(os.path.dirname(__file__), "..", "Datasets", "csv", "allergies.csv")
        df = self.master._master_spark.read.csv(path, header=True, inferSchema=True)

        new_cols = ['recorded_date', '_', 'uuid', 'encounter_id', 'causative_agent_id', 'code_system', 'causative_agent', 'agent_aim', 'agent_class', 'primary_reaction_id', 'primary_reaction', 'primary_reaction_severity', 'symptom_id', 'symptom_observed', 'symptom_severity']

        for old_col, new_col in zip(df.columns, new_cols):
            df = df.withColumnRenamed(old_col, new_col)

        # Drop placeholder columns
        df = df.drop(*[col for col in df.columns if col.startswith('_')])

        # Convert date columns to proper date format
        df = df.withColumn("recorded_date", to_date(col("recorded_date"), "yyyy-MM-dd"))

        #Capitalize first letter for all severities
        df = df.withColumn("primary_reaction_severity", initcap("primary_reaction_severity")) \
        .withColumn("symptom_severity", initcap("symptom_severity"))
        
        #Cast all reaction and symptom ids into int

        df = df.withColumn("primary_reaction_id", col("primary_reaction_id").cast("int")) \
        .withColumn("symptom_id", col("symptom_id").cast("int"))

        # Store in singleton
        self.master.setDataframes("allergies", df)
    
    def getDf(self):
        return self.master.getDataframes("allergies")
    
    def calculateKPIS(self):

        #KPI-1 total_allergic_population
        df = self.master.getDataframes("allergies")

        total_allergic_pop = df.select(col("uuid")).distinct().count()

        #KPI-2 Severe anaphylactic risk rate
        severe_anaphylatic_risk_rate = float((df.filter(col("primary_reaction_severity") == "Severe").count() / df.count()) * 100)

        #KPI-3 Drug hypersenitivity burden
        drug_hyp_burden = float((df.filter(col("code_system") == "RxNorm").count() / df.count()) * 100)

        #KPI-4 Poly allergen patient rate
        poly_allg_rate = float((df.groupBy("uuid").agg(
            count("*").alias("c1")
        ).filter(col("c1") >= 2).count() / df.select(col("uuid")).distinct().count() ) * 100)

        #KPI-5 Allergy risk stratification

        allg_rsk_strat = list(map(lambda x: (x[0], x[1], x[2]),
                                  df.filter((col("agent_class").isNotNull()) & (col("primary_reaction_severity").isNotNull())).groupBy("agent_class", "primary_reaction_severity").agg(

            count("*").alias("patient_count")
        ).rdd.map(tuple).collect()))

        self.master.setKPIS("allergies", allergyKPIS(total_allergic_population=total_allergic_pop, severe_anaphylactic_risk_rate=severe_anaphylatic_risk_rate, drug_hypersensitivity_rate= drug_hyp_burden, 
        poly_allergen_patient_rate=poly_allg_rate, allergy_risk_stratification=allg_rsk_strat))
        

    def calculateMetrics(self):

        df = self.master.getDataframes("allergies")

        #KPI-1 Top 10 causative agents
        top_10_causative_agents =  list(map(lambda x: {x[0]: x[1]}, df.groupBy("causative_agent").agg(

            count("*").alias("ca_count")
        ).orderBy(col("ca_count").desc()).limit(10).collect()))

        #KPI-2 Severity Distribution
        sever_distrib = list(map(lambda x: {x[0]: x[1]},df.filter(col("primary_reaction_severity").isNotNull()).groupBy("primary_reaction_severity").agg(count("*").alias("s_count")).collect()))

        #KPI-3  Allergen Distribution Breakdown
        allg_class_breakdown = list(map(lambda x: {x[0]: x[1]},df.filter(col("agent_class").isNotNull()).groupBy("agent_class").agg(count("*").alias("s_count")).collect()))

        self.master.setMetrics("allergies", allergyMetrics(top_10_causative_agents=top_10_causative_agents, severity_distribution=sever_distrib,allergy_discovery_trends=allg_class_breakdown))
        pass

# Optional: Standalone execution
if __name__ == "__main__":
    allergies_etl = AllergiesETL()
    df_proc = allergies_etl.getDf() 
    df_proc.show(15)
    print(f"Columns are:{df_proc.columns}")
    print(allergies_etl.master.getKPIS("allergies"))
    print(allergies_etl.master.getMetrics("allergies"))
