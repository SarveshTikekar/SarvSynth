# conditions_etl.py
from pyspark.sql.functions import col, to_date, regexp_extract, isnull, mean, countDistinct, sum, coalesce, current_date, datediff, avg, when, date_sub, count, trim, floor, startswith, expr
from pyspark.sql.types import IntegerType
from etl_pipeline.master import Master
import os
import math

class MedicationsETL:

    def __init__(self) -> None:
        self.master = Master()
        self.etl()
        #self.calculateKPIS()
        #self.calculateMetrics()

    def etl(self):
        """
        Load the transformed conditions DataFrame from CSV if not already loaded.
        """

        path=os.path.join(os.path.dirname(__file__), "..", "Datasets", "csv", "medications.csv")
        
        df = self.master._master_spark.read.csv(path, header=True, inferSchema=True)
        new_cols = ['medication_start', 'medication_end', 'uuid', '_', 'encounter_id','rxnorm_medication_id', 'medication', 'medication_cost', 'amount_covered_under_claim', 'quantity', 'net_cost', 'condition_id', 'condition_treated']

        for old_col, new_col in zip(df.columns, new_cols):
            df = df.withColumnRenamed(old_col, new_col)

        # Drop placeholder columns
        df = df.drop(*[col for col in df.columns if col.startswith('_')])
        
        #Normalize condition ids to integers
        df = df.withColumn("condition_id", expr("try_cast(condition_id as INT)"))
        # Store in singleton
        self.master.setDataframes("medications", df)   

# Optional: Standalone execution
if __name__ == "__main__":
    medications_etl = MedicationsETL()
    print(medications_etl.master.getDataframes("medications").show())
