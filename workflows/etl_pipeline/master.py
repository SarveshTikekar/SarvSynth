from pyspark.sql import SparkSession
from pyspark import StorageLevel

# -- We would be instantiating a SingleTon class here and that would be yielded to the API (Following System Design principles) -- 

class Master:

    _instance = None
    _master_spark = None
    def __new__(cls):
        if cls._instance is None:

            cls._instance = super().__new__(cls)
            cls._instance.__init_spark()
            cls._instance.dataframes = {}
        
        return cls._instance
    
    def __init_spark(self):
        self._master_spark = SparkSession.builder.appName("Hospital-ETL").master("local[2]").config("spark.driver.memory", "4g").config("spark.hadoop.fs.defaultFS", "file:///").getOrCreate()
    
    def __detach_spark(self):
        self._master_spark.stop()
        self._master_spark = None
    
    def getDataframes(self, key):
        return self.dataframes.get(key, {}).get("dataframe")
    
    def setDataframes(self, key, df):

        if key not in self.dataframes:
            self.dataframes[key] = {}

        self.dataframes[key]["dataframe"] = df.persist(StorageLevel.MEMORY_AND_DISK)

    def setKPIS(self, key, kpis):

        if key not in self.dataframes:
            self.dataframes[key] = {}

        self.dataframes[key]["kpis"] = kpis

    def getKPIS(self, key):
        return self.dataframes.get(key, {}).get("kpis")

    def setMetrics(self, key, metrics):
        if key not in self.dataframes:
            self.dataframes[key] = {}

        self.dataframes[key]["metrics"] = metrics

    def getMetrics(self, key):
         return self.dataframes.get(key, {}).get("metrics")

    def setAdvancedMetrics(self, key, ame):
        if key not in self.dataframes:
            self.dataframes[key] = {}

        self.dataframes[key]["advanced_metrics"] = ame
    
    def getAdvancedMetrics(self, key):
        return self.dataframes.get(key, {}).get("advanced_metrics")


       
