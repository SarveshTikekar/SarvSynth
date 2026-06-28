import os
from os.path import sep

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, length, regexp_extract, to_date, when

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"


def main():
    spark = (
        SparkSession.builder.appName("allergies-preview")
        .master("local[*]")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .getOrCreate()
    )

    path = os.path.join(os.getcwd(), "..", "Datasets", "csv", "allergies.csv")
    if not os.path.exists(path):
        print(f"File not found: {path}")
        return

    df = spark.read.csv(path, header=True, inferSchema=True)

    new_cols = [
        "allergy_detection_date",
        "allergy_cure_date",
        "uuid",
        "encounter_id",
        "allergy_code",
        "coding_system",
        "allergy_description",
        "allergy_type",
        "category",
        "primary_symptom_code",
        "primary_symptom_description",
        "primary_symptom_severity",
        "secondary_symptom_code",
        "secondary_symptom_description",
        "secondary_symptom_severity",
    ]

    for old_col, new_col in zip(df.columns, new_cols):
        df = df.withColumnRenamed(old_col, new_col)

    df = df.drop("_")

    df = df.withColumn(
        "allergy_detection_date", to_date(col("allergy_detection_date"))
    ).withColumn("allergy_cure_date", coalesce(col("allergy_cure_date")))

    # Spliting the allergy description into description and allergen nature

    df = (
        df.withColumn("allergy_description_raw", col("allergy_description"))
        .withColumn(
            "allergy_description",
            regexp_extract(col("allergy_description_raw"), r"([^(]+)\(", 1),
        )
        .withColumn(
            "allergen_nature",
            regexp_extract(col("allergy_description_raw"), r"\(([^)]+)\)", 1),
        )
        .drop("allergy_description_raw")
        .withColumn(
            "allergen_nature",
            when(length(col("allergen_nature")) == 0, "drug").otherwise(
                col("allergen_nature")
            ),
        )
    )

    df = (
        df.withColumn(
            "primary_symptom_description_raw", col("primary_symptom_description")
        )
        .withColumn(
            "secondary_symptom_description_raw", col("secondary_symptom_description")
        )
        .withColumn(
            "primary_symptom_description",
            regexp_extract(col("primary_symptom_description_raw"), r"([^(]+)\(", 1),
        )
        .withColumn(
            "primary_symptom_nature",
            regexp_extract(col("primary_symptom_description_raw"), r"\(([^)]+)\)", 1),
        )
        .withColumn(
            "secondary_symptom_description",
            regexp_extract(col("secondary_symptom_description_raw"), r"([^(]+)\(", 1),
        )
        .withColumn(
            "secondary_symptom_nature",
            regexp_extract(col("secondary_symptom_description_raw"), r"\(([^)]+)\)", 1),
        )
        .drop("secondary_symptom_description_raw")
        .drop("primary_symptom_description_raw")
        .withColumn(
            "allergen_nature",
            when(length(col("allergen_nature")) == 0, "drug").otherwise(
                col("allergen_nature")
            ),
        )
    )

    # df = df.withColumn(
    #     "primary_symptom_code", col("primary_symptom_code").cast("int")
    # ).withColumn("secondary_symptom_code", col("secondary_symptom_code").cast("int"))

    list_ = [
        "primary_symptom_severity",
        "primary_symptom_description",
        "primary_symptom_nature",
        "secondary_symptom_severity",
        "secondary_symptom_description",
        "secondary_symptom_nature",
    ]

    dict_ = {
        "primary_symptom_code": -1,
        "secondary_symptom_code": -1,
        **{x: "N/A" for x in list_},
    }
    df = df.fillna(dict_)

    # df = df.withColumn("allergy_record_uuid", uuid7())

    print(df.select("allergen_nature").show())
    print(df.select("primary_symptom_nature").show())
    print(df.select("secondary_symptom_nature").show())

    df.show(10, truncate=False)

    col_list = df.columns
    print(
        *col_list,
        "/n",
        sep=", ",
    )

    print(
        type(df["allergy_description"]), " and ", type(df.select("allergy_description"))
    )

if __name__ == "__main__":
    main()