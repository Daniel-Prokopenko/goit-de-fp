import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType


def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', "", str(text))


def main():
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()
    clean_udf = udf(clean_text, StringType())

    for table in ["athlete_bio", "athlete_event_results"]:
        df = spark.read.parquet(f"/opt/airflow/data/bronze/{table}")

        for column in df.columns:
            if dict(df.dtypes)[column] == "string":
                df = df.withColumn(column, clean_udf(col(column)))

        df = df.dropDuplicates()
        df.write.mode("overwrite").parquet(f"/opt/airflow/data/silver/{table}")
        df.show()

    spark.stop()


if __name__ == "__main__":
    main()
