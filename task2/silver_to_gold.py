from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp


def main():
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

    bio = (
        spark.read.parquet("/opt/airflow/data/silver/athlete_bio")
        .withColumn("height", col("height").cast("float"))
        .withColumn("weight", col("weight").cast("float"))
        .drop("country_noc")
    )

    results = spark.read.parquet("/opt/airflow/data/silver/athlete_event_results")

    df = results.join(bio, on="athlete_id", how="inner")

    agg_df = (
        df.groupBy("sport", "medal", "sex", "country_noc")
        .agg(avg("weight").alias("avg_weight"), avg("height").alias("avg_height"))
        .withColumn("timestamp", current_timestamp())
    )

    agg_df.write.mode("overwrite").parquet("/opt/airflow/data/gold/avg_stats")
    agg_df.show()
    spark.stop()


if __name__ == "__main__":
    main()
