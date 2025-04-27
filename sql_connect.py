from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, current_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType
from configs import kafka_config

jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_table = "athlete_bio"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"


spark = (
    SparkSession.builder.appName("JDBCToKafka")
    .config(
        "spark.jars",
        "mysql-connector-j-8.0.32.jar,spark-sql-kafka-0-10_2.12-3.5.5.jar,kafka-clients-3.5.1.jar",
    )
    .getOrCreate()
)

bio_df = (
    spark.read.format("jdbc")
    .options(
        url=jdbc_url,
        driver="com.mysql.cj.jdbc.Driver",
        dbtable=jdbc_table,
        user=jdbc_user,
        password=jdbc_password,
    )
    .load()
)

events_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"])
    .option("subscribe", "athlete_event_results")
    .option("startingOffsets", "latest")
    .load()
)


schema = (
    StructType()
    .add("athlete_id", IntegerType())
    .add("edition", StringType())
    .add("sport", StringType())
    .add("event", StringType())
    .add("medal", StringType())
)

parsed_events = events_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")


joined_df = parsed_events.join(bio_df, on="athlete_id", how="inner")

aggregated = (
    joined_df.filter((col("height") > 0) & (col("weight") > 0))
    .groupBy("sport", "medal", "sex", "country_noc")
    .agg(avg("height").alias("avg_height"), avg("weight").alias("avg_weight"))
    .withColumn("timestamp", current_timestamp())
)


def process_batch(df, epoch_id):
    if df.isEmpty():
        return

    df.persist()

    df.selectExpr(
        "CAST(sport AS STRING) AS key", "to_json(struct(*)) AS value"
    ).write.format("kafka").option(
        "kafka.bootstrap.servers", kafka_config["bootstrap_servers"]
    ).option(
        "topic", "danylo_athlete_stats"
    ).save()

    df.write.format("jdbc").option("url", jdbc_url).option(
        "dbtable", "aggregated_athlete_stats"
    ).option("user", jdbc_user).option("password", jdbc_password).option(
        "driver", "com.mysql.cj.jdbc.Driver"
    ).mode(
        "append"
    ).save()

    df.unpersist()


aggregated.writeStream.foreachBatch(process_batch).outputMode("update").option(
    "checkpointLocation", "/tmp/checkpoints/join_stream"
).start().awaitTermination()
