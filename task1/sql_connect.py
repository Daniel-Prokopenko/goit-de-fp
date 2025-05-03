from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, current_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType
from configs import kafka_config

jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_table = "athlete_bio"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"


spark = (
    SparkSession.builder.appName("JDBCToKafkaEnhanced")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "mysql:mysql-connector-java:8.0.32",
    )
    .getOrCreate()
)


bio_df = (
    spark.read.format("jdbc")
    .options(
        url=jdbc_url,
        driver="com.mysql.jdbc.Driver",
        dbtable=jdbc_table,
        user=jdbc_user,
        password=jdbc_password,
    )
    .load()
)

events_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0]) \
    .option("subscribe", "athlete_event_results") \
    .option("kafka.security.protocol", kafka_config["security_protocol"]) \
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"]) \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" '
            f'password="{kafka_config["password"]}";') \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", "500") \
    .load()

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

aggregated_stream_df = (
    joined_df.filter((col("height") > 0) & (col("weight") > 0))
    .groupBy("sport", "medal", "sex", "country_noc")
    .agg(avg("height").alias("avg_height"), avg("weight").alias("avg_weight"))
    .withColumn("timestamp", current_timestamp())
)


def foreach_batch_function(batch_df, batch_id):
    if batch_df.count() > 0:
        batch_df.selectExpr(
            "CAST(sport AS STRING) AS key", "to_json(struct(*)) AS value"
        ).write.format("kafka").option(
            "kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0]
        ).option(
            "topic", "danylo_athlete_stats"
        ).option(
            "kafka.security.protocol", kafka_config["security_protocol"]
        ).option(
            "kafka.sasl.mechanism", kafka_config["sasl_mechanism"]
        ).option(
            "kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" '
            f'password="{kafka_config["password"]}";',
        ).save()

        batch_df.write.format("jdbc").options(
            url=jdbc_url,
            driver="com.mysql.cj.jdbc.Driver",
            dbtable="olympic_dataset.aggregated_athlete_stats",
            user=jdbc_user,
            password=jdbc_password,
        ).mode("append").save()


aggregated_stream_df.writeStream.foreachBatch(foreach_batch_function).outputMode(
    "update"
).trigger(processingTime="10 seconds").option(
    "checkpointLocation", "/tmp/checkpoints/athlete_pipeline"
).start().awaitTermination()
