from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp, from_json
from pyspark.sql.types import StructType, StructField, StringType
from configs import kafka_config

jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_table = "olympic_dataset.athlete_bio"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"

spark = (
    SparkSession.builder.appName("JDBCToKafkaEnhanced")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "mysql:mysql-connector-java:8.0.32",
    )
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.streaming.backpressure.enabled", "true")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
# 1. Читаємо біографічні дані
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
# 2. Фільтруємо значення
bio_cleaned_df = bio_df.filter(
    (col("height").isNotNull())
    & (col("weight").isNotNull())
    & (col("height").cast("double").isNotNull())
    & (col("weight").cast("double").isNotNull())
)
# 3. Читаємо та записуємо результати змагань у Kafka
events_df = (
    spark.read.format("jdbc")
    .options(
        url="jdbc:mysql://217.61.57.46:3306/",
        driver="com.mysql.cj.jdbc.Driver",
        dbtable="olympic_dataset.athlete_event_results",
        user=jdbc_user,
        password=jdbc_password,
    )
    .load()
)

events_df.selectExpr(
    "CAST(athlete_id AS STRING) as key", "to_json(struct(*)) AS value"
).write.format("kafka").option(
    "kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0]
).option(
    "topic", "danylop_athlete_topic_input"
).option(
    "kafka.security.protocol", kafka_config["security_protocol"]
).option(
    "kafka.sasl.mechanism", kafka_config["sasl_mechanism"]
).option(
    "kafka.sasl.jaas.config",
    f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" '
    f'password="{kafka_config["password"]}";',
).save()


schema = StructType(
    [
        StructField("edition", StringType(), True),
        StructField("edition_id", StringType(), True),
        StructField("country_noc", StringType(), True),
        StructField("sport", StringType(), True),
        StructField("event", StringType(), True),
        StructField("result_id", StringType(), True),
        StructField("athlete", StringType(), True),
        StructField("athlete_id", StringType(), True),
        StructField("pos", StringType(), True),
        StructField("medal", StringType(), True),
        StructField("isTeamSport", StringType(), True),
    ]
)

kafka_stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
    .option("subscribe", "danylop_athlete_topic_input")
    .option("kafka.security.protocol", kafka_config["security_protocol"])
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
    .option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" '
        f'password="{kafka_config["password"]}";',
    )
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", "500")
    .load()
)

kafka_json_df = (
    kafka_stream_df.selectExpr("CAST(value AS STRING)")
    .select(from_json("value", schema).alias("data"))
    .select("data.*")
)

# 4. Об’єднуємо з біографічними даними
joined_stream_df = kafka_json_df.join(
    bio_cleaned_df, on="athlete_id", how="inner"
).drop(bio_cleaned_df["country_noc"])

# 5. Агрегуємо за видом спорту, медаллю, статтю та країною
aggregated_stream_df = joined_stream_df.groupBy(
    "sport", "medal", "sex", "country_noc"
).agg(
    avg("height").alias("avg_height"),
    avg("weight").alias("avg_weight"),
    current_timestamp().alias("timestamp"),
)


def foreach_batch_function(batch_df, batch_id):
    if batch_df.count() > 0:
        batch_df.selectExpr("to_json(struct(*)) AS value").write.format("kafka").option(
            "kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0]
        ).option("topic", "danylop_athlete_topic_output").option(
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
            dbtable="olympic_dataset.aggregated_results",
            user=jdbc_user,
            password=jdbc_password,
        ).mode("append").save()


# 6. Починаємо стрім
aggregated_stream_df.writeStream.foreachBatch(foreach_batch_function).outputMode(
    "update"
).trigger(processingTime="10 seconds").option(
    "checkpointLocation", "checkpoint/athlete_pipeline"
).start().awaitTermination()
