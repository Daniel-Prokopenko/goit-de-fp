from pyspark.sql import SparkSession
from configs import kafka_config

# # Налаштування конфігурації SQL бази даних
jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_table = "athlete_bio"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"

# # Створення Spark сесії
spark = (
    SparkSession.builder.config(
        "spark.jars",
        "mysql-connector-j-8.0.32.jar,kafka-clients-3.5.1.jar,spark-sql-kafka-0-10_2.12-3.5.5.jar",
    )
    .appName("JDBCToKafka")
    .getOrCreate()
)

# Читання даних з SQL бази даних
bio_df = (
    spark.read.format("jdbc")
    .options(
        url=jdbc_url,
        driver="com.mysql.cj.jdbc.Driver",  # com.mysql.jdbc.Driver
        dbtable=jdbc_table,
        user=jdbc_user,
        password=jdbc_password,
    )
    .load()
)

bio_df.show()

jdbc_table = "athlete_event_results"

events_df = (
    spark.read.format("jdbc")
    .options(
        url=jdbc_url,
        driver="com.mysql.cj.jdbc.Driver",  # com.mysql.jdbc.Driver
        dbtable=jdbc_table,
        user=jdbc_user,
        password=jdbc_password,
    )
    .load()
)

events_df = events_df.drop("country_noc")

events_df.show()

joined_df = bio_df.join(events_df, on="athlete_id", how="inner")

joined_df.show()

joined_df.createOrReplaceTempView("joined")

spark = SparkSession.builder.appName("danylo").getOrCreate()

result = spark.sql(
    """SELECT sport, medal, sex, country_noc, AVG(height) AS avg_height, AVG(weight) AS avg_weight, CURRENT_TIMESTAMP AS timestamp
                        FROM joined
                        WHERE height > 0 AND weight > 0
                        GROUP BY sport, medal, sex, country_noc
                        ORDER BY sport, medal, sex, country_noc"""
).show()