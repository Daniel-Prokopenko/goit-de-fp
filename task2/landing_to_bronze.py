import requests
from pyspark.sql import SparkSession


def download_data(local_file_path):
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + local_file_path + ".csv"
    print(f"Downloading from {downloading_url}")
    response = requests.get(downloading_url)
    if response.status_code == 200:
        with open(f"{local_file_path}.csv", "wb") as file:
            file.write(response.content)
        print(f"File saved as {local_file_path}.csv")
    else:
        exit(f"Failed to download {local_file_path}. Status: {response.status_code}")


def main():
    spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()

    for table in ["athlete_bio", "athlete_event_results"]:
        download_data(table)
        df = spark.read.option("header", True).csv(f"{table}.csv")
        df.write.mode("overwrite").parquet(f"/opt/airflow/data/bronze/{table}")
        df.show()

    spark.stop()


if __name__ == "__main__":
    main()
