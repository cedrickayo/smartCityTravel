from pyspark.sql import SparkSession

from jobs.comsummer import defin_schema_for_trajet

spark = SparkSession \
            .builder \
            .appName("Streaming Data from IoT devices to influxDB using KAFKA and Spark") \
            .master("local[*]") \
            .getOrCreate()

def test_trajet_schema():
    schema = defin_schema_for_trajet()

    df = spark.read.json("test/Data/trajet.json", schema=schema)

    assert df.count() == 1
    assert "tripId" in df.columns