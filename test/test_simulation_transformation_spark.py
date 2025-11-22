from pyspark.sql import SparkSession

from jobs.Stockage.mod_stockage import applatir_json_data, converted_dataframe

conn = SparkSession \
            .builder \
            .appName("Streaming Data from IoT devices to influxDB using KAFKA and Spark") \
            .master("local[*]") \
            .getOrCreate()

def test_applatir_json():

    df = conn.read.json("tests/Data/trajet.json")

    df_flat = applatir_json_data(df)

    assert "departure_name" in df_flat.columns
    assert "arrival_latitude" in df_flat.columns

def test_converted_dataframe():
    df = conn.read.json("tests/Data/trajet.json")

    df_flat = applatir_json_data(df)

    df_converted = converted_dataframe(df_flat)

    assert df_converted.count() == 1