from pyspark.sql import SparkSession

from jobs.Stockage.mod_stockage import applatir_json_data, converted_dataframe
from jobs.comsummer import defin_schema_for_trajet, final_data_frame_trajet

conn = SparkSession \
            .builder \
            .appName("Streaming Data from IoT devices to influxDB using KAFKA and Spark") \
            .master("local[*]") \
            .getOrCreate()

def test_applatir_json():

    df = conn.read.json("test/Data/trajet.json")

    df_flat = applatir_json_data(df)

    assert "departure_name" in df_flat.columns
    assert "arrival_latitude" in df_flat.columns

def test_converted_dataframe():
    df = conn.read.json("test/Data/trajet.json")

    df_flat = applatir_json_data(df)

    df_converted = converted_dataframe(df_flat)

    assert df_converted.count() == 1

def test_trajet_schema():
    schema = defin_schema_for_trajet()

    df = conn.read.json("test/Data/trajet.json", schema=schema)

    assert df.count() == 1
    assert "tripId" in df.columns

# def test_final_data_frame():
#     spark = SparkSession.builder.master("local[*]").appName("Test").getOrCreate()
#
#     #data = [Row(value='{"immatriculation":"123ABC","vin":"VIN123","marque":"Toyota","année":2020,"color":"Red","fuelType":"Gas","IDTrajet":"T1","IDChauffeur":"C1","speed":60,"DateDebutTrajet":"2025-11-22T12:00:00"}')]
#     #df = spark.createDataFrame(data)
#
#     df = conn.read.json("test/Data/trajet.json")
#
#     schema = defin_schema_for_trajet()
#     final_df = final_data_frame_trajet(df, schema)
#
#     assert final_df.count() == 1
#     assert "immatriculation" in final_df.columns