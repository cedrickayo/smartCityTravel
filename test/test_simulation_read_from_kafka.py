from unittest.mock import patch

from pyspark.sql import SparkSession

from jobs.comsummer import read_data_from_kafka

conn = SparkSession \
            .builder \
            .appName("Streaming Data from IoT devices to influxDB using KAFKA and Spark") \
            .master("local[*]") \
            .getOrCreate()

@patch("pyspark.sql.streaming.DataStreamReader.load")
def test_kafka_mock(mock_load):
    #simulation de lecture du flux streaming depuis kafka
    #Au lieu de lire les données depuis kafka, spark va lire le fichier static local grace à la méthode patch

    fake_df = conn.read.json("test/Data/trajet.json")

    mock_load.return_value=fake_df

    df=read_data_from_kafka(conn,"trajet",boostrap_server='kafka:9092')

    #verification que le dataframe contient exactement 1 ligne comme dans le fichier trajet.json
    assert df.count() == 1
