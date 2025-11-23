from unittest.mock import patch

from pyspark.sql import SparkSession

from jobs.comsummer import read_data_from_kafka

conn = SparkSession \
            .builder \
            .appName("Streaming Data from IoT devices to influxDB using KAFKA and Spark") \
            .master("local[*]") \
            .getOrCreate()

@patch("comsummer.SparkSession.readStream")
def test_kafka_mock(mock_readstream):
    #simulation de lecture du flux streaming depuis kafka
    #Au lieu de lire les données depuis kafka, spark va lire le fichier static local grace à la méthode patch

    mock_readstream.return_value = conn.read.json("test/Data/trajet.json")

    df=read_data_from_kafka(conn,"trajet")

    #verification que le dataframe contient exactement 1 ligne comme dans le fichier trajet.json
    assert df.count()== 1
