from kafka import KafkaProducer
import time
import json
import pytest

from app.ModelCity import serialize_datetime
from jobs.comsummer import create_spark_session, read_data_from_kafka, defin_schema_for_trajet, final_data_frame_trajet


@pytest.mark.integration
def test_spark_reads_from_kafka():
    # Envoyer un message test dans Kafka
    boostrap_server='localhost:9092'
    producer = KafkaProducer(bootstrap_servers=[boostrap_server],
                             value_serializer=lambda v: json.dumps(v, default=serialize_datetime).encode('utf-8'))
    test_msg = {"tripId":"TripABC123", "departure":{"name": "Domicile", "latitude": "46.851863", "longitude": "-71.207157"}, "arrival":{"name": "Bureau", "latitude": "46.797777", "longitude": "-71.263931"}, "heure_depart":"2025-01-01T10:00:05", "flag_arrivee": "False"}
    producer.send('vehicle_data', test_msg)
    producer.flush()
    time.sleep(5)  # attendre que Kafka reçoive le message

    spark = create_spark_session()
    df = read_data_from_kafka(spark, 'vehicle_data', boostrap_server)

    # Écrire le stream dans une mémoire temporaire
    query = (df.writeStream
             .format("memory")
             .queryName("test_kafka_stream")
             .outputMode("append")
             .start())

    time.sleep(5)  # attendre ingestion

    # Lire depuis la table mémoire
    df_result = spark.sql("SELECT * FROM test_kafka_stream")
    df_result.show()

    assert df.count() >= 1

    query.stop()
