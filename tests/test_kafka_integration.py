from kafka import KafkaProducer
import time
import json
import pytest

from app.ModelCity import serialize_datetime
from jobs.comsummer import create_spark_session, read_data_from_kafka, defin_schema_for_trajet, final_data_frame_trajet


@pytest.mark.integration
def test_spark_reads_from_kafka():
    # Envoyer un message test dans Kafka

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda v: json.dumps(v, default=serialize_datetime).encode('utf-8'))
    test_msg = {"tripId":"TripABC123", "departure":{"name": "Domicile", "latitude": "46.851863", "longitude": "-71.207157"}, "arrival":{"name": "Bureau", "latitude": "46.797777", "longitude": "-71.263931"}, "heure_depart":"2025-01-01T10:00:05", "flag_arrivee": "False"}
    producer.send('vehicle_data1', test_msg)
    producer.flush()
    time.sleep(5)  # attendre que Kafka reçoive le message

    spark = create_spark_session()
    df = read_data_from_kafka(spark, 'vehicle_data1')
    schema = defin_schema_for_trajet()
    final_df = final_data_frame_trajet(df, schema)
    assert final_df.count() == 1
