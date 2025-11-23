from unittest.mock import patch

from pyspark.sql import SparkSession

from jobs.Stockage.mod_stockage import applatir_json_data, converted_dataframe
from jobs.comsummer import write_to_db, write_data_into_s3, defin_schema_for_trajet

conn = SparkSession \
            .builder \
            .appName("Streaming Data from IoT devices to influxDB using KAFKA and Spark") \
            .master("local[*]") \
            .getOrCreate()



@patch("pyspark.sql.streaming.DataStreamWriter.start")
def test_write_stream_to_s3(mock_start):

    schema = defin_schema_for_trajet()

    fake_df = conn.read.json("test/Data/trajet.json", schema=schema)

    mock_start.return_value = fake_df

    df_flat = applatir_json_data(mock_start.return_value)

    df_converted = converted_dataframe(df_flat)

    query = write_to_db(df_converted, "check")

    # Assert
    mock_start.assert_called_once()


