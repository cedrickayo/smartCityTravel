# from unittest.mock import patch
#
# from pyspark.sql import SparkSession
#
#
# from jobs.comsummer import write_to_db, write_data_into_s3, defin_schema_for_trajet
#
# conn = SparkSession \
#             .builder \
#             .appName("Streaming Data from IoT devices to influxDB using KAFKA and Spark") \
#             .master("local[*]") \
#             .getOrCreate()
#
# @patch("consumer.SparkSession.writeStream")
# def test_kafka_mock(mock_readstream):
#     #simulation de lecture du flux streaming depuis kafka
#     #Au lieu de lire les données depuis kafka, spark va lire le fichier static local grace à la méthode patch
#
#     mock_readstream.return_value = conn.read.json("tests/Data/trajet.json")
#
#     df=read_data_from_kafka(conn,"trajet")
#
#     #verification que le dataframe contient exactement 1 ligne comme dans le fichier trajet.json
#     assert df.count()== 1
#
#
#
# @patch("pyspark.sql.streaming.DataStreamWriter.start")
# def test_write_stream_to_s3(mock_start):
#
#     schema = defin_schema_for_trajet()
#
#     mock_start.return_value = conn.read.json("tests/Data/trajet.json", schema=schema)
#
#     df_flat = applatir_json_data(df)
#
#     df_converted = converted_dataframe(df_flat)
#
#
# @patch("pyspark.sql.streaming.DataStreamWriter.start")
#     def test_write_stream_to_s3(self, mock_start):
#         """
#         Test that write_to_s3 calls writeStream with correct options
#         without actually writing to S3.
#         """
#         # Arrange: Create a dummy streaming DataFrame
#         schema = "name STRING, age INT"
#         df = self.spark.readStream.schema(schema).json("in-memory-path")
#
#         # Mock start() to return a fake query object
#         fake_query = MagicMock()
#         mock_start.return_value = fake_query
#
#         # Act
#         bucket_path = "s3://my-bucket/data"
#         query = write_to_s3(df, bucket_path)
#
#         # Assert
#         self.assertEqual(query, fake_query)
#         mock_start.assert_called_once()  # Ensure start() was called
#         # Optionally check that checkpointLocation and path were set
#         writer = df.writeStream.format("parquet").option(
#             "checkpointLocation", f"{bucket_path}/_checkpoints"
#         ).option("path", bucket_path).outputMode("append")
#         self.assertIsNotNone(writer)  # Just to show we can inspect writer
