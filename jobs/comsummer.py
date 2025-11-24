# cette partie consistera à se connecter sur les diiferntes topics sur kafka via spark, lire les donner de kafka les stocker dans un format parquet
#pour ensuite etre envoyé dans un bucket s3, ou encore les stocker dans une BD noSQL tel que influxDB, Elastisearch ou prometheurs,
# ensuite Grafana ou/et kibana  pour la visualisation des données temps reels
import concurrent.futures
import logging
import os

from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
from pyspark.sql.functions import decode, col, from_json, struct
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, BooleanType, DateType, FloatType, \
    DoubleType, TimestampType
import sys

from jobs.Stockage.mod_stockage import applatir_json_data, write_to_influxdb, converted_dataframe

sys.path.append("/opt/bitnami/spark/jobs")
#from storage.mod_stockage import write_to_elastidsearch

# Create the index with the mapping
es = Elasticsearch("http://elasticsearch:9200")

def create_spark_session():
    conn = None
    try:
        conn = SparkSession \
            .builder \
            .appName("Streaming Data from IoT devices to influxDB using KAFKA and Spark") \
            .master("local[*]") \
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,org.elasticsearch:elasticsearch-spark-30_2.12:9.0.0,com.github.influxdata:spark-influxdb_2.1:0.3.0,org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-s3:1.12.661,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2") \
            .config("spark.jars", "postgresql-42.6.0.jar") \
            .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.cassandra.connection.host", "cassandra") \
            .config("spark.cassandra.connection.port", "9042") \
            .config("spark.es.nodes", "elasticsearch") \
            .config("spark.es.port", "9200") \
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY")) \
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_KEY")) \
            .config("spark.hadoop.fs.s3a.session.token", os.getenv("AWS_TOKEN_KEY")) \
            .getOrCreate()

        logging.info("created spark session successfully")

    except Exception as e:
        print(f"Impossible de créer une session Spark {e}")
        logging.error(f"failed to create spark session {e}")
    return conn

def read_data_from_kafka(spark:SparkSession, topic):
    topics=topic
    boostrap_server='localhost:9092'
    try:
        df = (spark
              .readStream
              .format('kafka')
              .option('kafka.bootstrap.servers', boostrap_server)
              .option('subscribe', topics)
              .option('kafka.startingOffsets', 'earliest')  # earliest, latest
              .option('kafka.request.timeout.ms', 3000)
              .option('kafka.metadata.fetch.timeout.ms', 3000)
              .load())
        logging.info("Succesfully connect from spark to kafka broker")

        df.printSchema()
        print(df.isStreaming)

    except Exception as e:
        logging.exception(str(e))
        raise e

    return df

def write_data_into_s3(check):
    def write_data(df, batchId):
        print("Batch id: " + str(batchId))

        # write to parquet
        # .save('/opt/bitnami/spark/parquet/user_data.parquet')
        #.option('fs.s3a.committer.name', 'partitioned')
        #.partitionBy("column_name")
        print("Writing to s3 in parquet format .....")
        try:
            (df.write
             .format("parquet")
             .mode("append")
             .save(f"s3a://spark-streaming-bucket-cedric/raw_data/{check}")
             )
            logging.info("successfully write in parquet format")
        except Exception as e:
            logging.error(f"impossible de se connecter au bucket s3 à cause de {e}")


        #print("Writing to Elasticsearch .....")
        # write_to_elastidsearch(df)



        # #write to postgres DB
        # print("Writing to postgres DB .....")
        # try:
        #     (df.write
        #      .mode("append")
        #      .format("jdbc")
        #      .option("url", "jdbc:postgresql://postgres:5432/postgres")
        #      .option("driver","org.postgresql.Driver")
        #      .option("dbtable","test_keyspace.users")
        #      .option("user", "airflow")
        #      .option("password", "airflow")
        #      .save()
        #      )
        # except Exception as e:
        #     logging.error(f"impossible d'ecrire dans PostgresSQL database à cause de : {e}")
        #
        # #write to cassandra
        # print("Writing to cassandra DB .....")
        #
        # try:
        #
        #     (df.write
        #      .mode("append")
        #      .format("org.apache.spark.sql.cassandra")
        #      .option("user", "admin")
        #      .option("password", "admin")
        #      .options(table="users", keyspace="test_keyspace")
        #      .save()
        #      )
        #     print("data has been writed into cassandra")
        # except Exception as e:
        #     logging.error(f"impossible d'ecrire dans cassandra à cause de : {e}")

        # write to csv file
        # df.write.mode("append").csv("user_output")
        # df.write.format("csv").mode("complete").save("user_output1")
    return write_data


def defin_schema_for_vehicule():
    schema = StructType([
        StructField("immatriculation", StringType(), False),
        StructField("vin", StringType(), False),
        StructField("marque", StringType(), False),
        StructField("année", IntegerType(), True),
        StructField("color", StringType(), True),
        StructField("fuelType", StringType(), True),
        StructField("IDTrajet", StringType(), True),
        StructField("IDChauffeur", StringType(), True),
        StructField("speed", FloatType(), False),
        StructField("DateDebutTrajet", TimestampType(), True),

    ])

    return schema

def gps_coordonate_schema():

    schema = StructType([
        StructField("name", StringType(), False),
        StructField("latitude", StringType(), False),
        StructField("longitude", StringType(), False),
    ])

    return schema

def defin_schema_for_trajet():

    schema = StructType([
        StructField("tripId", StringType(), False),
        StructField("departure", gps_coordonate_schema(), False),
        StructField("arrival", gps_coordonate_schema(), False),
        StructField("heure_depart", TimestampType(), True),
        StructField("flag_arrivee", BooleanType(), True),

    ])
    return schema


def defin_schema_for_emergency():
    schema = StructType([
        StructField("EmergencyTime", TimestampType(), False),
        StructField("evehicleID", StringType(), False),
        StructField("emergencyTrip", StringType(), False),
        StructField("emergencyType", StringType(), True),
        StructField("emergencyStatus", StringType(), True),
    ])
    return schema

def defin_schema_for_weather():

    schema = StructType([
        StructField("WeatherTime", TimestampType(), False),
        StructField("wvehicleID", StringType(), False),
        StructField("weatherTrip", StringType(), False),
        StructField("weatherCondition", StringType(), True),
        StructField("temperature", IntegerType(), True),
        StructField("wind_speed", IntegerType(), True),
        StructField("wind_dir", StringType(), True),
        StructField("wind_degree", IntegerType(), True),
        StructField("humidity", IntegerType(), False),

    ])

    return schema

def final_data_frame(dataframe, schema):

    decode_df = dataframe.withColumn("decoded_value", decode(col("value"), "UTF-8"))
    print("--------------------------------1----------------------------------------")
    decode_df.printSchema()

    decode_df = decode_df.selectExpr("decoded_value")
    #print(type(decode_df))
    print("-------------------------------2-------------------------------------------")
    decode_df.printSchema()
    streaming_df = decode_df.select(from_json(decode_df.decoded_value, schema).alias("data"))
    #streaming_df.printSchema()
    df_final = streaming_df.select("data.*")
    print("---------------------------------3-------------------------------------------")
    df_final.printSchema()

    return df_final


def final_data_frame_trajet(dataframe, schema):

    decode_df = dataframe.withColumn("decoded_value", decode(col("value"), "UTF-8"))
    print("--------------------------------1----------------------------------------")
    decode_df.printSchema()

    decode_df = decode_df.selectExpr("decoded_value")
    #print(type(decode_df))
    print("-------------------------------2-------------------------------------------")
    decode_df.printSchema()
    streaming_df = decode_df.select(from_json(decode_df.decoded_value, schema).alias("data"))
    #streaming_df.printSchema()
    df_final = streaming_df.select("data.*")
    print("---------------------------------3-------------------------------------------")
    df_final.printSchema()

    df_flat = applatir_json_data(df_final)

    print("---------------------------------4-------------------------------------------")
    df_flat.printSchema()

    df_converted = converted_dataframe(df_flat)
    print("---------------------------------5-------------------------------------------")

    df_converted.printSchema()

    return df_converted

def write_data_console(dataframe):
    #Affichage en console
    print("Ecriture dans la console")
    query=(dataframe.writeStream
           .outputMode("append")
           .format("console")
           .option("truncate", "false")
           .option("checkpointLocation", "checkpoint_dir_kafka")
           .start())
    query.awaitTermination()
# .trigger(once=True) \
# .option("truncate", "false") \


def write_to_db(joined_dataframe, check):
    #write_to_s3 = write_data_into_s3(check)
    try:
        query = joined_dataframe.writeStream \
            .foreachBatch(write_data_into_s3(check)) \
            .option("checkpointLocation", f"s3a://spark-streaming-bucket-cedric/checkpointLocation/{check}") \
            .start()
        logging.info(f"✅ Stream {check} started and writing to S3.")
        query.awaitTermination()
    except Exception as e:
        logging.error(f"❌ Stream {check} failed: {e}")


def write_data_into_iceberg_table(checkpoints, tab):
    def write_data(df, batchId):
        print("Batch id: " + str(batchId))

        print(f"Writing to s3 into iceberg {tab} open table format .....")
        try:
            (df.write
             .format("iceberg")
             .mode("append")
             .saveAsTable(f"s3a://spark-streaming-bucket-cedric/iceberg_table/{tab}")
             )
            logging.info(f"successfully write in {tab} iceberg table format")
        except Exception as e:
            logging.error(f"impossible de se connecter au bucket s3 à cause de {e}")

    return write_data


def write_to_iceberg(joined_dataframe, checkpoints, tab):
    #write_to_s3 = write_data_into_s3(check)
    try:
        query = joined_dataframe.writeStream \
            .foreachBatch(write_data_into_iceberg_table(checkpoints,tab)) \
            .option("checkpointLocation", f"s3a://spark-streaming-bucket-cedric/checkpointLocation/iceberg/{checkpoints}") \
            .start()
        logging.info(f"✅ Stream {checkpoints} started and writing to iceberg table {tab} .")
        query.awaitTermination()
    except Exception as e:
        logging.error(f"❌ Stream {checkpoints} failed: {e}")

def delete_duplicate_columns(df):
    from collections import Counter
    col_counts = Counter(df.columns)
    duplicate_cols = [col for col, count in col_counts.items() if count > 1]

    # Supprimer les doublons sauf le premier
    cols_to_keep = []
    seen = set()
    for cols in df.columns:
        if cols not in seen:
            cols_to_keep.append(cols)
            seen.add(cols)

    df_unique = df.select(*cols_to_keep)

    return df_unique


def transform_sparkdatatype_to_esdatatype(spark_datatype, spark_fieldname):
    champ = ["tripId", "immatriculation", "vin", "vehicleID", "IDChauffeur", "emergencyStatus", "wind_dir"]

    if isinstance(spark_datatype,StringType):
        if spark_fieldname in champ:
            return { "type": "keyword" }
        else:
            return { "type": "text" }
    elif isinstance(spark_datatype,BooleanType):
        return {"type": "boolean"}
    elif isinstance(spark_datatype, IntegerType):
        return {"type": "integer"}
    elif isinstance(spark_datatype, DateType):
        return {"type": "date"}
    elif isinstance(spark_datatype, FloatType):
        return {"type": "float"}
    elif isinstance(spark_datatype, StructType):
        champ=["departure_location","arrival_location"]
        if spark_fieldname in champ:
            return {"type": "geo_point"}
        else:
            return {"properties": {
                field.name: transform_sparkdatatype_to_esdatatype(field.dataType, field.name)
                for field in spark_datatype.fields
            }}
    elif isinstance(spark_datatype, DoubleType):
        return {"type": "float"}
    elif isinstance(spark_datatype, TimestampType):
        return {"type": "date"}

def generate_mapping_es_from_df(df):
    maps = {
        "mappings": {
            "properties": {}
        }
    }

    for field in df.schema.fields:
        print(field.name)
        maps['mappings']['properties'][field.name] = transform_sparkdatatype_to_esdatatype(field.dataType, field.name)

    return maps

# def write_to_elastidsearch(df):
#     if not es.indices.exists:
#         es.indices.create(index="vehicule_trajets", body=generate_mapping_es_from_df(df))
#     try:
#         (df.write.
#          format("org.elasticsearch.spark.sql").
#          mode("append")
#          .option("es.nodes", "elasticsearch")
#          .option("es.port", "9200")
#          #.option("es.nodes.discovery", "false")
#          #.option("es.nodes.wan.only", "true") # Use this if connecting to a remote cluster
#          #.option("es.index.auto.create", "true")
#          .option("es.mapping.id", "tripId") # Map the 'tripId' field as the document ID in Elasticsearch
#          #.option("es.mapping.exclude", "id")
#          .option("es.resource", "vehicule_trajets")
#          .save()
#
#          )
#         logging.info("successfully write in elasticsearch")
#     except Exception as e:
#         logging.error(f"impossible d'écrire dans la base elasticsearch à cause de {e}")
#         print(f"impossible de stocker dans la base elasticsearch à cause de {e}")


def main():
    #liste des topics
    #trajet_data,weather_data,emergency_data,vehicle_data
    topic_car='vehicle_data1'
    topic_trajet='trajet_data1'
    topic_weather='weather_data1'
    topic_emergency='emergency_data1'
    try:
    #Création d'une session Spark
        spark = create_spark_session()


        if spark==None:
            print("PAs correct")
        else:
            print("c'est correct")
            #lecture des topics trajet
            df_trajet=read_data_from_kafka(spark,topic_trajet)

            df_cars=read_data_from_kafka(spark,topic_car)

            df_emergency=read_data_from_kafka(spark, topic_emergency)

            df_weather=read_data_from_kafka(spark, topic_weather)

            #assocition des data au schema
            final_df_trajet=final_data_frame_trajet(df_trajet,defin_schema_for_trajet())
            final_df_cars=final_data_frame(df_cars,defin_schema_for_vehicule())
            final_df_emergency=final_data_frame(df_emergency,defin_schema_for_emergency())
            final_df_weather=final_data_frame(df_weather, defin_schema_for_weather())

            # ecriture des données de deplacements temps réel dans influx db

            # df_real_time = applatir_json_data(final_df_trajet)
            #
            # print("-------------------------------------------données applaties-------------------------------")
            #
            # print(df_real_time)
            #
            # df_real_time.printSchema()
            #
            print("-------------------------------------------Ecrire dans Influxdb-------------------------------")
            #write_to_influxdb(final_df_trajet)

            #join all data frame in one using the trajet ID as key join
            # joined_df = (final_df_trajet.join(final_df_cars, (final_df_trajet['tripId'] == final_df_cars['IDTrajet']))
            #                             .join(final_df_emergency,(final_df_trajet['tripId'] == final_df_emergency['emergencyTrip']))
            #                             .join(final_df_weather,(final_df_trajet['tripId'] == final_df_weather['weatherTrip']))
            #              )

            # colones = ('IDTrajet', 'emergencyTrip', 'weatherTrip')
            # dropped_colum=joined_df.drop(*colones)
            #
            #
            # data=(dropped_colum.withColumn("departure_location",
            #                          struct(
            #                              col("departure.latitude").alias("lat"),
            #                              col("departure.longitude").alias("lon")
            #                          ))
            #               .withColumn("arrival_location",
            #                           struct(
            #                               col("arrival.latitude").alias("lat"),
            #                               col("arrival.longitude").alias("lon")
            #                           )))

            #print("dernier schema")
            # joined_df.printSchema()
            #
            # #execution en parallele
            with concurrent.futures.ThreadPoolExecutor() as executor:
                executor.submit(write_to_influxdb, final_df_trajet)
                executor.submit(write_to_db, final_df_trajet,"trajet")
                executor.submit(write_to_db, final_df_cars, "cars")
                executor.submit(write_to_db, final_df_emergency, "emergency")
                executor.submit(write_to_db, final_df_weather,"weather")
                #executor.submit(write_to_iceberg, final_df_trajet,"trajet","ice_trajet")
                #executor.submit(write_to_iceberg, final_df_cars, "cars","ice_cars")
                #executor.submit(write_to_iceberg, final_df_emergency, "emergency","ice_emergency")
                #executor.submit(write_to_iceberg, final_df_weather,"weather","ice_weather")

            #write_to_influxdb(joined_df)
            #write_to_db(joined_df)



            #affichage des topics joint
            #write_data_console(final_df_trajet)

            #insertion en base de données
            # write_to_db(data)


    except Exception as e:
        print(e)


if __name__ == '__main__':
    main()