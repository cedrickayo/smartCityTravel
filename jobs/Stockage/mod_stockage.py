import logging
import os
from datetime import datetime

import requests
from elasticsearch import Elasticsearch
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, BooleanType, IntegerType, DateType, FloatType, StructType, DoubleType, \
    TimestampType
from pyspark.sql.utils import to_str

# Create the index with the mapping
es=Elasticsearch("http://elasticsearch:9200")


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

def write_to_elastidsearch(df):
    if not es.indices.exists:
        es.indices.create(index="vehicule_trajets", body=generate_mapping_es_from_df(df))
    try:
        (df.write.
         format("org.elasticsearch.spark.sql").
         mode("append")
         .option("es.nodes", "elasticsearch")
         .option("es.port", "9200")
         #.option("es.nodes.discovery", "false")
         #.option("es.nodes.wan.only", "true") # Use this if connecting to a remote cluster
         #.option("es.index.auto.create", "true")
         .option("es.mapping.id", "tripId") # Map the 'tripId' field as the document ID in Elasticsearch
         #.option("es.mapping.exclude", "id")
         .option("es.resource", "vehicule_trajets/_doc")
         .save()

         )
        logging.info("successfully write in elasticsearch")
    except Exception as e:
        logging.error(f"impossible d'écrire dans la base elasticsearch à cause de {e}")
        print(f"impossible de stocker dans la base elasticsearch à cause de {e}")

def write_data_into_influx(df, batchId):
    print("Batch id: " + str(batchId))

    #Appel de la fonction transform_data_to_lp pour qui transformer le dataframe en données comprehensibles à inserer dans influxdb

    # .rdd	: Accès bas niveau, pour appliquer des fonctions Python : tranforme le df en une collection distribuée de lignes, et chaque ligne est représentée comme un objet Row
    # .map(row_to_lp)	: Convertit chaque ligne en format Line Protocol
    # .filter(...)	: Évite les lignes nulles ou mal formatées
    # .collect()	: Ramène tous les résultats dans une liste Python sur le driver

    df.printSchema()
    # df.show(truncate=False) affiche le dataframe
    # df.select("tripId", "departure_name", "departure_latitude", "departure_longitude", "arrival_name",
    #                "arrival_latitude", "arrival_longitude", "heure_depart", "flag_arrivee").show(truncate=False)
    lines = transform_dataframe_to_rdd(df).map(transform_row_to_lp).filter(lambda x: x is not None).collect()

    #print(lines)

    if not lines:
        logging.warning(f"Aucune ligne valide à insérer dans InfluxDB pour le batch {batchId}.")
        return
    data="\n".join(lines)
    print(data)

    print("Writing to influxDB .....")
    print(os.getenv("INFLUXDB_ORG"))
    print(os.getenv('INFLUXDB_TOKEN'))
    try:
        res = requests.post(
            url="http://influxdb:8086/api/v2/write",
            params={
                "org": os.getenv("INFLUXDB_ORG"),
                "bucket": os.getenv("INFLUXDB_BUCKET"),
                "precision": "s"
            }, #le timestamp ici  doit être peut avoir les valeurs ns, ms ou s pour nano, micro, millinsecondes
            headers={
                "Authorization": f"Token {os.getenv('INFLUXDB_TOKEN')}",
                "Content-Type": "text/plain"
            },
            data=data)
        # (df.write.
        #  format("influxdb")
        #  .mode("append")
        #  .option("host", "influxdb")
        #  .option("port", "8086")
        #  .option("user", os.getenv("INFLUXDB_USER"))
        #  .option("password", os.getenv("INFLUXDB_USER_PASSWORD"))
        #  .option("token", os.getenv("INFLUXDB_TOKEN"))
        #  .option("org", os.getenv("INFLUXDB_ORG"))
        #  .option("bucket", os.getenv("INFLUXDB_BUCKET"))
        #  .save()
        #  )

    except Exception as e:
        logging.error(f"impossible de stocker dans la base InfluxDB à cause de {e}")
    else:
        if res.status_code == 204:
            logging.info(" ✅ successfully write in influxDB")
        else:
            logging.error(f"❌ Erreur InfluxDB: {res.status_code} - {res.text}")

#once=True
#.option("truncate", "false") \
#.trigger(once=True) \
def write_to_influxdb(joined_dataframe):
    query = joined_dataframe.writeStream \
        .foreachBatch(write_data_into_influx) \
        .option("checkpointLocation", "/opt/bitnami/spark/checkpoint_dir_influx") \
        .start()
    query.awaitTermination()


#Contrairement à une base de données relationnelle comme PostgreSQL ou MySQL,
#InfluxDB n’utilise pas des “tables”, mais plutôt une structure orientée séries temporelles, organisée autour de buckets, mesures, tags, et champs
#donc on postes ou ecrit dans le bucket en fait
# | Élément           | Rôle                                                                          |
# | ----------------- | ----------------------------------------------------------------------------- |
# | **Bucket**        | Équivaut à une base de données — tu POSTes tes données ici                    |
# | **\_measurement** | Équivaut à une “table logique” (ex: `"trajets"`)                              |
# | **tags**          | Indexés — utilisés pour filtrer rapidement (ex: `tripId`, `departure`)        |
# | **fields**        | Valeurs numériques/textuelles — non indexées (ex: `latitude`, `flag_arrivee`) |
# | **timestamp**     | Clé primaire — chaque point est daté et stocké selon sa temporalité           |

def applatir_json_data(df):
        #le df retourné à cette structure or il devait etre sous cle valeur, rasion pour laquelle on va le transformé
        #
        # StructField("tripId", StringType(), False),
        # StructField("departure", gps_coordonate_schema(), False),
        # StructField("arrival", gps_coordonate_schema(), False),
        # StructField("heure_depart", DateType(), True),
        # StructField("flag_arrivee", BooleanType(), True),

    df_flat = df.selectExpr(
        "tripId",
        "departure.name as departure_name",
        "departure.latitude as departure_latitude",
        "departure.longitude as departure_longitude",
        "arrival.name as arrival_name",
        "arrival.latitude as arrival_latitude",
        "arrival.longitude as arrival_longitude",
        "heure_depart",
        "flag_arrivee"
    )

    df_flat.printSchema()
    #df_flat.show()

    return df_flat

def converted_dataframe(df):
    df_converted = df.withColumn("departure_latitude", col("departure_latitude").cast(DoubleType())) \
        .withColumn("departure_longitude", col("departure_longitude").cast(DoubleType())) \
        .withColumn("arrival_latitude", col("arrival_latitude").cast(DoubleType())) \
        .withColumn("arrival_longitude", col("arrival_longitude").cast(DoubleType()))
    return df_converted


def transform_row_to_lp(df_flat):
    #pour inscrire les données dans influx, celle ci doivent avoir la structure ci dessous en line protocol
    #donc chaque data passée en parametre sera transformé en texte
    #trajets,tripId=T123,departure=Paris,arrival=Lyon departure_latitude=48.85,departure_longitude=2.35,flag_arrivee=true 1721742750
    #measurement=>trajets, tags=> tripId=T123,departure=Paris,arrival=Lyon fields=>departure_latitude=48.85,departure_longitude=2.35 flag_arrivee=true timestamp=>1721742750
    try:


        timestamp = int(df_flat['heure_depart'].timestamp())

        measurement = "trajets"
        tags = f"tripId={df_flat['tripId']},departure={df_flat['departure_name']},arrival={df_flat['arrival_name']}"
        fields = (
            f"departure_latitude={df_flat['departure_latitude']},"
            f"departure_longitude={df_flat['departure_longitude']},"
            f"arrival_latitude={df_flat['arrival_latitude']},"
            f"arrival_longitude={df_flat['arrival_longitude']},"
            f"flag_arrivee={str(df_flat['flag_arrivee']).lower()}"
        )
        # Optionnel : ajouter timestamp (sinon Influx utilise "now()")
        print(f"{measurement},{tags} {fields} {timestamp}")
        return f"{measurement},{tags} {fields} {timestamp}"
    except Exception as e:
        logging.warning(f"Erreur ligne on data {df_flat} due to : {e}")
        return None

def transform_dataframe_to_rdd(df):
    # Un RDD est une collection distribuée de lignes, et chaque ligne est représentée comme un objet Row
    # un exemple de rdd qui represente en fait les valeurs de mon dataframe lorsque je fais dataframe.show()
    # Row(
    #     tripId='T001',
    #     departure_name='Paris',
    #     departure_latitude=48.8566,
    #     departure_longitude=2.3522,
    #     arrival_name='Lyon',
    #     arrival_latitude=45.764,
    #     arrival_longitude=4.8357,
    #     heure_depart=datetime.datetime(2025, 7, 23, 15, 30, 0),
    #     flag_arrivee=False
    #   )
    return df.rdd
