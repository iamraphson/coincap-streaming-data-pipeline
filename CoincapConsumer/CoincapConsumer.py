import io
import os
import avro.io
import avro.schema
import pyspark.sql.types as T
#from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession, functions as func


asset_schema_location = os.environ.get('ASSET_SCHEMA_LOCATION', './schemas/assets.avsc') 
def load_schema(schema_path):
    return avro.schema.parse(open(schema_path).read())

def decode_asset_avro_message(data):
    asset_avro_schema = load_schema(asset_schema_location)

    bytes_reader = io.BytesIO(data)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(asset_avro_schema)
    return reader.read(decoder)


class CoincapConsumer:
    def __init__(self) -> None:
        self.spark = SparkSession.builder \
            .appName('coincap-consumer') \
            .config("spark.cassandra.connection.host", os.environ.get('ASSET_CASSANDRA_HOST', 'localhost')) \
            .config("spark.cassandra.connection.port", os.environ.get('ASSET_CASSANDRA_PORT', 9042)) \
            .config("spark.cassandra.auth.username", os.environ.get('ASSET_CASSANDRA_USERNAME')) \
            .config("spark.cassandra.auth.password", os.environ.get('ASSET_CASSANDRA_PASSWORD')) \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel('WARN')


    def read_from_kafka(self, topic: str):
        self.df_raw_stream = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", os.environ.get('REDPANDA_BROKERS', 'localhost:9092')) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .load()

    def parse_avro_message_4rm_kafka(self):
        decode_asset_avro_message_udf = func.udf(decode_asset_avro_message, returnType=T.MapType(T.StringType(), T.StringType()))
        df_value_stream = self.df_raw_stream.selectExpr('value')

        self.df_pure_stream = df_value_stream.withColumn("decoded_aseet_data", decode_asset_avro_message_udf(func.col('value'))) \
            .select('decoded_aseet_data', func.explode("decoded_aseet_data")) \
            .select('decoded_aseet_data.id','decoded_aseet_data.asset_name', 'decoded_aseet_data.asset_price', 'decoded_aseet_data.collected_at') \
            .withColumn('asset_price', func.col('asset_price').cast('float')) \
            .withColumn('consumed_at', 
                func.date_format(func.from_utc_timestamp(func.current_timestamp(), "GMT"), "yyyy-MM-dd HH:mm:ss")
            )
    

    def sink_console(self, output_mode: str = 'complete', processing_time: str = '10 seconds'):
        self.df_pure_stream.writeStream \
            .outputMode(output_mode) \
            .trigger(processingTime=processing_time) \
            .format("console") \
            .option("truncate", False) \
            .start()
        

    def sink_into_elasticsearch(self, output_mode: str = 'complete'):
        """
            Sad news, i can't use ES becuase the depanienes are not working with Spark 3.5.0 or 3.5.1
            https://github.com/elastic/elasticsearch-hadoop/issues/2210
        """
        self.df_pure_stream.writeStream \
            .outputMode(output_mode) \
            .format("org.elasticsearch.spark.sql") \
            .option("checkpointLocation", "checkpoint_location") \
            .option("es.nodes", "coincap-elasticsearch") \
            .option("es.port", "9200") \
            .option("es.index.auto.create", "true") \
            .option("es.nodes.wan.only", "true") \
            .option("es.resource", "assets/_doc") \
            .start()

    def sink_into_cassandra(self, output_mode: str = 'complete'):
        print('Writing to Cassandra')
        self.df_pure_stream.writeStream \
            .format('org.apache.spark.sql.cassandra') \
            .option('checkpointLocation', 'checkpoint_location') \
            .options(
                table=os.environ.get('ASSET_CASSANDRA_TABLE'), 
                keyspace=os.environ.get('ASSET_CASSANDRA_KEYSPACE')
            ) \
            .outputMode(output_mode) \
            .option('ttl', '86400') \
            .start()
    
    def waitingForTermination(self):
        self.spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    consumer = CoincapConsumer()

    consumer.read_from_kafka(topic=os.environ.get('ASSET_PRICES_TOPIC', 'data.asset_prices'))
    df_stream = consumer.parse_avro_message_4rm_kafka()

    #consumer.sink_console(output_mode='append')
    #consumer.sink_into_elasticsearch(output_mode='append')
    consumer.sink_into_cassandra(output_mode='append')

    consumer.waitingForTermination()

    