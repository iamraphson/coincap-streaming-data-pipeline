from pyspark.sql import SparkSession, functions as func
import pyspark.sql.types as T
import avro.schema
import avro.io
import io
import os

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
            .getOrCreate()

        self.spark.sparkContext.setLogLevel('WARN')


    def read_from_kafka(self, topic: str):
        return self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", os.environ.get('REDPANDA_BROKERS', 'localhost:9092')) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .load()

    def parse_avro_message_4rm_kafka(self, df_stream):
        decode_asset_avro_message_udf = func.udf(decode_asset_avro_message, returnType=T.MapType(T.StringType(), T.StringType()))
        df_value_stream = df_stream.selectExpr('value')

        return df_value_stream.withColumn("decoded_aseet_data", decode_asset_avro_message_udf(func.col('value'))) \
            .select('decoded_aseet_data', func.explode("decoded_aseet_data")) \
            .select('decoded_aseet_data.asset_name', 'decoded_aseet_data.asset_price', 'decoded_aseet_data.collected_at') \
            .withColumn('asset_price', func.col('asset_price').cast('float')) \
            .withColumn('consumed_at', 
                func.date_format(func.from_utc_timestamp(func.current_timestamp(), "GMT"), "yyyy-MM-dd HH:mm:ss")
            )
    

    def sink_console(self, df_stream, output_mode: str = 'complete', processing_time: str = '5 seconds'):
        df_stream.writeStream \
            .outputMode(output_mode) \
            .trigger(processingTime=processing_time) \
            .format("console") \
            .option("truncate", False) \
            .start()
    
    def waitingForTermination(self):
        self.spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    consumer = CoincapConsumer()

    df_raw_stream= consumer.read_from_kafka(topic=os.environ.get('ASSET_PRICES_TOPIC', 'data.asset_prices'))
    df_stream = consumer.parse_avro_message_4rm_kafka(df_stream=df_raw_stream)

    consumer.sink_console(df_stream=df_stream,output_mode='append')

    consumer.waitingForTermination()

    