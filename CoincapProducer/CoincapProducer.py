import websocket
import datetime as dt
import json
import avro.schema
import avro.io
import io
import os
from kafka import KafkaProducer, errors
import uuid

class CoincapProducer:
    def __init__(self) -> None:
        self.asset_avro_schema = self.load_schema('./schemas/assets.avsc')
        self.asset_topic = os.environ.get('ASSET_PRICES_TOPIC', 'data.asset_prices')
        self.asset_topic_plain = 'data.asset_prices_plain'

        config = {
            'bootstrap_servers': os.environ.get('REDPANDA_BROKERS', 'localhost:9092').split(',')
        }

        self.producer = KafkaProducer(**config)

        websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp(
            'wss://ws.coincap.io/prices?assets=bitcoin,ethereum',
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )

        self.ws.run_forever(reconnect=5)

    def on_open(self, ws) -> None:
        print('Opened Connection!')

    def on_close(self, ws, close_status_code, close_msg) -> None:
        print('### Closed ###')
        print('close details ->', close_status_code, close_msg)

    def on_error(self, ws, error) -> None:
        print('### Error ###')
        print(error)

    def on_message(self, ws, message) -> None:
        print('### message ###')
        assets = json.loads(message)
        print('assets', assets)
        for asset, price in assets.items():
            payload = {
                'id': f'{str(uuid.uuid4())}',
                'asset_name': asset,
                'asset_price': price,
                'collected_at': dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }

            self.publish_asset_prices_plain(json.dumps(payload, default=str).encode('utf-8'))
            encoded_data = self.encode_message(payload, self.asset_avro_schema)
            self.publish_asset_prices(encoded_data)


    def load_schema(self, schema_path):
        return avro.schema.parse(open(schema_path).read())

    def encode_message(self, data, schema):
        writer = avro.io.DatumWriter(schema)
        buffer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(buffer)
        writer.write(data, encoder)

        return buffer.getvalue()

    def publish_asset_prices(self, value):
        try:
            self.producer.send(topic=self.asset_topic, value=value)
        except errors.KafkaTimeoutError as e:
            print('Kafka error', e.__str__())
        except Exception as e:
            print('Error occuring during kafka production', e.__str__())

    def publish_asset_prices_plain(self, value):
        try:
            self.producer.send(topic=self.asset_topic_plain, value=value)
        except errors.KafkaTimeoutError as e:
            print('Kafka error', e.__str__())
        except Exception as e:
            print('Error occuring during kafka production plain', e.__repr__())


if __name__ == "__main__":
    CoincapProducer()
