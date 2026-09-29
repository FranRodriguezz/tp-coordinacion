import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top = {}
        self.eof_count = {}
        
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self.input_exchange.stop_consuming()

    def _process_data(self, client_id, fruit, amount):
        client_fruits = self.fruit_top.get(client_id, {})
        client_fruits[fruit] = client_fruits.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))
        self.fruit_top[client_id] = client_fruits

    def _process_eof(self, client_id):
        count = self.eof_count.get(client_id, 0) + 1
        if count < SUM_AMOUNT:
            self.eof_count[client_id] = count
            return

        self.eof_count.pop(client_id, None)
        fruits = self.fruit_top.pop(client_id, {})
        top = sorted(fruits.values(), reverse=True)[:TOP_SIZE]
        fruit_top = [[item.fruit, item.amount] for item in top]
        self.output_queue.send(
            message_protocol.internal.serialize(
                [message_protocol.internal.DATA, client_id, fruit_top]
            )
        )

    def process_messsage(self, message, ack, nack):
        [msg_type, client_id, payload] = message_protocol.internal.deserialize(message)
        if msg_type == message_protocol.internal.DATA:
            [fruit, amount] = payload
            self._process_data(client_id, fruit, amount)
        elif msg_type == message_protocol.internal.EOF:
            self._process_eof(client_id)
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)
        self.input_exchange.close()
        self.output_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
