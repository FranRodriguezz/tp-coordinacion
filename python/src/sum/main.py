import os
import logging

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.amount_by_fruit = {}

    def _process_data(self, client_id, fruit, amount):
        client_fruits = self.amount_by_fruit.get(client_id, {})
        client_fruits[fruit] = client_fruits.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))
        self.amount_by_fruit[client_id] = client_fruits


    def _flush_client(self, client_id):
        client_fruits = self.amount_by_fruit.pop(client_id, {})

        logging.info(f"Sending totals for client {client_id}")
        for final_fruit_item in client_fruits.values():
            message = message_protocol.internal.serialize(
                [
                    message_protocol.internal.DATA,
                    client_id,
                    [final_fruit_item.fruit, final_fruit_item.amount],
                ]
            )
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(message)

        logging.info(f"Sending EOF for client {client_id}")
        eof_message = message_protocol.internal.serialize(
            [message_protocol.internal.EOF, client_id, []]
        )
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(eof_message)

    def _process_eof(self, client_id, seen_by):
        if ID in seen_by:
            self.input_queue.send(
                message_protocol.internal.serialize(
                    [message_protocol.internal.EOF, client_id, seen_by]
                )
            )
            return

        self._flush_client(client_id)
        seen_by.append(ID)

        if len(seen_by) < SUM_AMOUNT:
            self.input_queue.send(
                message_protocol.internal.serialize(
                    [message_protocol.internal.EOF, client_id, seen_by]
                )
            )

    def process_data_messsage(self, message, ack, nack):
        [msg_type, client_id, payload] = message_protocol.internal.deserialize(message)
        if msg_type == message_protocol.internal.DATA:
            [fruit, amount] = payload
            self._process_data(client_id, fruit, amount)
        elif msg_type == message_protocol.internal.EOF:
            self._process_eof(client_id, payload)
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_data_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
