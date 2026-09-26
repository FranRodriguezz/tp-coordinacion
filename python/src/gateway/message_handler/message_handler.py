from common import message_protocol
import uuid


class MessageHandler:

    def __init__(self):
        self.id = str(uuid.uuid4())

    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([message_protocol.internal.DATA, self.id, [fruit, amount]])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([message_protocol.internal.EOF, self.id, []])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        if self.id == fields[1] and fields[0] == message_protocol.internal.RESULT:
            return fields[2]

        return None

