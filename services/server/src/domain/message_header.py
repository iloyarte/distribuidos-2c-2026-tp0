from enum import IntEnum


class MessageType(IntEnum):
    ACK = 1
    BET = 2
    AWAITING_WINNERS = 3
    WINNER = 4
    FINISH = 5


MESSAGE_TYPE_SIZE = 1
PAYLOAD_LEN_SIZE = 4
HEADER_SIZE = MESSAGE_TYPE_SIZE + PAYLOAD_LEN_SIZE

MAX_PAYLOAD_LEN = (1 << (PAYLOAD_LEN_SIZE * 8)) - 1


class MessageHeader:
    def __init__(self, msg_type: MessageType, payload_len: int) -> None:
        self.type = msg_type
        self.payload_len = payload_len

    def serialize(self) -> bytes:
        if not 0 <= self.payload_len <= MAX_PAYLOAD_LEN:
            raise ValueError(
                f"payload_len out of range: {self.payload_len} does not fit in {PAYLOAD_LEN_SIZE} bytes"
            )

        return bytes(
            [
                self.type & 0xFF,
                (self.payload_len >> 24) & 0xFF,
                (self.payload_len >> 16) & 0xFF,
                (self.payload_len >> 8) & 0xFF,
                self.payload_len & 0xFF,
            ]
        )

    @staticmethod
    def deserialize(buf: bytes) -> "MessageHeader":
        if len(buf) != HEADER_SIZE:
            raise ValueError(
                f"invalid header size: expected {HEADER_SIZE} bytes, got {len(buf)}"
            )

        msg_type_value = buf[0]
        payload_len = (
            (buf[1] << 24) | (buf[2] << 16) | (buf[3] << 8) | buf[4]
        )

        return MessageHeader(MessageType(msg_type_value), payload_len)
