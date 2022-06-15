from amqpstorm import AMQPError

from .clients import RabbitMQConnectionClient
from .exceptions import MessageSendError, MessageReceiveError, AckMessageError
from .mq import MessageQueueContract
from .queue import QueueContract


class RabbitMQQueue(QueueContract):
    def __init__(self, uri: str, exchange: str, routing: str):
        self.uri = uri
        self.exchange = exchange
        self.routing = routing

    def uri(self) -> str:
        return self.uri


class RabbitMQ(MessageQueueContract):
    def __init__(self, queue: RabbitMQQueue, rabbitmq_client: RabbitMQConnectionClient, retry_num=3):
        self.__rabbitmq_client = rabbitmq_client
        self.__queue = queue
        self._channel = None
        self._retry_num = retry_num

    def connect(self):
        if self.__rabbitmq_client.is_closed:
            self.__rabbitmq_client.open()
        self._channel = self.__rabbitmq_client.channel()
        self._channel.exchange.declare(exchange=self.__queue.exchange, durable=True)
        self._channel.queue.declare(self.__queue.uri, durable=True)
        self._channel.queue.bind(queue=self.__queue.uri,
                                 exchange=self.__queue.exchange,
                                 routing_key=self.__queue.routing)

    def send(self, message: str, delivery_mode=2):
        properties = {
            'delivery_mode': delivery_mode
        }
        for _ in range(self._retry_num):
            try:
                self._channel.basic.publish(body=message,
                                            routing_key=self.__queue.routing,
                                            exchange=self.__queue.exchange,
                                            properties=properties)
                return
            except AMQPError as error:
                self.connect()

        raise MessageSendError

    def receive(self, messages_number: int = 1, no_ack=False, break_on_empty=True):
        try:
            self._channel.basic.qos(messages_number, global_=True)
            self._channel.basic.consume(queue=self.__queue.uri, no_ack=False)
            for message in self._channel.build_inbound_messages(break_on_empty=break_on_empty):
                yield message
        except AMQPError as error:
            raise MessageReceiveError(error)

    def queue(self) -> RabbitMQQueue:
        return self.__queue

    def ack_message(self, message):
        try:
            message.ack()
        except AMQPError as error:
            raise AckMessageError(error)

    def disconnect(self):
        self._channel.close()
        self.__rabbitmq_client.close()
