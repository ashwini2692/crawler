class RabbitMQError(Exception):
    pass


class MessageSendError(RabbitMQError):
    pass


class MessageReceiveError(RabbitMQError):
    pass


class AckMessageError(RabbitMQError):
    pass