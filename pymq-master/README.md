# pymq
Python Message Broker Wrapper package provides client interface to work with queues and implemented certain mq wrappers.

#### General usage
To create a new mq client you should define following classes:
1. Queue (inherit from QueueContract) -- contains queue parameters like url, name, etc.
2. ConnectionClient -- allows to connect to queues and provides default methods (for example, Connection, UriConnection from amqpstorm)
3. MQ Client (inherit from MessageQueueContract) -- MQ client init with queue (Queue) and connection (ConnectionClient) and implements methods to work with queue. 

#### Implemented clients

##### RabbitMQ

As fo now, there is RabbitMQ python wrapper allows to send and receive messages from RabbitMQ queues, based on AMQPStorm packages.

Usage example

`from pymq import RabbitMQ, RabbitMQQueue, RabbitMQConnectionClient`

`permits_queue = RabbitMQQueue(uri=os.getenv("PERMITS_QUEUE_NAME"),
                                  exchange=os.getenv("PERMITS_EXCHANGE_NAME"),
                                  routing=os.getenv("PERMITS_ROUTING_KEY"))`
                                  
`rabbitmq_client = RabbitMQConnectionClient(os.getenv("RABBITMQ_CONNECTION_STRING"))`

`rmq = RabbitMQ(permits_queue, rabbitmq_client)`

`rmq.send(json.dumps(permit_dict))`

`messages = rmq.receive(MESSAGE_RECEIVE_BATCH_SIZE)`


