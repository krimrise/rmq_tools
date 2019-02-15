#!/usr/bin/env python
import pika
import logging

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

SIGNAL = 'test'
HOST = 'localhost'


class Publisher(object):
    EXCHANGE = 'ex'
    EXCHANGE_TYPE = 'fanout'
    PUBLISH_INTERVAL = 1
    QUEUE = ''
    ROUTING_KEY = ''

    def __init__(self, host):
        self._connection = None
        self._channel = None
        self._deliveries = None
        self._acked = None
        self._nacked = None
        self._message_number = None
        self._stopping = False

        self._host = host

    def run(self):
        credentials = pika.PlainCredentials("guest", "guest")

        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self._host,
                                      credentials=credentials))

        self._channel = self._connection.channel()

        self._channel.exchange_declare(self.EXCHANGE, self.EXCHANGE_TYPE, durable=True)

    def publish(self, message):
        self._channel.basic_publish(exchange=self.EXCHANGE,
                      routing_key=self.ROUTING_KEY,
                      body=message)
        LOGGER.info('Published message.')

    def close(self):
        LOGGER.info('Close connection.')
        self._connection.close()


def main():
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    # Connect to localhost:5672 as guest with the password guest and virtual host "/" (%2F)
    pub = Publisher(HOST)
    try:
        pub.run()
        pub.publish(SIGNAL)
    except KeyboardInterrupt:
        pub.close()
    pub.close()


if __name__ == '__main__':
    main()
