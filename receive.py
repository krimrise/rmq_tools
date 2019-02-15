#!/usr/bin/env python
import pika
import os
import logging

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

SIGNAL = 'test'
HOST = 'localhost'


class Consumer(object):
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

    def callback_func(self, channel, method, properties, body):
#        print("{} received '{}'".format(self._host, body))
        self.do(body)

    def do(self, message):
        if message == SIGNAL:
            LOGGER.info('reboot os')
            print('reboot computer... Signal %s' % message)
            os.system('echo "Hi"')
        else:
            LOGGER.info('No reboot os')
            print('No problem! Signal %s' % message)

    def run(self):
        credentials = pika.PlainCredentials("guest", "guest")

        LOGGER.info('Connecting to %s', self._host)
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self._host,
                                      credentials=credentials))

        self._channel = self._connection.channel()

        result = self._channel.queue_declare(durable=True, exclusive=True, auto_delete=True)
        LOGGER.info('queue declare is %s', result.method.queue)

        LOGGER.info('exchange_declare %s', self.EXCHANGE)
        self._channel.exchange_declare(self.EXCHANGE, self.EXCHANGE_TYPE, durable=True)

        self._channel.queue_bind(result.method.queue,
                                 exchange=self.EXCHANGE)

        self._channel.basic_consume(self.callback_func,
                                    result.method.queue,
                                    no_ack=True)

        LOGGER.info('start_consuming')
        self._channel.start_consuming()

    def stop(self):
        LOGGER.info('Stopping')
        self._channel.stop_consuming()

    def close(self):
        LOGGER.info('Close connection.')
        self._connection.close()


def main():
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    # Connect to localhost:5672 as guest with the password guest and virtual host "/" (%2F)
    con = Consumer(HOST)
    try:
        con.run()
    except KeyboardInterrupt:
        con.stop()
    con.close()
#    con.publish('S1')


if __name__ == '__main__':
    main()
