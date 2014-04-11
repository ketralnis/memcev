from Queue import Queue
import threading
import contextlib

import _memcev

class Client(object):
    def __init__(self, host, port, size=5):
        self.host = host
        self.port = port
        self.size = size

        self.requests = Queue()

        # we're going to track with servers slots are used in Python, so this is
        # a Queue of integers that are just slot pointers
        self.connections = Queue(maxsize=size)
        for x in range(size):
            self.connections.put(x)

        self.eventloop = _memcev.EventLoop(host,
                                           port,
                                           self.size,
                                           self.requests)
        threading.Thread(target=self.eventloop.start).run()

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__,
                               self.host,
                               self.port)

    def __del__(self):
        # TODO
        self.requests.put(('stop',))

    @contextlib.contextmanager
    def get_connection(self):
        connection_number = self.connections.get()
        yield connection_number
        self.connections.task_done()

        # put it back
        self.connections.put(connection_number)

    @staticmethod
    def get_response(response, expected_tags):
        tag = response[0]

        if tag == 'error':
            raise Exception(tag)

        elif tag not in expected_tags:
            raise Exception('Unknown tag %s on %r' % (tag, response))

    def set(self, key, value, wait=True):
        q = Queue() if wait else None
        self.requests.put(('set', key, value, q))
        self.eventloop.notify()

        if not wait:
            return

        # just wait and validate the response
        self.get_response(q.get(), ['setted'])

    def get(self, key):
        q = Queue()
        self.requests.put(('get', key, q))
        self.eventloop.notify()

        tag, value = self.get_response(q.get(), ['getted'])

        return value

def test(host='localhost', port=11211):
    print 'clientfor %s:%d' % (host, port)
    client = Client(host, port)
    print 'client', client
    print 'eventloop', client.eventloop
    # print 'eventloop.requests', client.eventloop.requests
    # print 'eventloop.connections', client.eventloop.connections

if __name__ == '__main__':
    test()