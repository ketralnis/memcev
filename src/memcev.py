from Queue import Queue
import threading
import contextlib

import _memcev

class Client(object):
    def __init__(self, host, port, size=5):
        self.host = host
        self.port = port
        self.size = size

        # all communication with the event loop is done via this queue
        self.requests = Queue()

        self.eventloop = _memcev.EventLoop(host,
                                           port,
                                           self.size,
                                           self.requests)

        # connections will be built and connected by the eventloop thread, so
        # make sure that the first thing that he does when he comes up is
        # connect to them
        for x in range(self.size):
            self.connect(host, port)

        self.thread = threading.Thread(target=self.eventloop.start)
        self.thread.start()

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__,
                               self.host,
                               self.port)

    def __del__(self):
        self.stop()

    def connect(self, host, port):
        q = Queue()
        self.requests.append(("connect", self.host, self.port, q))
        self.check_response(q, ('connected',))

    def stop(self):
        """
        Instructs the eventloop to stop, but doesn't stop it immediately or
        block for it to finish
        """
        self.requests.put(('stop',))

    @staticmethod
    def check_response(q, expected_tags):
        response = q.get()
        tag = response[0]

        if tag == 'error':
            raise Exception(tag)

        elif tag not in expected_tags:
            raise Exception('Unknown tag %s on %r' % (tag, response))

        return response

    def set(self, key, value, wait=True):
        q = Queue() if wait else None
        self.requests.put(('set', key, value, q))
        self.eventloop.notify()

        if not wait:
            return

        # just wait and validate the response
        self.check_response(q, ['setted'])

    def get(self, key):
        q = Queue()
        self.requests.put(('get', key, q))
        self.eventloop.notify()

        tag, value = self.check_response(q, ['getted'])

        return value

    def join(self):
        self.requests.join()
        return self.thread.join()

def test(host='localhost', port=11211):
    print 'clientfor %s:%d' % (host, port)
    client = Client(host, port)
    print 'client', client
    print 'eventloop', client.eventloop

    print 'setting'
    print client.set('foo', 'bar')
    print 'getting'
    print client.get('foo')

    client.join()
    # print 'eventloop.requests', client.eventloop.requests
    # print 'eventloop.connections', client.eventloop.connections

if __name__ == '__main__':
    test()