from Queue import Queue
import threading
import contextlib

import _memcev

class Client(object):
    def __init__(self, host, port, size=5):
        self.host = host
        self.port = port
        self.size = size

        # all communication with the event loop is done via this queue by
        # inserting work tuples. A work tuple consists in a string, a response
        # Queue, and then any arguments to the command
        self.requests = Queue()

        self.eventloop = _memcev.EventLoop(host,
                                           port,
                                           self.size,
                                           self.requests)

        self.thread = threading.Thread(name="_memcev.EventLoop",
                                       target=self.eventloop.start)
        self.thread.start()

        # make sure that he started successfully
        check_queue = Queue()
        self.send_request('check', check_queue)
        self.check_response(check_queue, ["checked"], timeout=10)

        # connections will be built and connected by the eventloop thread, so
        # make sure that the first thing that he does when he comes up is
        # connect to them
        for x in range(self.size):
            self.connect(host, port)

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__,
                               self.host,
                               self.port)

    def __del__(self):
        self.stop(wait=True)

    def send_request(self, *a):
        # make sure they're valid work tuples. We'd much rather bail here than
        # in the thread where the eventloop can't response
        assert len(a) >= 2
        assert isinstance(a[0], str)
        assert a[1] is None or isinstance(a[1], Queue)

        self.requests.put(a)
        self.eventloop.notify()

    def connect(self, host, port):
        q = Queue()
        self.send_request("connect", q, self.host, self.port)
        self.check_response(q, ['connected'])

    def stop(self, wait=True):
        """
        Instructs the eventloop to stop
        """
        self.send_request('stop', None)
        if wait:
            return self.thread.join()

    @staticmethod
    def check_response(q, expected_tags, timeout=None):
        response = q.get(timeout=timeout)
        tag = response[0]

        if tag == 'error':
            raise Exception(tag)

        elif tag not in expected_tags:
            raise Exception('Unknown tag %s on %r' % (tag, response))

        return response

    def set(self, key, value, wait=True):
        q = Queue() if wait else None
        self.send_request('set', q, key, value)

        if not wait:
            return

        # just wait and validate the response
        self.check_response(q, ['setted'])

    def get(self, key):
        q = Queue()
        self.send_request('get', q, key)

        tag, value = self.check_response(q, ['getted'])

        return value

def test(host='localhost', port=11211):
    print 'clientfor %s:%d' % (host, port)
    client = Client(host, port)

    print 'setting'
    setted = client.set('foo', 'bar')
    print 'setted', setted
    print 'getting'
    gotted = client.get('foo')
    print 'gotted', gotted

    print 'stopping'
    client.stop(wait=True)

    # print 'eventloop.requests', client.eventloop.requests
    # print 'eventloop.connections', client.eventloop.connections

if __name__ == '__main__':
    test()