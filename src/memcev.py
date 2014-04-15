from Queue import Queue, Empty
import threading
import re

import _memcev

class Client(_memcev._MemcevClient):
    def __init__(self, host, port, size=5):
        _memcev._MemcevClient.__init__(self)

        assert isinstance(host, str)
        self.host = host

        # python will assert these types for us
        self.port = port
        self.size = size

        # all communication with the event loop is done via this queue by
        # inserting work tuples. A work tuple consists in a string, a response
        # Queue, and then any arguments to the command
        self.requests = Queue()

        # we'll encase our connections in a queue to easily use it as a
        # list/semaphore at the same time
        self.connections = Queue(self.size)

        self.thread = threading.Thread(name="_memcev._MemcevClient.start",
                                       target=self.start)
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
        self.close(wait=True)
        return _memcev._MemcevClient.__del__(self)

    def handle_work(self):
        # somebody told our C client that there is work to be done in the
        # self.requests queue by calling self.notify() and he's asking us to
        # check on it. Because of libev event coallescing there may be several
        # items, or none
        while True:
            try:
                work = self.requests.get_nowait()
            except Empty:
                # no work to do, exit and move on
                return

            print 'got work item', work

            tag = work[0]
            queue = work[1]

            if tag == 'check':
                if queue:
                    queue.put(('checked',))
            elif tag == 'connect':
                if queue:
                    queue.put(('connected',))
            elif tag == 'get':
                if queue:
                    queue.put(('getted', 'fakevalue'))
            elif tag == 'set':
                if queue:
                    queue.put(('setted',))
            elif tag == 'stop':

                self.stop()

                if queue:
                    queue.put(('stopped',))

            else:
                raise Exception("Unknown tag %r" % (tag,))

            self.requests.task_done()


    def send_request(self, *a):
        # make sure they're valid work tuples. We'd much rather bail here than
        # in the thread where the eventloop can't respond
        assert len(a) >= 2
        assert isinstance(a[0], str)
        assert a[1] is None or isinstance(a[1], Queue)

        self.requests.put(a)
        self.notify()

    @staticmethod
    def valid_key(key, valid_re = re.compile('^[a-zA-Z0-9]{,250}$')):
        return isinstance(key, str) and valid_re.match(key)

    def connect(self, host, port):
        q = Queue()
        self.send_request("connect", q, self.host, self.port)
        tag, = self.check_response(q, ['connected'])

    def close(self, wait=True):
        """
        Instructs the eventloop to stop
        """
        q = Queue() if wait else None
        self.send_request('stop', q)
        if wait:
            tag, = self.check_response(q, ['stopped'])
            self.thread.join()

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
        assert self.valid_key(key)
        assert isinstance(value, str)
        assert len(value) <= 250

        q = Queue() if wait else None
        self.send_request('set', q, key, value)

        if not wait:
            return

        # just wait and validate the response
        tag, = self.check_response(q, ['setted'])

    def get(self, key):
        assert self.valid_key(key)

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
    getted = client.get('foo')
    print 'getted', getted

    print 'stopping'
    client.close(wait=True)
    print 'closed'

    # print 'eventloop.requests', client.eventloop.requests
    # print 'eventloop.connections', client.eventloop.connections

if __name__ == '__main__':
    test()