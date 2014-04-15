from Queue import Queue, Empty
from collections import deque
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
        self.requests = deque()

        # we'll encase our connections in a queue to easily use it as a
        # list/semaphore at the same time
        self.connections = Queue()

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
            q = Queue()
            self.send_request("connect", q)
            tag, = self.check_response(q, ['connected'])

        self.d = {}

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
                work = self.requests.popleft()
            except IndexError:
                # no work to do, exit and move on
                return

            print 'got work item', id(work), work, self.requests

            tag = work[0]
            queue = work[1]
            args = work[2:]

            if tag == 'check':
                if queue:
                    queue.put(('checked',))

                continue

            elif tag == 'connect':
                assert not args

                if not hasattr(self, 'num_connections'):
                    self.num_connections = 0
                self.num_connections += 1

                self.connections.put('connection%d' % self.num_connections)

                if queue:
                    queue.put(('connected',))

                continue

            elif tag == 'stop':
                self.stop()

                if queue:
                    queue.put(('stopped',))

                continue

            elif tag not in ('get', 'set'):
                raise Exception("Unknown tag %r" % (tag,))

            # those are the only commands that can be done without a connection,
            # so next step is to get one

            try:
                connection = self.connections.get_nowait()
            except Empty:
                # no connections available, just put the work tuple back where
                # we found it. when the next person finishes, we'll get called
                # again

                # n.b. there is a subtle ordering issue here with request
                # scheduling that can happen if we are pre-empted. We popped
                # from the left of the deque, and if we didn't find any
                # connections to use we pushed it back on the left where we
                # found it, right? But if we've been pre-empted by another
                # thread who called popleft in the mean time, it's possible that
                # we aren't doing the work in exactly the order that we received
                # it. This is a small enough issue and we're trying not to block
                # here it's not really worth solving but it could be solved by
                # wrapping the get-check- return operation in a mutex if it
                # leads to starvation or something in practise.
                self.requests.appendleft(work)
                return

            print 'got connection', connection

            try:

                if tag == 'get':
                    key, = args
                    if queue:
                        queue.put(('getted', self.d.get(key)))

                elif tag == 'set':
                    key, value = args
                    self.d[key] = value
                    if queue:
                        queue.put(('setted',))

            finally:
                # put it back when we're done
                self.connections.put(connection)


    def send_request(self, *a):
        # make sure they're valid work tuples. We'd much rather bail here than
        # in the thread where the eventloop can't respond
        assert len(a) >= 2
        assert isinstance(a[0], str)
        assert a[1] is None or isinstance(a[1], Queue)

        self.requests.append(a)
        self.notify()

    @staticmethod
    def valid_key(key, valid_re = re.compile('^[a-zA-Z0-9]{,250}$')):
        return isinstance(key, str) and valid_re.match(key)

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
    client.close()
    print 'closed'

if __name__ == '__main__':
    test()