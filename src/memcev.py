from Queue import Queue, Empty
from collections import deque
import threading
import re
from functools import partial
import traceback

import _memcev

class Client(_memcev._MemcevClient):
    # 5 seconds is a long time
    timeout = 5000

    def __init__(self, host, port, size=5, debug=True):
        _memcev._MemcevClient.__init__(self)

        assert isinstance(host, str)
        self.host = host

        # python will assert these types for us
        self.port = port
        self.size = size

        self._closed = False

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

        self.check()

        # connections will be built and connected by the eventloop thread, so
        # make sure that the first thing that he does when he comes up is
        # connect to them
        for x in range(self.size):

            # a better algorithm here is to start by establishing one
            # connection to check for reachability, and establish all of the
            # other ones lazily, up to self.size. Often you want to close them
            # over time if your steady-state is a smaller number. But here
            # we'll just make a fixed pool for simplicity

            try:
                self.simple_request('connect', tags='connected')
            except Exception:
                # raise an exception of any of these fail to connect
                self.close()
                raise

    def check(self):
        # make sure that he started successfully
        return self.simple_request('check', timeout=10, tags='checked')

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__,
                               self.host,
                               self.port)

    def __del__(self):
        print 'del1'
        if self.requests:
            print 'del2'
            print 'Warning: stopping %r with %d requests remaining' % (self, len(self.requests))
        print 'del3'
        self.close()
        # super's dealloc is always called
        print 'del4'

        print 'del5'


    def handle_work(self):
        # somebody told our C client that there is work to be done in the
        # self.requests queue by calling self.notify() and he's asking us to
        # check on it. Because of libev event coallescing there may be several
        # items, or none, so we make it safe to have multiple outstanding
        # notifies
        while not self._closed:
            try:
                work = self.requests.popleft()
            except IndexError:
                # no work to do, exit and move on
                return

            print 'got work item', id(work), work, len(self.requests)

            tag = work[0]
            queue = work[1]
            args = work[2:]

            try:
                self._handle_work(tag, queue, args)
            except StopIteration:
                return
            except Exception as e:
                # if we hit an error in that function, which should never happen
                # except due to bugs, we can try to propagate it out into the
                # calling queue if there is one
                if not queue:
                    raise

                # let the empty queue exception get bubbled back into C
                queue.put_nowait(('error', "%s(%s)" % (e.__class__.__name__,
                                                       e.message)))

    def _handle_work(self, tag, queue, args):
        # handle a single work item
        if tag == 'check':
            assert not args

            # should we do this check in C to verify that that machinery works
            # too?
            if queue:
                queue.put(('checked',))

            return

        elif tag == 'connect':
            assert not args

            # call out to C to start the process
            self._connect(partial(self.notify_connected, queue))

            return

        elif tag == 'stop':
            assert not args

            self.stop()

            if queue:
                queue.put(('stopped',))

            # we can't handle any more work without hanging
            raise StopIteration

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

            # n.b. there is a subtle request scheduling/ordering issue here
            # with request scheduling that can happen if we are pre-empted. We
            # popped from the left of the deque, and if we didn't find any
            # connections to use we pushed it back on the left where we found
            # it, right? But if we've been pre-empted by another thread who
            # called popleft in the mean time, it's possible that we aren't
            # doing the work in exactly the order that we received it. This is
            # a small enough issue and since we're trying not to block here
            # it's not really worth solving; but if it leads to starvation or
            # something in practise it could be solved at a mild performance
            # cost by wrapping the get-check-return operation in a mutex
            self.requests.appendleft((tag, queue) + args)
            return

        print 'got connection', connection

        # it's very important that the callback functions here (1) are called
        # and (2) free up the connection when we're done. Exceptions thrown
        # here are bad because we can't know how much of the work they did
        # (did they start using the connection? is it in a good state? can we
        # put it back in the connections queue?). So there needs to be effort
        # done to make sure that they can't throw any, since that exception
        # will occur in another thread where we can't get to it

        if tag == 'get':
            key, = args

            return self._get(connection, key,
                             partial(self.notify_getset, queue, connection))

        elif tag == 'set':
            key, value = args

            return self._set(connection, key, value,
                             partial(self.notify_getset, queue, connection))

    def send_request(self, *a):
        # validate and send a request to the event loop

        # make sure they're valid work tuples. We'd much rather bail here than
        # in the thread where the eventloop can't respond
        assert len(a) >= 2
        assert isinstance(a[0], str)
        assert a[1] is None or isinstance(a[1], Queue)

        self.requests.append(a)
        self.notify()

    def simple_request(self, *a, **kw):
        # we communicate via a queue, so this tries to wrap that behaviour to
        # make it simpler

        # mixing *a and regular kw args can be a pain
        wait = kw.pop('wait', True)
        timeout = kw.pop('timeout', self.timeout)
        tags = kw.pop('tags', None)
        if isinstance(tags, str):
            # in the common case there's only one allowed response type, so
            # remove the tuple boiler plate where possible
            tags = (tags,)
        assert not kw

        q = Queue() if wait else None

        self.send_request(a[0], q, *a[1:])

        if wait:
            response = q.get(timeout=timeout)
            tag = response[0]

            if tag == 'error':
                raise Exception(response[1])

            if tags and tag not in tags:
                raise Exception("Unexpected tag %r in %r" % (tag, response))

            return response

    @staticmethod
    def valid_key(key, valid_re = re.compile('^[a-zA-Z0-9]{,250}$')):
        # Make sure the key is a valid memcached key

        # this isn't actually the valid set of memcached keys, in actuality
        # the valid set of characters is everything *except*:

        # null (0x00)
        # space (0x20)
        # tab (0x09)
        # newline (0x0a)
        # carriage-return (0x0d)

        # but since this is just a toy library I'm optimising for debugging
        # ease and this makes it easier to type and read them for now

        return isinstance(key, str) and valid_re.match(key)

    def close(self):
        """
        Instructs the eventloop to stop
        """
        if self._closed:
            return

        self.simple_request('stop', tags='stopped')
        self.thread.join()
        self._closed = True

    @staticmethod
    def check_response(q, expected_tags, timeout=None):
        # fetch a response from the event loop thread and validate it, turning
        # "error" responses into real exceptions on the calling thread
        response = q.get(timeout=timeout)
        tag = response[0]

        if tag == 'error':
            raise Exception(tag)

        elif tag not in expected_tags:
            raise Exception('Unknown tag %s on %r' % (tag, response))

        return response

    def set(self, key, value, wait=True):
        "Set the given key with the given value into memcached"
        assert self.valid_key(key)
        assert isinstance(value, str)
        assert len(value) <= 250

        return self.simple_request('set', key, value, wait=wait, tags='setted')

    def get(self, key):
        "Get the given key from memcached and return it, or None if it's not present"
        assert self.valid_key(key)

        tag, value = self.simple_request('get', key, tags='getted')

        return value

    def notify_connected(self, response_q, result_tuple):
        # Callback function called on the event loop thread after a connection
        # has been attempted
        if result_tuple[0] == 'connected':
            tag, connection = result_tuple
            self.connections.put(connection)

        if response_q:
            response_q.put(result_tuple)

        self.notify()

    def notify_getset(self, response_q, connection, result_tuple):
        # Callback function called on the event loop thread after a get or set
        # has been attempted
        print 'notify_getset', self, response_q, connection, result_tuple
        if response_q:
            response_q.put(result_tuple)

        self.connections.put(connection)

        self.notify()

def test(host='localhost', port=11211):
    print 'clientfor %s:%d' % (host, port)
    client = Client(host, port)
    print 'client', client

    try:

        print 'getting empty'
        getted = client.get('doesntexist')
        print 'getted empty:', getted
        return

        print 'setting'
        setted = client.set('foo', 'bar')
        print 'setted', setted
        print 'getting'
        getted = client.get('foo')
        print 'getted', getted

    finally:

        print 'stopping, closed:', client._closed
        client.close()
        print 'closed:', client._closed

        del client
        print 'delled'

if __name__ == '__main__':
    test()
