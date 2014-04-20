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

        if debug:
            # In general we should be able to kill this thread just due to the
            # destructor being triggered, so this shouldn't be necessary. But in
            # dev we write bugs all of the time, so don't let those hang the
            # program while we are trying to fix this problems
            self.thread.daemon = True

        self.thread.start()

        # make sure that he started successfully
        self.simple_request('check', timeout=10, tags='checked')

        # connections will be built and connected by the eventloop thread, so
        # make sure that the first thing that he does when he comes up is
        # connect to them
        for x in range(self.size):

            # a better algorithm here is to start by establishing one connection
            # to check for reachability, and establish all of the other ones
            # lazily, up to self.size. Often you want to close them over time if
            # your steady-state is a smaller number. But here we'll just make a
            # fixed pool for simplicity

            try:
                self.simple_request('connect', wait=True, tags='connected')
            except Exception:
                # raise an exception of any of these fail to connect
                self.close()
                raise

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__,
                               self.host,
                               self.port)

    def __del__(self):
        self.close()
        # super's dealloc is always called

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

            print 'got work item', id(work), work, len(self.requests)

            tag = work[0]
            queue = work[1]
            args = work[2:]

            try:
                self._handle_work(tag, queue, args)
            except Exception as e:
                # if we hit an error in that function, which should never happen
                # except due to bugs, we can try to propagate it out into the
                # calling queue if there is one
                if queue:
                    try:
                        queue.put_nowait(('error', "%s(%s)" % (e.__class__.__name__,
                                                               e.message)))
                    except:
                        traceback.print_exc()
                else:
                    traceback.print_exc()

    def _handle_work(self, tag, queue, args):
        # handle a single work item
        if tag == 'check':
            assert not args

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

            return

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
            # it. This is a small enough issue and since we're trying not to
            # block here it's not really worth solving; but if it leads to
            # starvation or something in practise it could be solved by
            # wrapping the get-check-return operation in a mutex
            self.requests.appendleft((tag, queue) + args)
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

    def simple_request(self, *a, **kw):
        # we communicate via a queue, so try to wrap that behaviour to make it
        # simpler

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
        "Make sure the key is a valid memcached key"
        return isinstance(key, str) and valid_re.match(key)

    def close(self):
        """
        Instructs the eventloop to stop
        """
        if self._closed:
            return

        self.simple_request('stop', wait=True, tags='stopped')
        self.thread.join()
        self._closed = True

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
        if result_tuple[0] == 'connected':
            self.connections.put(result_tuple[1])

        if response_q:
            response_q.put(result_tuple)

def test(host='localhost', port=11211):
    print 'clientfor %s:%d' % (host, port)
    client = Client(host, port)
    print 'client', client

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
