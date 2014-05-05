from Queue import Queue, Empty
from collections import deque
import threading
import re
from functools import partial

import _memcev

class Client(_memcev._MemcevClient):
    """
    A libev-based memcached client that manages a fixed-size pool
    """

    # 5 seconds is a long time for a memcached call
    timeout = 5000

    def __init__(self, host, port, size=5, debug=False):
        """
        Build a Client

        Arguments:
        host: The hostname of the memcached server
        port: the TCP port that memcached is running on
        size: how many connections to build and keep around
        """

        _memcev._MemcevClient.__init__(self)

        self.host = host
        self.port = port
        self.size = size

        # makes multiple calls to close() idempotent
        self._closed = False

        # all communication with the event loop is done via this queue by
        # inserting work tuples. A work tuple consists in a string, a response
        # Queue, and then any arguments to the command. Any changes here must be
        # paid with a call to self.notify()
        self.requests = deque()

        # we'll encase our connections in a queue to easily use it as a
        # list/semaphore at the same time. Any changes here must be paid with a
        # call to self.notify()
        self.connections = Queue()

        # the actual thread that runs the event loop
        self.thread = threading.Thread(name="_memcev._MemcevClient.start",
                                       target=self.start)
        self.thread.daemon = debug
        self.thread.start()

        # make sure all of our machinery is running
        self.check()

        # connections will be built and connected by the eventloop thread, so
        # make sure that the first thing that he does when he comes up is
        # connect to them
        for x in range(self.size):

            # a better algorithm here is to start by establishing one connection
            # to check for reachability, and establish all of the other ones
            # lazily, up to self.size. Often you also want to close them over
            # time if your steady-state is a smaller number. But here we'll just
            # make a fixed pool for simplicity

            try:
                self._simple_request('connect', tags='connected')
            except Exception:
                # raise an exception of any of these fail to connect
                self.close()
                raise

    def check(self):
        "make sure that the event loop stuff all works started successfully"
        return self._simple_request('check', timeout=10, tags='checked')

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__,
                               self.host,
                               self.port)

    def __del__(self):
        # calling this isn't strictly necessary but can help speed up the
        # disconnection process
        if self.requests:
            print 'Warning: stopping %r with %d requests remaining' % (self, len(self.requests))
        self.close()
        # super's dealloc is always called

    def _handle_work(self):
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

            tag = work[0]
            queue = work[1]
            args = work[2:]

            try:
                self.__handle_work(tag, queue, args)
            except StopIteration:
                return
            except Exception as e:
                # if we hit an error in that function, which should never happen
                # except due to bugs, we can try to propagate it out into the
                # calling queue if there is one
                if not queue:
                    raise

                # let the empty queue exception get bubbled back into C
                queue.put_nowait(('error', e))

    def __handle_work(self, tag, queue, args):
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
            self._connect(self.host, self.port,
                          partial(self._notify_connected, queue))

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

        # it's very important that the callback functions here (1) are called
        # and (2) free up the connection when we're done. Exceptions thrown
        # here are bad because we can't know how much of the work they did
        # (did they start using the connection? is it in a good state? can we
        # put it back in the connections queue?). So there needs to be effort
        # done to make sure that they can't throw any, since that exception
        # will occur in another thread where we can't get to it

        if tag == 'get':
            key, = args

            return self._getset_request(connection,
                                        self._build_get_request(key),
                                        partial(self._parse_get_response, key), '',
                                        partial(self._notify_getset, queue, connection))

        elif tag == 'set':
            key, value, expire = args

            return self._getset_request(connection,
                                        self._build_set_request(key, value, expire),
                                        partial(self._parse_set_response, key), '',
                                        partial(self._notify_getset, queue, connection))

    def _send_request(self, *a):
        # validate and send a request to the event loop

        # make sure they're valid work tuples. We'd much rather bail here than
        # in the thread where the eventloop can't respond
        assert len(a) >= 2
        assert isinstance(a[0], str)
        assert a[1] is None or isinstance(a[1], Queue)

        self.requests.append(a)
        self.notify()

    def _simple_request(self, *a, **kw):
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

        self._send_request(a[0], q, *a[1:])

        if wait:
            response = q.get(timeout=timeout)
            tag = response[0]

            if tag == 'error':
                # we can either get real exception objects or just strings
                if isinstance(response[1], Exception):
                    raise response[1]
                else:
                    raise Exception(response[1])

            if tags and tag not in tags:
                raise Exception("Unexpected tag %r in %r" % (tag, response))

            return response

    @staticmethod
    def _valid_key(key, valid_re = re.compile('^[a-zA-Z0-9]{1,250}$')):
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

        self._simple_request('stop', tags='stopped')
        self.thread.join()
        del self.thread
        self._closed = True

    def set(self, key, value, expire=0, wait=True):
        "Set the given key with the given value into memcached"

        if not self._valid_key(key) or len(key) > 250:
            raise ValueError("Invalid key: %r" % (key,))

        if not isinstance(value, str) or len(value) > 1024*1024:
            raise ValueError("values must be strings of len<=1mb")

        return self._simple_request('set', key, value, expire, wait=wait, tags='setted')

    def get(self, key):
        "Get the given key from memcached and return it, or None if it's not present"

        if not self._valid_key(key):
            raise ValueError("Invalid key: %r" % (key,))

        tag, key, value = self._simple_request('get', key, tags='getted')

        return value

    def _notify_connected(self, response_q, result_tuple):
        # Callback function called on the event loop thread after a connection
        # has been attempted. Note that this may be a success or failure message

        if result_tuple[0] == 'connected':
            tag, connection = result_tuple
            self.connections.put(connection)

        if response_q:
            response_q.put(result_tuple)

        self.notify()

    def _notify_getset(self, response_q, connection, result_tuple):
        # Callback function called on the event loop thread after a get or set
        # has been attempted
        if response_q:
            response_q.put(result_tuple)

        self.connections.put(connection)

        self.notify()

    @staticmethod
    def _check_server_errors(body):
        if body == 'ERROR\r\n':
            raise Exception('Unknown error from server')

        m = re.match('CLIENT_ERROR (.*)\r\n', body)
        if m:
            raise Exception("Client error: %s" % m.group(1))

        m = re.match('SERVER_ERROR (.*)\r\n', body)
        if m:
            raise Exception("Server error: %s" % m.group(1))

    @classmethod
    def _build_get_request(cls, key):
        assert cls._valid_key(key)
        request = 'get %s\r\n' % (key,)
        return request

    @classmethod
    def _parse_get_response(cls, key, acc, newdata):
        # parse a response from a GET request. May be a partial response. return
        # a tuple of one of:
        #   (True, response)
        #   (False, new accumulator)

        # we're just doing regex matching here rather than any proper parsing.
        # this works for our limited use case but if we add get_multi or
        # anything more complicated we'll have to revisit

        # acc starts as ''
        received_so_far = acc + newdata

        try:
            cls._check_server_errors(received_so_far)
        except Exception as e:
            return True, ('error', e)

        # these will need to be changed if we support get_multi in the future

        if received_so_far == 'END\r\n':
            return True, ('getted', key, None)

        m = re.match(r'VALUE ([^ ]+) ([^ ]+) ([0-9]+)\r\n(.*)\r\nEND\r\n', received_so_far)

        if not m:
            return False, received_so_far

        rkey = m.group(1)
        #rflags = m.group(2)
        byteslen = int(m.group(3))
        payload = m.group(4)

        if len(payload) != byteslen:
            # we're not done, this is a coincidence
            return False, received_so_far

        return True, ('getted', rkey, payload)

    @classmethod
    def _build_set_request(cls, key, value, expiration):
        assert cls._valid_key(key)

        return "set %s 0 %d %d\r\n%s\r\n" % (key, expiration, len(value), value)

    @classmethod
    def _parse_set_response(cls, key, acc, newdata):
        received_so_far = acc + newdata

        try:
            cls._check_server_errors(received_so_far)
        except Exception as e:
            return True, ('error', e)

        # the only valid response
        if received_so_far == 'STORED\r\n':
            return True, ('setted',)

        return False, received_so_far
