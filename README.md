# memcev

A memcached client that internally uses libev

Examples:

    >>> from memcev import Client
    >>> c = Client('localhost', 11211)

    >>> print c.get('doesntexist')
    None

    >>> c.set('foo', 'bar')
    >>> print c.get('foo')
    bar

Known issues:

* single server, no distribution
* string values only
* no compression
* only get and set. No get_multi/set_multi etc
* issuing a stop() will cause anyone in other threads that are blocked on a
  response to sleep for forever. not a big deal since it's only really called
  on dealloc
  - could fix this by keeping track of all sleeping clients
* we require Python 2.7 because we use the capsule API
* we don't really handle EINTR, except where libev does it for us
* we don't really handle timeouts at any point in the stack except connections
* we don't really handle errors that require a reconnect. you just lose the
  connection. in fact, libev only seems to tell us about certain errors so there
  may be others that go unreported until a socket read/write fails, which we
  then also fail to handle :)
* we aren't ready for Python 3
* our set of valid memcached keys is more restrictive than memcached's
* we don't work without EV_MULTIPLICITY
* there's no exception hierarchy, you just get "Exception"
* we make no effort to deal with broken or malicious servers that could cause us
  to block forever or use up all of our memory with long, invalid responses