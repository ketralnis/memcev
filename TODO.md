* move modules into sub namespace?
* nose/doctests
* make setup.py portable
* change assertions to real exceptions
* hunt down all of the TODOs, which are mostly error handing, and printfs
* for some reason an exception on the request queue blocks the whole interpreter
* probably need to do a full GC audit, esp. w.r.t. connections where it will go noticed the most
* need to document where notify() is and should be be called
* double check where we need docstrings and where we need comments instead
* actual docs
* rename private methods with a _

Known bugs:

* issuing a stop() will cause anyone in other threads that are blocked on a
  response to sleep for forever. not a big deal since it's only really called
  on dealloc
  - could fix this by keeping track of all sleeping clients
* we require 2.7 because we use the capsule API
* we don't really handle EINTR, except where libev does it for us
* we don't really handle timeouts at any point in the stack except connections
* we don't really handle errors that require a reconnect. you just lose the
  connection. in fact, libev only seems to tell us about certain errors so there
  may be others that go unreported until a socket read/write fails
* we aren't ready for Python 3
* our set of valid memcached keys is restricted to alphanumerics
* we don't work without EV_MULTIPLICITY
* there's no exception hierarchy, you just get "Exception"
* we make no effort to deal with broken or malicious servers that could cause us
  to block forever or use up all of our memory with long, invalid responses