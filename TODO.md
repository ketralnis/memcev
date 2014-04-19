* move modules into sub namespace?
* nose/doctests
* make setup.py portable
* change assertions to real exceptions
* hunt down all of the TODOs, which are mostly error handing
* for some reason an exception on the request queue blocks the whole interpreter

Known bugs:
* issuing a stop() will cause anyone in other threads that are blocked on a
  response to sleep for forever
  - can fix this by keeping track of all sleeping clients
* we require 2.7 because we use the capsule API
* we don't really handle EINTR, except where libev does it for us
* we don't really handle timeouts
* we don't really handle errors that require a reconnect. you just lose the
  connection. in fact, libev only seems to tell us about certain errors so there
  may be others that go unreported
