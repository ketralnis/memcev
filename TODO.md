* move modules into sub namespace?
* nose/doctests
* make setup.py portable
* change assertions to real exceptions
* wrap the raw queue use with some convenient wrappers
* hunt down all of the TODOs, which are mostly error handing

Known bugs:
* issuing a stop will cause anyone blocking on a response to sleep for forever
  - fix this by keeping track of all sleeping clients
* we require 2.7 because we use the capsule API

