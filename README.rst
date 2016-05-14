============
 Next Event
============

This code is made to process a set of "events",
where each event consists of 

* a key identifying an object the event is associated with
* some data that constitutes the event
* a timestamp of the event

The result is a list of events with the same data as the orginal event,
*and* the timestamp of the next event for the same key.

There is a built-in timeout after which events are no longer "waiting" for another event.

See the test code for use.
