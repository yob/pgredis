# pgredis

A server that talks the redis protocol to clients, and stores all data in a postgres table.

This is (presumably) doing to be much slower than keeping the data in memory
like real-redis, but sometimes keeping the data safe and replicated is more
useful than raw performance. You probably shouldn't use this for caching.

## Development

There's not much here yet. To play along, install docker and start the server
like this:

    $ ./auto/run

Then query it with the standard redis-cli:

    $ redis-cli -h 127.0.0.1 set foo bar
    "OK"

    $ redis-cli -h 127.0.0.1 get foo
    "dummy"
