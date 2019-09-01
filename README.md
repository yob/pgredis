# pgredis

WARNING: This is purely a side project for me to learn about postgres, redis
and golang.  It is not in production anywhere, and I take no responsibility for
it.

A server that talks the redis protocol to clients, and stores all data in a
postgres table.

This is (presumably) doing to be much slower than keeping the data in memory
like real-redis, but sometimes keeping the data safe and replicated is more
useful than raw performance. You probably shouldn't use this for caching.

This project aims to:

* implement a significant part of the redis protocol, but probably not all of
  it. Some commands aren't relevant and some are hard so I won't bother unless
  there's a need

* use postgres as efficiently as possible

* support multiple instances of pgredis pointing at the same database, for high
  availability. Where required, database locks will be used to ensure
  consistency for multiple clients

* never store any state outside postgres. Instances of pgredis can come and go
  as needed and the data will always be safe in postgres.

* avoid any support for redis clustering features. There's no need to cluster
  when you can have multiple instances accessing a shared database

## Development

There's not much here yet. To play along, install docker and start the server
like this:

    $ ./auto/run

Then query it with the standard redis-cli:

    $ redis-cli -h 127.0.0.1 set foo bar
    "OK"

    $ redis-cli -h 127.0.0.1 get foo
    "bar"

## Tests

There is a test suite written in ruby. Run it like this:

    $ ./auto/run-specs

The tests are run against a real redis server, and then against pgredis.

If a real redis spec fail, then adjust the spec to pass. The real redis server
is our reference implementation, so it is always right.

If the real redis specs are green and a pgredis spec fails, then it's likely a
bug in pgredis rather than the specs.

## TODO

* spec and implement a few connection commands, like ECHO, PING and QUIT
* spec and implement list commands (or at least the ones required by sidekiq)
* improve stdout/stderr of the server
  * log in a consistent way
  * add debug levels so it's possible to print full commands and responses if required
  * don't print an error when clients disconnect
* default to leaving some dangerous commands (like FLUSHALL) disabled. Allow the operator
  to opt-in to enabling them with a CLI switch
* Prometheas metrics endpoint?
