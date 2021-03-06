# pgredis

WARNING: This is purely a side project for me to learn about postgres, redis
and golang.  It is not in production anywhere, and I take no responsibility for
it.

A server that talks the redis protocol to clients, and stores all data in a
postgres database.

This is (presumably) going to be much slower than keeping the data in memory
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

* Minimise (avoid completely, if possible) DB writes on any read command (GET,
  MGET, LRANGE, etc). Keep reads as fast as possible, and pay the IO price on
  writes (which are hopefully less frequent!). An example: expired keys remain in
  the database, are ignored by read commands, and are only removed when a write
  command notices them.

## Building and Running

We use a docker based environment to build against a consistent golang environment.

    $ ./auto/build

The compiled binary will be stored in bin/. To start the server:

    $ ./bin/pgredis server --database "postgres://user:password@host/dbname?sslmode=disable"

The specific database URL will vary depending on your environment. Once a
connection to the database is successfully opened, pgredis will automatically
create a the tables it needs.

## Development

There's not much here yet. To play along, install docker and start the server
like this:

    $ ./auto/run

Then query it with the standard redis-cli. Not all commands are implemented, but many basic ones are.

### Strings

    $ redis-cli -h 127.0.0.1 set foo bar
    "OK"

    $ redis-cli -h 127.0.0.1 get foo
    "bar"

### Incrementing

    $ redis-cli -h 127.0.0.1 incr foo
    (integer) 1

    $ redis-cli -h 127.0.0.1 decr foo
    (integer) 0

### Lists

    $ redis-cli -h 127.0.0.1 lpush  bar a
    (integer) 1

    $ redis-cli -h 127.0.0.1 lpush  bar b
    (integer) 2

    $ redis-cli -h 127.0.0.1 lrange bar 0 1
    1) "b"
    2) "a"

### Sets

    $ redis-cli -h 127.0.0.1 sadd baz a
    (integer) 1

    $ redis-cli -h 127.0.0.1 sadd baz b
    (integer) 1

    $ redis-cli -h 127.0.0.1 sadd baz b
    (integer) 0

    $ redis-cli -h 127.0.0.1 smembers baz
    1) "a"
    2) "b"

### Sorted Sets

    $ redis-cli -h 127.0.0.1 zadd boo 2 b
    (integer) 1

    $ redis-cli -h 127.0.0.1 zadd boo 1 a
    (integer) 1

    $ redis-cli -h 127.0.0.1 zrange boo 0 1
    1) "a"
    2) "b"

    $ redis-cli -h 127.0.0.1 zrange boo 0 1 WITHSCORES
    1) "a"
    2) "1"
    3) "b"
    4) "2"

### Hashes

    $ redis-cli -h 127.0.0.1 hset foo aaa 1
    (integer) 1

    $ redis-cli -h 127.0.0.1 hset foo bbb 2
    (integer) 1

    $ redis-cli -h 127.0.0.1 hget foo aaa
    "1"

    $ redis-cli -h 127.0.0.1 hgetall foo
    1) "aaa"
    2) "1"
    3) "bbb"
    4) "2"

## Tests

There is a test suite written in ruby. Run it like this:

    $ ./auto/run-specs

The tests are run against a real redis server, and then against pgredis.

If a real redis spec fail, then adjust the spec to pass. The real redis server
is our reference implementation, so it is always right.

If the real redis specs are green and a pgredis spec fails, then it's likely a
bug in pgredis rather than the specs.

We also use the [sidekiq](https://github.com/mperham/sidekiq) test suite as a
smoke test to confirm basic redis compatibility with a real world application.
Run the sidekiq tests like this:

    $ ./auto/run-sidekiq-tests
