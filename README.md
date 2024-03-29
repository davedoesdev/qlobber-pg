PostgreSQL queue for Node.js:

  - Supports pub-sub and work queues.

  - Highly configurable.

  - Full set of unit tests, including stress tests.

  - API-compatible with
    [qlobber-fsq](https://github.com/davedoesdev/qlobber-fsq).
    
      - Use as an alternative when you need to use the queue from
        multiple hosts (and prefer to use a database over a distributed
        filesystem).

  - Use as an alternative to [RabbitMQ](http://www.rabbitmq.com/),
    [Redis pub-sub](http://redis.io/topics/pubsub) etc.
    
      - For example when you already have a PostgreSQL server.

  - Supports AMQP-like topics with single- and multi-level wildcards.

  - Tested on Linux and Windows.

# Examples

``` javascript
const { QlobberPG } = require('qlobber-pg');
const qpg = new QlobberPG({
    name: 'example1',
    db: {
        host: '/var/run/postgresql',
        database: 'qlobber-pg'
    }
});
qpg.subscribe('foo.*', (data, info) => {
    console.log(info.topic, data.toString());
    const assert = require('assert');
    assert.equal(info.topic, 'foo.bar');
    assert.equal(data, 'hello');
});
qpg.publish('foo.bar', 'hello');
```

You can publish messages using a separate process if you like:

``` javascript
const { QlobberPG } = require('qlobber-pg');
const qpg = new QlobberPG({
    name: 'example2',
    db: {
        host: '/var/run/postgresql',
        database: 'qlobber-pg'
    }
});
qpg.publish('foo.bar', 'hello', qpg.stop);
```

Or use the streaming interface to read and write messages:

``` javascript
const { QlobberPG } = require('qlobber-pg');
const qpg = new QlobberPG({
    name: 'example3',
    db: {
        host: '/var/run/postgresql',
        database: 'qlobber-pg'
    }
});
function handler(stream, info) {
    const data = [];
    stream.on('readable', function () {
        let chunk;
        while (chunk = this.read()) {
            data.push(chunk);
        }
    });
    stream.on('end', function () {
        const s = Buffer.concat(data).toString();
        console.log(info.topic, s);
        const assert = require('assert');
        assert.equal(info.topic, 'foo.bar');
        assert.equal(s, 'hello');
    });
}
handler.accept_stream = true;
qpg.subscribe('foo.*', handler);
qpg.publish('foo.bar').end('hello');
```

The API is described
[here](http://rawgit.davedoesdev.com/davedoesdev/qlobber-pg/master/docs/index.html).

# Installation

To install the module:

``` shell
npm install qlobber-pg
```

You need to create a database on your PostgreSQL server. You can use an
administration tool (e.g. [pgAdmin](https://www.pgadmin.org/)) or the
command line, for example:

``` shell
psql -c 'create database "qlobber-pg";'
```

Then you need to run migrations on your database to create the table
that `qlobber-pg` uses:

``` shell
npm run migrate
```

Note: The database is assumed to be named `qlobber-pg`. If you created a
database with a different name, you’ll need to change it in
[config/default.json](config/default.json).

# Limitations

  - `qlobber-pg` provides no guarantee that the order messages are given
    to subscribers is the same as the order in which the messages were
    written. If you want to maintain message order between readers and
    writers then you’ll need to do it in your application (using ACKs,
    sliding windows etc). Alternatively, use the `order_by_expiry`
    constructor option to have messages delivered in order of the time
    they expire.

  - `qlobber-pg` does its best not to lose messages but in exceptional
    circumstances (e.g. process crash, file system corruption) messages
    may get dropped. You should design your application to be resilient
    against dropped messages.

  - `qlobber-pg` makes no assurances about the security or privacy of
    messages in transit or at rest. It’s up to your application to
    encrypt messages if required.

  - `qlobber-pg` supports Node 10 onwards.

  - Publish topics are restricted to characters `A-Za-z0-9_.`

  - Subscription topics are restricted to characters `A-Za-z0-9_*#.`

# How it works

Publishing a message creates a row in a table in the database. The
columns for each message are:

  - The ID, a big serial number. This is used in queries to make sure
    only messages published since the last check are returned.

  - The topic, a
    [ltree](https://www.postgresql.org/docs/current/ltree.html) label.
    Using ltree means wildcards (`*` and `#`) can be supported when
    subscribing to topics.

  - The expiry time, a time stamp. Messages which have expired are
    periodically deleted from the table.

  - Whether the message should be processed by a single `QlobberPG`
    instance or by all connected `QlobberPG` instances. This is a
    boolean which determines whether each message has work queue or pub
    sub semantics.
    
      - An [advisory
        lock](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS)
        is used to make sure a work queue message is only processed by a
        single `QlobberPG` instance.

  - The message payload, as a byte array.

  - The name of the `QlobberPG` instance which published the message.
    This is used in queries to make sure only messages published since
    the last check are returned. The ID isn’t enough on its own because
    incrementing the ID and adding the row isn’t an atomic operation.
    That is, incrementing the current ID in the table and actually
    inserting a row can be interleaved.

The database is periodically queried for new messages and a trigger is
optionally created to invoke a check as soon as a message is published.

The query made against the table is constructed from the topics to which
the `QlobberPG` instance is subscribed.

# Licence

[MIT](LICENCE)

# Test

To run the default tests (including stress tests):

``` shell
npm test
```

To run the multi-process tests (each process publishing and subscribing
to different messages):

``` shell
npm run test-multi [-- --queues=<number of queues>]
```

If you omit `--queues` then one process will be created per core.

To run the distributed tests (one process per remote host, each one
publishing and subscribing to different messages):

``` shell
npm run test-remote [-- --remote=<host1> --remote=<host2> ...]
```

You can specify as many remote hosts as you like. The test uses
[cp-remote](https://github.com/davedoesdev/cp-remote) to run a module on
each remote host. Make sure on each host:

  - The `qlobber-pq` module is installed at the same location.

  - The same PostgreSQL server is accessible.

Please note the distributed tests don’t run on Windows.

# Lint

``` shell
npm run lint
```

# Code Coverage

``` shell
npm run coverage
```

[c8](https://github.com/bcoe/c8) results are available
[here](http://rawgit.davedoesdev.com/davedoesdev/qlobber-pg/master/coverage/lcov-report/index.html).

Coveralls page is [here](https://coveralls.io/r/davedoesdev/qlobber-pg).

# Benchmarking

To run the benchmark:

``` shell
npm run bench -- --rounds=<number of rounds> \
                 --size=<message size> \
                 --ttl=<message time-to-live in seconds> \
                 (--queues=<number of queues> | \
                  --remote=<host1> --remote=<host2> ...)
```

If you provide at least one `--remote=<host>` argument then the
benchmark will be distributed across multiple hosts using
[cp-remote](https://github.com/davedoesdev/cp-remote). Make sure on each
host:

  - The `qlobber-pq` module is installed at the same location.

  - The same PostgreSQL server is accessible.
