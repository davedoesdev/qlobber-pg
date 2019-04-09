const { Writable } = require('stream');
const { EventEmitter } = require('events');
const { Client } = require('pg');
const { queue, asyncify } = require('async');
const iferr = require('iferr');
const { Qlobber, QlobberDedup } = require('qlobber');

// TODO:
// Single messages
//   Inc put back (keep trying singles)
// Unsubscribe
// Events
// Existing messages and 'existing' property in info
// Tests from qlobber-fsq
// Doc that name will go into SQL

class CollectStream extends Writable {
    constructor() {
        super();
        this._chunks = [];
        this._len = 0;
        this.on('finish', () => {
            this.emit('buffer', Buffer.concat(this._chunks, this._len));
        });
    }

    _write(chunk, encdoding, cb) {
        this._chunks.push(chunk);
        this._len += chunk.length;
        cb();
    }
}

class QlobberPG extends EventEmitter {
    constructor(options) {
        super();
        options = options || {};
        this._name = options.name;
        this._db = options.db;
        this._single_ttl = options.single_ttl || (60 * 60 * 1000); // 1h
        this._multi_ttl = options.multi_ttl || (60 * 1000); // 1m
        this._expire_interval = options.expire_interval || (10 * 1000) // 10s
        this._do_dedup = options.dedup === undefined ? true : options.dedup;
        this._handler_concurrency = options.handler_concurrency;

        this._topics = new Map();

        this._filters = options.filter || [];
        if (typeof this._filters[Symbol.iterator] !== 'function') {
            this._filters = [this._filters];
        }

        const qoptions = Object.assign({
            cache_adds: this._topics
        }, options);

        if (this._do_dedup) {
            this._matcher = new QlobberDedup(qoptions);
        } else {
            this._matcher = new Qlobber(qoptions);
        }
    }

    _warning(err) {
        if (err && !this.emit('warning', err)) {
            console.error(err);
        }
        return err;
    }

    _expire() {
        this._queue.push(cb => {
            this._client.query('DELETE FROM messages WHERE expires <= NOW()', cb);
        }, err => {
            this._warning(err);
            this._expire_timeout = setTimeout(this._expire.bind(this), this._expire_interval);
        });
    }

    _filter(info, handlers, cb) {
        const next = i => {
            return (err, ready, handlers) => {
                if (handlers) {
                    if (this._do_dedup) {
                        if (!(handlers instanceof Set)) {
                            handlers = new Set(handlers);
                        }
                    } else if (!Array.isArray(handlers)) {
                        handlers = Array.from(handlers);
                    }
                }

                if (err || !ready || (i === this._filters.length)) {
                    return cb(err, ready, handlers);
                }

                this._filters[i].call(this, info, handlers, next(i + 1));
            };
        };

        next(0)(null, true, handlers);
    }

    _call_handlers2(handlers, info, cb) {
        cb = cb || this._warning.bind(this);

        let called = false;
        let done_err = null;
        let waiting = [];
        const len = this._do_dedup ? handlers.size : handlers.length;

        const done = err => {
            this._warning(err);

            const was_waiting = waiting;
            waiting = [];

            if (was_waiting.length > 0) {
                process.nextTick(() => {
                    for (let f of was_waiting) {
                        f(err);
                    }
                });
            }

            const was_called = called;
            called = true;
            done_err = err;

            if (!was_called) {
                cb();
            }
        };

        const wait_for_done = (err, cb) => {
            this._warning(err);
            if (cb) {
                if (called) {
                    cb(done_err);
                } else {
                    waiting.push(cb);
                }
            }
        };
        wait_for_done.num_handlers = len;

        if (len === 0) {
            return done();
        }

        const deliver_message = lock_client => {
            let data = info.data;
            if (data.startsWith('\\x')) {
                data = data.substr(2);
            }
            data = Buffer.from(data, 'hex');

            info.expires = Date.parse(info.expires);
            info.size = data.length;

            let stream;
            let destroyed = false;
            let delivered_stream = false;

            const common_callback = (err, cb) => {
                if (destroyed) {
                    wait_for_done(err, cb);
                    return null;
                }

                destroyed = true;
                if (stream) {
                    stream.destroy(err);
                }

                return err => {
                    if (cb) {
                        process.nextTick(cb, err);
                    }

                    // From Node 10, 'readable' is not emitted after destroyed.
                    // 'end' is emitted though (at least for now); if this
                    // changes then we could emit 'end' here instead.
                    if (stream) {
                        stream.emit('readable');
                    }
                    done(err);
                };
            };

            const multi_callback = (err, cb) => {
                const cb2 = common_callback(err, cb);
                if (!cb2) {
                    return;
                }
                cb2();
            };

            const single_callback = (err, cb) => {
                const cb2 = common_callback(err, cb);
                if (!cb2) {
                    return;
                }
                this._in_transaction(cb => {
                    this._queue.unshift(asyncify(async () => {
                        try {
                            if (err) {
                                await this._client.query('DELETE FROM messages WHERE id = $1', [
                                    info.id
                                ]);
                            }
                        } finally {
                            await lock_client.end();
                        }
                    }), cb);
                }, cb2);
            };

            const hcb = info.single ? single_callback : multi_callback;
            hcb.num_handlers = len;

            const ensure_stream = () => {
                if (stream) {
                    return stream;
                }

                stream = new PassThrough();
                stream.end(data);
                stream.setMaxListeners(0);

                const sdone = () => {
                    if (destroyed) {
                        return;
                    }
                    destroyed = true;
                    stream.destroy();
                    done();
                };

                stream.once('end', sdone);

                stream.on('error', err => {
                    this._warning(err);
                    sdone();
                });
            };

            for (let handler of handlers) {
                if (handler.accept_stream) {
                    handler.call(this, ensure_stream(), info, hcb);
                    delivered_stream = true;
                } else {
                    handler.call(this, data, info, hcb);
                }
            }

            if (!delivered_stream && !info.single) {
                done();
            }
        };

        if (!info.single) {
            return deliver_message();
        }

        this._in_transaction(cb => {
            this._queue.unshift(asyncify(async () => {
                const lock_client = new Client(this._db);
                let r;
                try {
                    await lock_client.connect();
                    r = await lock_client.query('SELECT pg_try_advisory_lock($1)', [
                        info.id
                    ]);
                } catch (ex) {
                    await lock_client.end();
                    throw ex;
                }
                if (!r.rows[0].pg_try_advisory_lock) {
                    await lock_client.end();
                    return null;
                }
                try {
                    r = await this._client.query('SELECT EXISTS(SELECT 1 FROM messages WHERE id = $1)', [
                        info.id
                    ]);
                } catch (ex) {
                    await lock_client.end();
                    return null;
                }
                if (r.rows[0].exists) {
                    return lock_client;
                }
                await lock_client.end();
                return null;
            }), cb);
        }, iferr(done, lock_client => {
            if (!lock_client) {
                return done();
            }
            deliver_message(lock_client);
        }));
    }

    _call_handlers(handlers, info) {
        if (this._handler_queue) {
            return this._handler_queue.push({
                handlers,
                info
            });
        }
        this._call_handlers2(handlers, info);
    }

    _message(msg) {
        const payload = JSON.parse(msg.payload);
        const handlers = this._matcher.match(payload.topic);

        this._filter(payload, handlers, (err, ready, handlers) => {
            this._warning(err);
            if (!ready) {
                return;
            }
            if (payload.single) {
                if (this._do_dedup) {
                    if (handlers.size > 0) {
                        handlers = new Set([handlers.values().next().value]);
                    }
                } else if (handlers.length > 0) {
                    handlers = [handlers[0]];
                }
            }
            this._call_handlers(handlers, payload);
        });
    }

    start(cb) {
        if (this._client) {
            return cb();
        }

        this._client = new Client(this._db);
        this._queue = queue((task, cb) => task(cb));

        if (this._handler_concurrency) {
            this._handler_queue = queue((task, cb) => {
                this._call_handlers2(task.handlers, task.info, cb);
            }, this._handler_concurrency);
        }

        this._client.connect(iferr(cb, () => {
            this._client.on('notification', msg => {
                process.nextTick(this._message.bind(this), msg);
            }); 
            this._client.query('LISTEN new_message', iferr(cb, () => {
                this._expire();
                cb();
            }));
        }));
    }

    stop(cb) {
        if (this._expire_timeout) {
            clearTimeout(this._expire_timeout);
            delete this._expire_timeout;
        }
        if (this._queue) {
            this._queue.kill();
            delete this._queue;
        }
        if (this._handler_queue) {
            this._handler_queue.kill();
            delete this._handler_queue;
        }
        if (this._client) {
            const client = this._client;
            delete this._client;
            return client.end(cb);
        }
        cb();
    }

    _end_transaction(cb) {
        return (err, ...args) => {
            if (err) {
                return this._queue.unshift(cb => {
                    this._client.query('ROLLBACK', cb);
                }, err2 => cb(err2 || err, ...args));
            }

            this._queue.unshift(cb => {
                this._client.query('COMMIT', cb);
            }, err => cb(err, ...args));
        };
    }

    _in_transaction(f, cb) {
        this._queue.push(
            cb => this._client.query('BEGIN', cb),
            iferr(cb, () => f(this._end_transaction(cb))));
    }

    _ltreeify(topic) {
        return topic.replace(/(^|\.)(\*)($|\.)/, '$1$2{1}$3')
                    .replace(/(^|\.)#($|\.)/, '$1*$2');
    }
        
    _update_trigger(cb) {
        this._in_transaction(cb => {
            this._queue.unshift(asyncify(async () => {
                await this._client.query('DROP TRIGGER IF EXISTS ' + this._name + ' ON messages');
                await this._client.query('CREATE TRIGGER ' + this._name + ' AFTER INSERT ON messages FOR EACH ROW WHEN ((NEW.expires > NOW()) AND (' + Array.from(this._topics.keys()).map(t => "(NEW.topic ~ '" + this._ltreeify(t) +"')").join(' OR ') + ')) EXECUTE PROCEDURE new_message()');
            }), cb);
        }, cb);
    }

    subscribe(topic, handler, options, cb) {
        if (typeof options === 'function') {
            cb = options;
            options = undefined;
        }

        options = options || {};
        cb = cb || this._warning.bind(this);

        this._matcher.add(topic, handler);
        this._update_trigger(cb);
    }

    publish(topic, payload, options, cb) {
        if ((typeof payload !== 'string') &&
            !Buffer.isBuffer(payload) &&
            (payload !== undefined)) {
            cb = options;
            options = payload;
            payload = undefined;
        }

        if (typeof options === 'function') {
            cb = options;
            options = undefined;
        }

        options = options || {};
        cb = cb || this._warning.bind(this);

        const now = Date.now();
        const expires = now + (options.ttl || (options.single ? this._single_ttl : this._multi_ttl));
        const single = !!options.single;

        const insert = data => {
            this._queue.push(cb => this._client.query('INSERT INTO messages(topic, expires, single, data) VALUES($1, $2, $3, $4)', [
                topic,
                new Date(expires),
                single,
                data
            ], cb), iferr(cb, () => cb.call(this, null, {
                topic,
                expires,
                single,
                size: data.length
            })));
        };

        if (Buffer.isBuffer(payload)) {
            return insert(payload);
        }

        if (typeof payload === 'string') {
            return insert(Buffer.from(payload));
        }

        const s = new CollectStream();
        s.on('buffer', insert);
        return s;
    }
}

exports.QlobberPG = QlobberPG;
