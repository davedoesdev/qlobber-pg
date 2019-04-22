const { Writable, PassThrough } = require('stream');
const { EventEmitter } = require('events');
const { Client } = require('pg');
const { queue, asyncify } = require('async');
const iferr = require('iferr');
const { Qlobber, QlobberDedup } = require('qlobber');
const escape = require('pg-escape');

// TODO:
// Existing messages and 'existing' property in info
// Events
// Tests from qlobber-fsq

// Note: Publish topics should be restricted to A-Za-z0-9_
//       (ltree will error otherwise)
//       Subscriptions should be restricted A-Za-z0-9_*#
//       (qlobber-pg will error otherwise)

class CollectStream extends Writable {
    constructor() {
        super({ autoDestroy: true });
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
        this._check_interval = options.poll_interval || (1 * 1000) // 1s
        this._do_dedup = options.dedup === undefined ? true : options.dedup;
        this._do_single = options.single === undefined ? true : options.single;
        this._order_by_expiry = options.order_by_expiry;

        this._topics = new Map();
        this._deferred = new Set();

        this.filters = options.filter || [];
        if (typeof this.filters[Symbol.iterator] !== 'function') {
            this.filters = [this.filters];
        }

        const qoptions = Object.assign({
            cache_adds: this._topics
        }, options);

        if (this._do_dedup) {
            this._matcher = new QlobberDedup(qoptions);
        } else {
            this._matcher = new Qlobber(qoptions);
        }

        this._queue = queue((task, cb) => task(cb));

        this._message_queue = queue(this._handle_message.bind(this),
                                    options.message_concurrency || 1);

        if (options.handler_concurrency) {
            this._handler_queue = queue((task, cb) => {
                setImmediate(task.cb);
                this._call_handlers2(task.handlers, task.info, cb);
            }, options.handler_concurrency);
        }

        this.stopped = false;
        this.active = true;

        const emit_error = err => {
            if (err) {
                this.stopped = true;
                this.emit('error', err);
            }
        };

        const close_and_emit_error = err => {
            if (err) {
                this.stopped = true;
                this._client.end(() => this.emit('error', err));
            }
        };

        this._client = new Client(this._db);
        this._client.on('error', this.emit.bind(this, 'error'));

        if (options.notify !== false) {
            this._client.on('notification', msg => {
                if (this._chkstop()) {
                    return;
                }
                this._check_now();
            });
        }

        this._queue.push(cb => {
            if (this._chkstop()) {
                return;
            }
            this._client.connect(cb);
        }, emit_error);

        this._queue.push(cb => {
            if (this._chkstop()) {
                return;
            }
            this._client.query(escape('DROP TRIGGER IF EXISTS %I ON messages', this._name), cb);
        }, close_and_emit_error);

        this._queue.push(cb => {
            if (this._chkstop()) {
                return;
            }
            this._client.query('SELECT id FROM messages ORDER BY id DESC LIMIT 1', cb);
        }, iferr(close_and_emit_error, r => {
            this._last_id = (r.rows.length > 0) ? r.rows[0].id : -1;
        }));

        this._queue.push(cb => {
            if (this._chkstop()) {
                return;
            }
            this._client.query(escape('LISTEN new_message_%I', this._name), cb);
        }, close_and_emit_error);

        this._queue.push(cb => {
            if (this._chkstop()) {
                return;
            }
            this._expire();
            this._check();
            this.emit('start');
            cb();
        });
    }

    _chkstop() {
        if (this.stopped && this.active) {
            this.active = false;
            this.emit('stop');
        }

        return this.stopped;
    }

    _warning(err) {
        if (err && !this.emit('warning', err)) {
            console.error(err);
        }
        return err;
    }

    _check_now() {
        if (this._check_timeout) {
            clearTimeout(this._check_timeout);
            delete this._check_timeout;
            this._check();
        } else {
            this._check_delay = 0;
        }
    }

    refresh_now() {
        if (this._expire_timeout) {
            clearTimeout(this._expire_timeout);
            delete this._expire_timeout;
            this._expire();
        } else {
            this._expire_delay = 0;
        }
        this._check_now();
    }

    force_refresh() { // qlobber-fsq compatibility
        this.refresh_now();
    }

    _expire() {
        delete this._expire_timeout;
        this._expire_delay = this._expire_interval;
        this._queue.push(cb => {
            if (this._chkstop()) {
                return cb();
            }
            this._client.query('DELETE FROM messages WHERE expires <= NOW()', cb);
        }, err => {
            if (this._chkstop()) {
                return;
            }
            this._warning(err);
            this._expire_timeout = setTimeout(this._expire.bind(this), this._expire_delay);
        });
    }

    _check() {
        delete this._check_timeout;
        this._check_delay = this._check_interval;
        this._queue.push(cb => {
            if (this._chkstop()) {
                return cb();
            }
            const test = this._topics_test(true);
            if (!test) {
                return cb(null, { rows: [] });
            }
            this._client.query(`SELECT * FROM messages AS NEW WHERE (${test}) ORDER BY ${this._order_by_expiry ? 'expires' : 'id'}`, cb);
        }, (err, r) => {
            if (this._chkstop()) {
                return;
            }
            if (!this._warning(err)) {
                const deferred = this._deferred;
                this._deferred = new Set();
                const done = () => {
                    this._check_timeout = setTimeout(this._check.bind(this), this._check_delay);
                };
                if (r.rows.length === 0) {
                    return done();
                }
                let count = 0;
                const check = () => {
                    if (++count === r.rows.length) {
                        done();
                    }
                };
                let next_id = this._last_id;
                for (let msg of r.rows) {
                    if (msg.id > next_id) {
                        next_id = msg.id;
                    }
                    this._message(msg, deferred.has(msg.id), check);
                }
                this._last_id = next_id;
            }
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

                if (err || !ready || (i === this.filters.length)) {
                    return cb(err, ready, handlers);
                }

                this.filters[i].call(this, info, handlers, next(i + 1));
            };
        };

        next(0)(null, true, handlers);
    }

    _call_handlers2(handlers, info, cb) {
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

            if (!was_called && !this._chkstop()) {
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

        const deliver_message = lock_client => {
            let data = info.data;
            if (!Buffer.isBuffer(data)) {
                if (data.startsWith('\\x')) {
                    data = data.substr(2);
                }
                data = Buffer.from(data, 'hex');
            }

            info.expires = Date.parse(info.expires);
            info.size = data.length;

            let stream;
            let destroyed = false;
            let delivered_stream = false;

            const common_callback = (err, cb) => {
                if (destroyed) {
                    this._warning(err);
                    return err => wait_for_done(err, cb);
                }

                destroyed = true;
                if (stream) {
                    stream.destroy(err);
                } else {
                    this._warning(err);
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
                common_callback(err, cb)();
            };

            const single_callback = (err, cb) => {
                const cb2 = common_callback(err, cb);
                if (!lock_client) {
                    return cb2();
                }
                const lc = lock_client;
                lock_client = null;
                if (err) {
                    return lc.end(cb2);
                }
                lc.query('DELETE FROM messages WHERE id = $1', [
                    info.id
                ], err => lc.end(err2 => cb2(err || err2)));
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

                return stream;
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

        if ((len === 0) || this._chkstop()) {
            return done();
        }

        if (!info.single) {
            return deliver_message();
        }

        this._queue.push(asyncify(async () => {
            if (this._chkstop()) {
                return null;
            }
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
            if (!r.rows[0].pg_try_advisory_lock || this._chkstop()) {
                await lock_client.end();
                return null;
            }
            try {
                r = await this._client.query('SELECT EXISTS(SELECT 1 FROM messages WHERE (id = $1 AND (expires > NOW())))', [
                    info.id
                ]);
            } catch (ex) {
                await lock_client.end();
                throw ex;
            }
            if (r.rows[0].exists) {
                return lock_client;
            }
            await lock_client.end();
            return null;
        }), iferr(done, lock_client => {
            if (!lock_client) {
                return done();
            }
            if (this._chkstop()) {
                return lock_client.end(done);
            }
            deliver_message(lock_client);
        }));
    }

    _call_handlers(handlers, info, cb) {
        if (this._handler_queue) {
            return this._handler_queue.push({
                handlers,
                info,
                cb
            });
        }
        this._call_handlers2(handlers, info, cb);
    }

    _handle_message(payload, cb) {
        const handlers = this._matcher.match(payload.topic);
        this._filter(payload, handlers, (err, ready, handlers) => {
            this._warning(err);
            if (!ready) {
                this._deferred.add(payload.id);
                return cb();
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
            if (handlers.length === 0) {
                return cb();
            }
            this._call_handlers(handlers, payload, cb);
        });
    }

    _should_handle_message(payload) {
        if (payload.id > this._last_id) {
            return !payload.single || this._do_single;
        }

        return payload.single && this._do_single;
    }

    _message(payload, deferred, cb) {
        if (deferred || this._should_handle_message(payload)) {
            return this._message_queue.push(payload, cb);
        }
        cb();
    }

    stop(cb) {
        cb = cb || (err => {
            if (err) {
                this.emit('error', err);
            }
        });

        if (this.stopped) {
            return cb.call(this);
        }
        this.stopped = true;

        this._client.end(err => {
            this._warning(err);

            if (this._expire_timeout && this._check_timeout) {
                setImmediate(this._chkstop.bind(this));
            }

            if (this._expire_timeout) {
                clearTimeout(this._expire_timeout);
                delete this._expire_timeout;
            }

            if (this._check_timeout) {
                clearTimeout(this._check_timeout);
                delete this._check_timeout;
            }

            if (this.active) {
                return this.once('stop', cb.bind(this, err));
            }

            cb.call(this, err);
        });
    }

    stop_watching(cb) { // qlobber-fsq compatibility
        stop(cb);
    }

    _end_transaction(cb) {
        return (err, ...args) => {
            if (err) {
                return this._queue.unshift(cb => {
                    if (this._chkstop()) {
                        return cb(new Error('stopped'));
                    }
                    this._client.query('ROLLBACK', cb);
                }, err2 => cb(err2 || err, ...args));
            }

            this._queue.unshift(cb => {
                if (this._chkstop()) {
                    return cb(new Error('stopped'));
                }
                this._client.query('COMMIT', cb);
            }, err => cb(err, ...args));
        };
    }

    _in_transaction(f, cb) {
        this._queue.push(
            cb => {
                if (this._chkstop()) {
                    return cb(new Error('stopped'));
                }
                this._client.query('BEGIN', cb);
            },
            iferr(cb, () => f(this._end_transaction(cb))));
    }

    _ltreeify(topic) {
        return topic.replace(/(^|\.)(\*)($|\.)/, '$1$2{1}$3')
                    .replace(/(^|\.)#($|\.)/, '$1*$2');
    }

    _topics_test(check) {
        const topics = this._topics;
        if (topics.size === 0) {
            if (!check || this._deferred.size == 0) {
                return null;
            }
        }
        let r = '';
        if (topics.size > 0) {
            r = `(${Array.from(topics.keys()).map(t => escape('(NEW.topic ~ %L)', this._ltreeify(t))).join(' OR ')})`;
            if (check) {
                r = `((${r}) AND ((NEW.id > ${this._last_id}) OR NEW.single))`;
            }
        }
        if (check && (this._deferred.size > 0)) {
            const bracket = r;
            if (bracket) {
                r = `(${r} OR `;
            }
            r = `${r}(${Array.from(this._deferred).map(id => escape('(NEW.id = %L)', String(id))).join(' OR ')})`;
            if (bracket) {
                r = `${r})`;
            }
        }
        return `(${r} AND (NEW.expires > NOW()))`;
    }
        
    _update_trigger(cb) {
        this._in_transaction(cb => {
            this._queue.unshift(asyncify(async () => {
                if (this._chkstop()) {
                    throw new Error('stopped');
                }
                await this._client.query(escape('DROP TRIGGER IF EXISTS %I ON messages', this._name));
                const test = this._topics_test(false);
                if (test) {
                    await this._client.query(escape(`CREATE TRIGGER %I AFTER INSERT ON messages FOR EACH ROW WHEN ${test} EXECUTE PROCEDURE new_message(%I)`, this._name, this._name));
                }
            }), cb);
        }, cb);
    }

    _valid_stopic(topic, cb) {
        for (let label of topic.split('.')) {
            if ((label !== '*') &&
                (label !== '#') &&
                !/^[A-Za-z0-9_]+$/.test(label)) {
                cb(new Error('invalid subscription topic: ' + topic));
                return false;
            }
        }
        return true;
    }

    subscribe(topic, handler, options, cb) {
        if (typeof options === 'function') {
            cb = options;
            options = undefined;
        }

        options = options || {};
        const cb2 = (err, ...args) => {
            this._warning(err);
            if (cb) {
                cb.call(this, err, ...args);
            }
        };

        if (!this._valid_stopic(topic, cb)) {
            return;
        }

        this._matcher.add(topic, handler);
        this._update_trigger(cb2);
    }

    unsubscribe(topic, handler, cb) {
        if (typeof topic === 'function') {
            cb = topic;
            topic = undefined;
            handler = undefined;
        }

        const cb2 = (err, ...args) => {
            this._warning(err);
            if (cb) {
                cb.call(this, err, ...args);
            }
        };

        if (topic === undefined) {
            this._matcher.clear();
        } else {
            if (!this._valid_stopic(topic, cb2)) {
                return;
            }
            if (handler === undefined) {
                this._matcher.remove(topic);
            } else {
                this._matcher.remove(topic, handler);
            }
        }

        this._update_trigger(cb2);
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
        const cb2 = (err, ...args) => {
            this._warning(err);
            if (cb) {
                cb.call(this, err, ...args);
            }
        };

        const now = Date.now();
        const expires = now + (options.ttl || (options.single ? this._single_ttl : this._multi_ttl));
        const single = !!options.single;

        const insert = data => {
            if (this._chkstop()) {
                return cb2(new Error('stopped'));
            }
            // Note: ltree will validate topic (maxlen 255) to A-Za-z0-9_
            this._queue.push(cb => this._client.query("INSERT INTO messages(topic, expires, single, data) VALUES($1, $2, $3, decode($4::text, 'hex'))", [
                topic,
                new Date(expires),
                single,
                data.toString('hex')
            ], cb), iferr(cb2, () => cb2(null, {
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

        s.once('buffer', insert);

        s.once('error', function (err) {
            this.removeListener('buffer', insert);
            this.once('close', () => {
                cb2(err);
            });
            this.end();
        });

        return s;
    }
}

exports.QlobberPG = QlobberPG;
