'use strict';
const { Writable, PassThrough, pipeline } = require('stream');
const { EventEmitter } = require('events');
const { Client, types } = require('pg');
const QueryStream = require('pg-query-stream');
const { queue, asyncify } = require('async');
const iferr = require('iferr');
const { Qlobber, QlobberDedup } = require('qlobber');
const escape = require('pg-escape');

const global_lock = -1;

/* global BigInt */
const minus_one_n = BigInt(-1); // eslint and documentation fail on -1n

types.setTypeParser(20 /* int8, bigserial */, BigInt);

// Note: Publish labels are restricted to A-Za-z0-9_
//       Subscription labels are restricted to A-Za-z0-9_*#

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

/**
 * Creates a new `QlobberPG` object for publishing and subscribing to a
 * PostgreSQL queue.
 *
 * @param {Object} options - Configures the PostgreSQL queue.
 * @param {String} options.name - Unique identifier for this `QlobberPG`
 *     instance. Every instance connected to the queue at the same time
 *     must have a different name.
 * @param {Object} options.db - [`node-postgres` configuration](https://node-postgres.com/api/client)
 *     used for communicating with PostgreSQL.
 * @param {Integer} [options.single_ttl=1h] - Default time-to-live
 *     (in milliseconds) for messages which should be read by at most one
 *     subscriber. This value is added to the current time and the resulting
 *     expiry time is put into the message's database row. After the expiry
 *     time, the message is ignored and deleted when convenient.
 * @param {Integer} [options.multi_ttl=1m] - Default time-to-live
 *     (in milliseconds) for messages which can be read by many subscribers.
 *     This value is added to the current time and the resulting expiry time is
 *     put into the message's database row. After the expiry time, the message
 *     is ignored and deleted when convenient.
 * @param {Integer} [options.expire_interval=10s] - Number of milliseconds
 *     between deleting expired messages from the database.
 * @param {Integer} [options.poll_interval=1s] - Number of milliseconds between
 *     checking the database for new messages.
 * @param {Boolean} [options.notify=true] - Whether to use a database trigger
 *     to watch for new messages. Note that this will be done in addition to
 *     polling the database every `poll_interval` milliseconds.
 * @param {Integer} [options.message_concurrency=1] - The number of messages
 *     to process at once.
 * @param {Integer} [options.handler_concurrency=0] - By default (0), a message
 *     is considered handled by a subscriber only when all its data has been
 *     read. If you set `handler_concurrency` to non-zero, a message is
 *     considered handled as soon as a subscriber receives it. The next message
 *     will then be processed straight away. The value of `handler_concurrency`
 *     limits the number of messages being handled by subscribers at any one
 *     time.
 * @param {Boolean} [options.order_by_expiry=false] - Pass messages to
 *     subscribers in order of their expiry time.
 * @param {Boolean} [options.dedup=true] - Whether to ensure each handler
 *     function is called at most once when a message is received.
 * @param {Boolean} [options.single=true] - Whether to process messages meant
 *     for _at most_ one subscriber (across all `QlobberPG` instances), i.e.
 *     work queues.
 * @param {String} [options.separator='.'] - The character to use for separating
 *     words in message topics.
 * @param {String} [options.wildcard_one='*'] - The character to use for
 *     matching exactly one word in a message topic to a subscriber.
 * @param {String} [options.wildcard_some='#'] - The character to use for
 *     matching zero or more words in a message topic to a subscriber.
 * @param {Function | Array<Function>} [options.filter] - Function called before each message is processed.
 * - The function signature is: `(info, handlers, cb(err, ready, filtered_handlers))`
 * - You can use this to filter the subscribed handler functions to be called
 *   for the message (by passing the filtered list as the third argument to
 *   `cb`).
 * - If you want to ignore the message _at this time_ then pass `false` as the
 *   second argument to `cb`. `options.filter` will be called again later with
 *   the same message.
 * - Defaults to a function which calls `cb(null, true, handlers)`.
 * - `handlers` is an ES6 Set, or array if `options.dedup` is falsey.
 * - `filtered_handlers` should be an ES6 Set, or array if `options.dedup`
 *   is falsey. If not, `new Set(filtered_handlers)` or
 *   `Array.from(filtered_handlers)` will be used to convert it.
 * - You can supply an array of filter functions - each will be called in turn
 *   with the `filtered_handlers` from the previous one.
 * - An array containing the filter functions is also available as the `filters`
 *   property of the `QlobberPG` object and can be modified at any time.
 * @param {Integer} [options.batch_size=100] - Passed to https://github.com/brianc/node-pg-query-stream[`node-pg-query-stream`]
 *     and specifies how many messages to retrieve from the database at a time
 *     (using a cursor).
 */
class QlobberPG extends EventEmitter {
    constructor(options) {
        super();
        options = options || {};
        this._name = options.name;
        this._db = options.db;
        this._single_ttl = options.single_ttl || (60 * 60 * 1000); // 1h
        this._multi_ttl = options.multi_ttl || (60 * 1000); // 1m
        this._expire_interval = options.expire_interval || (10 * 1000); // 10s
        this._check_interval = options.poll_interval || (1 * 1000); // 1s
        this._notify = options.notify == undefined ? true : options.notify;
        this._do_dedup = options.dedup === undefined ? true : options.dedup;
        this._do_single = options.single === undefined ? true : options.single;
        this._order_by_expiry = options.order_by_expiry;
        this._batch_size = options.batch_size || 100;

        this._topics = new Map();
        this._deferred = new Set();

        this.filters = options.filter || [];
        if (typeof this.filters[Symbol.iterator] !== 'function') {
            this.filters = [this.filters];
        }

        this._matcher_options = Object.assign({
            cache_adds: this._topics
        }, options);

        if (this._do_dedup) {
            this._matcher = new QlobberDedup(this._matcher_options);
        } else {
            this._matcher = new Qlobber(this._matcher_options);
        }

        this._matcher_marker = {};

        this._queue = queue((task, cb) => task(cb));

        this._message_queue = queue((task, cb) => {
            setImmediate(task.cb);
            this._handle_message(task.payload, cb);
        }, options.message_concurrency || 1);
        
        if (options.handler_concurrency) {
            this._handler_queue = queue((task, cb) => {
                setImmediate(task.cb);
                this._call_handlers2(task.handlers, task.info, cb);
            }, options.handler_concurrency);
        }

        this.stopped = false;
        this.active = true;
        this.initialized = false;

        const emit_error = err => {
            if (err) {
                this.stopped = true;
                /**
                 * Error event. Emitted if an unrecoverable error occurs.
                 * QlobberPG may stop querying the database for messages.
                 *
                 * @event error
                 * @memberof QlobberPG
                 * @type {Object}
                 */
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

        if (this._notify !== false) {
            this._client.on('notification', () => {
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

        this._queue.push(asyncify(async () => {
            if (this._chkstop()) {
                return;
            }
            await this._client.query('SELECT pg_advisory_lock($1)', [
                global_lock
            ]);
            try {
                await this._client.query(escape('DROP TRIGGER IF EXISTS %I ON messages', this._name));
            } finally {
                await this._client.query('SELECT pg_advisory_unlock($1)', [
                    global_lock
                ]);
            }
        }), close_and_emit_error);

        this._reset_last_ids(close_and_emit_error);

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
            /**
             * Start event. Emitted when messages can be published and
             * subscribed to.
             *
             * @event start
             * @memberof QlobberPG
             */
            this.initialized = true;
            this.emit('start');
            cb();
        });
    }

    _reset_last_ids(cb) {
        this._queue.push(cb => {
            if (this._chkstop()) {
                return;
            }
            this._client.query('SELECT DISTINCT ON (publisher) publisher, id FROM messages ORDER BY publisher, id DESC', cb);
        }, iferr(cb, r => {
            this._last_ids = new Map();
            for (let row of r.rows) {
                this._last_ids.set(row.publisher, row.id);
            }
            cb();
        }));
    }

    _chkstop() {
        if (this.stopped && this.active) {
            this.active = false;
            /**
              * Stop event. Emitted after {@link QlobberPG#stop} has been called
              * and access to the database has stopped.
              *
              * @event stop
              * @memberof QlobberPG
              */
            this.emit('stop');
        }

        return this.stopped;
    }

    _warning(err) {
        /**
         * Warning event. Emitted if a recoverable error occurs.
         * QlobberPG will continue to query the database for messages.
         *
         * If you don't handle this event, the error will be written to
         * `console.error`.
         *
         * @event warning
         * @memberof QlobberPG
         * @type {Object}
         */
        if (err && !this.emit('warning', err)) {
            console.error(err); // eslint-disable-line no-console
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

    /**
     * Check the database for new messages now rather than waiting for the next
     * periodic check to occur.
     */
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

    /**
     * Same as {@link QlobberPG#refresh_now}.
     */
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
                const passthru = new PassThrough();
                passthru.end();
                return cb(null, passthru);
            }
            //console.log("QUERYING", this._name, test);
            cb(null, this._client.query(new QueryStream(`SELECT * FROM messages AS NEW WHERE (${test}) ORDER BY ${this._order_by_expiry ? 'expires' : 'id'}`, [], {
                batchSize: this._batch_size
            })));
        }, (err, stream) => {
            if (this._chkstop()) {
                return;
            }
            if (err) {
                return this.emit('error', err);
            }
            const deferred = this._deferred;
            this._deferred = new Set();
            const extra_matcher = this._extra_matcher;
            delete this._extra_matcher;
            //console.log("LAST_IDS BEFORE", this._name, this._last_ids);
            let last_ids = new Map();
            pipeline(stream, new Writable({
                objectMode: true,
                write: (msg, encoding, cb) => {
                    //console.log("GOTMSG", this._name, msg);
                    let last_id = this._last_ids.get(msg.publisher);
                    if (last_id === undefined) {
                        last_id = minus_one_n;
                    }
                    if (msg.id > last_id) {
                        last_ids.set(msg.publisher, msg.id);
                    }
                    this._message(msg, deferred, extra_matcher, cb);
                }
            }), err => {
                if (this._chkstop()) {
                    return;
                }
                if (err) {
                    return this.emit('error', err);
                }
                //console.log("LAST_IDS UPDATE", this._name, last_ids);
                for (let [publisher, id] of last_ids) {
                    this._last_ids.set(publisher, id);
                }
                //console.log("LAST_IDS AFTER", this._name, this._last_ids);
                this._check_timeout = setTimeout(this._check.bind(this), this._check_delay);
            });
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

    _num_handlers(handlers) {
        return this._do_dedup ? handlers.size : handlers.length;
    }

    _call_handlers2(handlers, info, cb) {
        let called = false;
        let done_err = null;
        let waiting = [];
        const len = this._num_handlers(handlers);

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
            info.expires = info.expires.getTime();
            info.size = info.data.length;

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
                stream.end(info.data);
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
                    handler.call(this, info.data, info, hcb);
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

        const lock = async lock_client => {
            let r = await lock_client.query('SELECT pg_try_advisory_lock($1)', [
                info.id
            ]);
            if (!r.rows[0].pg_try_advisory_lock || this._chkstop()) {
                return false;
            }
            r = await lock_client.query('SELECT EXISTS(SELECT 1 FROM messages WHERE (id = $1 AND (expires > NOW())))', [
                info.id
            ]);
            if (!r.rows[0].exists || this._chkstop()) {
                return false;
            }
            return true;
        };

        (async () => {
            const lock_client = new Client(this._db);
            try {
                await lock_client.connect();
            } catch (ex) {
                return done(ex);
            }
            let deliver;
            try {
                deliver = await lock(lock_client);
            } catch (ex) {
                await lock_client.end();
                return done(ex);
            }
            if (!deliver) {
                await lock_client.end();
                return done();
            }
            deliver_message(lock_client);
        })();
    }

    _call_handlers(handlers, info, cb) {
        if (this._handler_queue) {
            return this._handler_queue.push({ handlers, info, cb });
        }
        this._call_handlers2(handlers, info, cb);
    }

    _handle_message(payload, cb) {
        const handlers = payload.has_extra_handlers ?
            payload.extra_handlers : this._matcher.match(payload.topic);

        this._filter(payload, handlers, (err, ready, handlers) => {
            this._warning(err);
            if (!ready) {
                this._deferred.add(payload.id);
                if (payload.has_extra_handlers) {
                    // Remember handlers for existing messages
                    const em = this._ensure_extra_matcher();
                    em._extra_handlers.set(payload.id, payload.extra_handlers);
                    em._matcher_markers.set(payload.id, payload.matcher_marker);
                }
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
            if (this._num_handlers(handlers) === 0) {
                return cb();
            }
            this._call_handlers(handlers, payload, cb);
        });
    }

    _set_extra(payload, extra_matcher, prev_extra_handlers) {
        let extra_handlers = extra_matcher.match(payload.topic);
        let has_extra_handlers = this._num_handlers(extra_handlers) > 0;
        if (prev_extra_handlers) {
            // We had handlers to receive this as an existing message
            if ((extra_matcher._matcher_markers.get(payload.id) ===
                 this._matcher_marker) &&
                !has_extra_handlers) {
                // No unsubscribe happened and no new handlers
                extra_handlers = prev_extra_handlers;
                has_extra_handlers = true;
            } else {
                // Add previous handlers to new ones
                const handlers = this._matcher.match(payload.topic);
                for (let h of prev_extra_handlers) {
                    if (this._do_dedup) {
                        if (handlers.has(h)) {
                            extra_handlers.add(h);
                        }
                    } else if (handlers.indexOf(h) >= 0) {
                        extra_handlers.push(h);
                    }
                }
                has_extra_handlers = this._num_handlers(extra_handlers) > 0;
            }
            extra_matcher._extra_handlers.delete(payload.id);
            extra_matcher._matcher_markers.delete(payload.id);
        }
        // We'll call handlers for existing message
        payload.extra_handlers = extra_handlers;
        payload.has_extra_handlers = has_extra_handlers;
        // If any message is delayed by filter, remember marker so we know if
        // any unsubscribe happened
        payload.matcher_marker = this._matcher_marker;
    }

    _message(payload, deferred, extra_matcher, cb) {
        if (payload.expires <= Date.now()) {
            return cb();
        }

        let last_id = this._last_ids.get(payload.publisher);
        if (last_id === undefined) {
            last_id = minus_one_n;
        }

        if (payload.id <= last_id) {
            payload.existing = true;
        }

        const is_deferred = deferred.has(payload.id);

        if (payload.existing && extra_matcher && !payload.single) {
            const prev_extra_handlers =
                extra_matcher._extra_handlers.get(payload.id);

            if (prev_extra_handlers || !is_deferred) {
                this._set_extra(payload, extra_matcher, prev_extra_handlers);

                if (!payload.has_extra_handlers) {
                    return cb();
                }
            }
        } else if (!is_deferred) {
            if (payload.existing) {
                if (!payload.single || !this._do_single) {
                    return cb();
                }
            } else if (payload.single && !this._do_single) {
                return cb();
            }
        }

        this._message_queue.push({ payload, cb });
    }

    /**
     * Stop checking for new messages.
     *
     * @param {Function} [cb] - Optional function to call once access to the
     *     database has stopped. Alternatively, you can listen for the
     *     {@link QlobberPG#stop} event.
     */
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

    /**
     * Same as {@link QlobberPG#stop}.
     */
    stop_watching(cb) { // qlobber-fsq compatibility
        this.stop(cb);
    }

    _ltreeify(topic) {
        const t = topic.split('.');
        const r = [];
        let asterisks = 0;
        for (let i = 0; i < t.length; ++i) {
            const l = t[i];
            switch (l) {
            case '*':
                ++asterisks;
                if (t[i+1] !== '*') {
                    r.push(`*{${asterisks}}`);
                    asterisks = 0;
                }
                break;
            case '#':
                if (t[i+1] !== '#') {
                    r.push('*');
                }
                break;
            default:
                r.push(l);
                break;
            }
        }
        return r.join('.');
    }

    _or_topics(topics) {
        return `(${Array.from(topics.keys()).map(t => {
            if (t === '') {
                return "(NEW.topic = '')";
            }
            return escape('(NEW.topic ~ %L)', this._ltreeify(t));
        }).join(' OR ')})`;
    }

    _maybe_or(s, t) {
        return s ? `(${s} OR ${t})` : t;
    }

    _topics_test(check) {
        const topics = this._topics;
        const extra_topics = this._extra_matcher &&
                             this._extra_matcher._extra_topics;
        if ((topics.size === 0) &&
            (!check || ((this._deferred.size === 0) &&
                        (!extra_topics || extra_topics.size === 0)))) {
            return null;
        }
        let r = '';
        if (topics.size > 0) {
            r = this._or_topics(topics);
            if (check) {
                const last_ids = Array.from(this._last_ids);
                if (last_ids.length > 0) {
                    r = `(${r} AND (${last_ids.map(([publisher, id]) => escape('((NEW.publisher = %L) AND (NEW.id > %s))', publisher, id)).join(' OR ')} OR (${last_ids.map(([publisher]) => escape('(NEW.publisher != %L)', publisher)).join(' AND ')}) OR NEW.single))`;
                }
            }
        }
        if (check && (this._deferred.size > 0)) {
            r = this._maybe_or(r, `(${Array.from(this._deferred).map(id => escape('(NEW.id = %L)', String(id))).join(' OR ')})`);
        }
        if (check && extra_topics && (extra_topics.size > 0)) {
            r = this._maybe_or(r, this._or_topics(extra_topics));
        }
        return `(${r} AND (NEW.expires > NOW()))`;
    }
        
    _update_trigger(cb) {
        if (!this._notify) {
            return cb();
        }
        this._queue.push(asyncify(async () => {
            if (this._chkstop()) {
                throw new Error('stopped');
            }
            await this._client.query('SELECT pg_advisory_lock($1)', [
                global_lock
            ]);
            try {
                await this._client.query(escape('DROP TRIGGER IF EXISTS %I ON messages', this._name));
                const test = this._topics_test(false);
                if (test) {
                    await this._client.query(escape(`CREATE TRIGGER %I AFTER INSERT ON messages FOR EACH ROW WHEN ${test} EXECUTE PROCEDURE new_message(%I)`, this._name, this._name));
                }
            } finally {
                await this._client.query('SELECT pg_advisory_unlock($1)', [
                    global_lock
                ]);
            }
        }), cb);
    }

    _valid_topic_length(topic, cb) {
        if (topic.length > 255) {
            cb(new Error(`topic too long: ${topic}`));
            return false;
        }
        return true;
    }

    _valid_stopic(topic, cb) {
        if (!this._valid_topic_length(topic, cb)) {
            return false;
        }
        if (topic === '') {
            return true;
        }
        for (let label of topic.split('.')) {
            if ((label !== '*') &&
                (label !== '#') &&
                !/^[A-Za-z0-9_]+$/.test(label)) {
                cb(new Error(`invalid subscription topic: ${topic}`));
                return false;
            }
        }
        return true;
    }

    _valid_ptopic(topic, cb) {
        if (!this._valid_topic_length(topic, cb)) {
            return false;
        }
        if (topic === '') {
            return true;
        }
        for (let label of topic.split('.')) {
            if (!/^[A-Za-z0-9_]+$/.test(label)) {
                cb(new Error(`invalid publication topic: ${topic}`));
                return false;
            }
        }
        return true;
    }

    _ensure_extra_matcher() {
        if (!this._extra_matcher) {
            const extra_topics = new Map();

            const matcher_options = Object.assign(this._matcher_options, {
                cache_adds: extra_topics
            });

            if (this._do_dedup) {
                this._extra_matcher = new QlobberDedup(matcher_options);
            } else {
                this._extra_matcher = new Qlobber(matcher_options);
            }

            this._extra_matcher._extra_topics = extra_topics;
            this._extra_matcher._extra_handlers = new Map();
            this._extra_matcher._matcher_markers = new Map();
        }

        return this._extra_matcher;
    }

    /**
     * Subscribe to messages in the PostgreSQL queue.
     *
     * @param {String} topic - Which messages you're interested in receiving.
     * Message topics are split into words using `.` as the separator. You can
     * use `*` to match exactly one word in a topic or `#` to match zero or more
     * words. For example, `foo.*` would match `foo.bar` whereas `foo.#` would
     * match `foo`, `foo.bar` and `foo.bar.wup`. Note you can change the
     * separator and wildcard characters by specifying the `separator`,
     * `wildcard_one` and `wildcard_some` options when
     * {@link QlobberPG|constructing} `QlobberPG` objects. See the [`qlobber`
     * documentation](https://github.com/davedoesdev/qlobber#qlobberoptions)
     * for more information. Valid characters in `topic` are: `A-Za-z0-9_*#.`

     * @param {Function} handler - Function to call when a new message is
     * received on the PostgreSQL queue and its topic matches against `topic`.
     * `handler` will be passed the following arguments:
     * - **`data`** ([`Readable`](http://nodejs.org/api/stream.html#stream_class_stream_readable) | [`Buffer`](http://nodejs.org/api/buffer.html#buffer_class_buffer))
     *   Message payload as a Readable stream or a Buffer.
     *   By default you'll receive a Buffer. If `handler` has a property
     *   `accept_stream` set to a truthy value then you'll receive a stream.
     *   Note that _all_ subscribers will receive the same stream or content for
     *   each message. You should take this into account when reading from the
     *   stream. The stream can be piped into multiple
     *   [Writable](http://nodejs.org/api/stream.html#stream_class_stream_writable)
     *   streams but bear in mind it will go at the rate of the slowest one.
     * - **`info`** (`Object`) Metadata for the message, with the following
     *   properties:
     *   - **`topic`** (`String`)  Topic the message was published with.
     *   - **`expires`** (`Integer`) When the message expires (number of
     *     milliseconds after January 1970 00:00:00 UTC).
     *   - **`single`** (`Boolean`) Whether this message is being given to at
     *     most one subscriber (across all `QlobberPG` instances).
     *   - **`size`** (`Integer`) Message size in bytes.
     *   - **`publisher`** (`String`) Name of the `QlobberPG` instance which
     *     published the message.
     *  - **`done`** (`Function`) Function to call one you've handled the
     *    message. Note that calling this function is only mandatory if
     *    `info.single === true`, in order to delete and unlock the message
     *    row in the database table. `done` takes two arguments:
     *    - **`err`** (`Object`) If an error occurred then pass details of the
     *      error, otherwise pass `null` or `undefined`.
     *    - **`finish`** (`Function`) Optional function to call once the message
     *      has been deleted and unlocked, in the case of
     *      `info.single === true`, or straight away otherwise. It will be
     *      passed the following argument:
     *      - **`err`** (`Object`) If an error occurred then details of the
     *        error, otherwise `null`.
     *
     * @param {Object} [options] - Optional settings for this subscription.
     * @param {Boolean} [options.subscribe_to_existing=false] - If `true` then
     * `handler` will be called with any existing, unexpired messages that
     * match `topic`, as well as new ones. If `false` (the default) then
     * `handler` will be called with new messages only.
     *
     * @param {Function} [cb] - Optional function to call once the subscription
     * has been registered. This will be passed the following argument:
     * - **`err`** (`Object`) If an error occurred then details of the error,
     *   otherwise `null`.
     */
    subscribe(topic, handler, options, cb) {
        if (typeof options === 'function') {
            cb = options;
            options = undefined;
        }

        //console.log("SUBSCRIBE", this._name, topic);

        options = options || {};
        const cb2 = (err, ...args) => {
            this._warning(err);
            if (cb) {
                cb.call(this, err, ...args);
            }
        };

        if (!this._valid_stopic(topic, cb2)) {
            return;
        }

        this._matcher.add(topic, handler);

        if (options.subscribe_to_existing) {
            this._ensure_extra_matcher().add(topic, handler);
        }

        this._update_trigger(cb2);
    }

    /**
     * Unsubscribe from messages in the PostgreSQL queue.
     *
     * @param {String} [topic] - Which messages you're no longer interested in
     *     receiving via the `handler` function. This should be a topic you've
     *     previously passed to {@link QlobberPG#subscribe}. If `topic` is
     *     `undefined` then all handlers for all topics are unsubscribed.
     *
     * @param {Function} [handler] - The function you no longer want to be
     *     called with messages published to the topic `topic`. This should be a
     *     function you've previously passed to {@link QlobberPG#subscribe}.
     *     If you subscribed `handler` to a different topic then it will still
     *     be called for messages which match that topic. If `handler` is
     *     `undefined`, all handlers for the topic `topic` are unsubscribed.
     *
     * @param {Function} [cb] - Optional function to call once `handler` has
     *     been unsubscribed from `topic`. This will be passed the following
     *     argument:
     * - **`err`** (`Object`)  If an error occurred then details of the error,
     *   otherwise `null`.
     */
    unsubscribe(topic, handler, cb) {
        //console.log("UNSUB", topic, handler, cb);

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
            if (this._extra_matcher) {
                this._extra_matcher.clear();
            }
        } else {
            if (!this._valid_stopic(topic, cb2)) {
                return;
            }
            if (handler === undefined) {
                this._matcher.remove(topic);
                if (this._extra_matcher) {
                    this._extra_matcher.remove(topic);
                }
            } else {
                this._matcher.remove(topic, handler);
                if (this._extra_matcher) {
                    this._extra_matcher.remove(topic, handler);
                }
            }
        }

        this._matcher_marker = {};

        this._update_trigger(cb2);
    }

    /**
     * Publish a message to the PostgreSQL queue.
     *
     * @param {String} topic - Message topic. The topic should be a series of
     *     words separated by `.` (or the `separator` character you passed to
     *     the {@link QlobberPG|constructor}). Valid characters in `topic` are:
     *     `A-Za-z0-9_.`
     *
     * @param {String|Buffer} payload - Message payload. If you don't pass a
     *     payload then `publish` will return a [`Writable`](http://nodejs.org/api/stream.html#stream_class_stream_writable)
     *     for you to write the payload info.
     *
     * @param {Object} options - Optional settings for this publication.
     * @param {Boolean} [options.single=false] - If `true` then the message
     *     will be given to _at most_ one interested subscriber, across all
     *     `QlobberPG` instances querying the PostgreSQL queue. Otherwise all
     *     interested subscribers will receive the message (the default).
     * @param {Integer} [options.ttl] - Time-to-live (in milliseconds) for this
     *     message. If you don't specify anything then `single_ttl` or
     *     `multi_ttl` (provided to the {@link QlobberPG|constructor}) will be
     *     used, depending on the value of `single`. After the time-to-live
     *     for the message has passed, the message is ignored and deleted when
     *     convenient.
     * @param {String} [options.encoding="utf8"] - If `payload` is a string,
     *     the encoding to use when writing to the database.
     *
     * @param {Function} [cb] - Optional function to call once the message has
     *     been written to the database. It will be passed the following
     *     arguments:
     * - **`err`** (`Object`) If an error occurred then details of the error,
     *   otherwise `null`.
     * - **`info`** (`Object`) Metadata for the message. See
     *   {@link QlobberPG#subscribe} for a description of `info`'s properties.
     *
     * @return {Stream|undefined} - A [`Writable`](http://nodejs.org/api/stream.html#stream_class_stream_writable)
     *     if no `payload` was passed, otherwise `undefined`.
     */
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

        if (!this._valid_ptopic(topic, cb2)) {
            return;
        }

        const now = Date.now();
        const expires = now + (options.ttl || (options.single ? this._single_ttl : this._multi_ttl));
        const single = !!options.single;

        const insert = data => {
            if (this._chkstop()) {
                return cb2(new Error('stopped'));
            }
            //console.log("PUBLISHING", this._name, topic, single);
            // Note: ltree will validate topic (maxlen 255) to A-Za-z0-9_
            this._queue.push(cb => {
                if (this._chkstop()) {
                    return cb(new Error('stopped'));
                }
                this._client.query("INSERT INTO messages(topic, expires, single, data, publisher) VALUES($1, $2, $3, decode($4::text, 'hex'), $5)", [
                    topic,
                    new Date(expires),
                    single,
                    data.toString('hex'),
                    this._name
                ], cb);
            }, iferr(cb2, () => {
                //console.log("PUBLISHED", this._name, topic, single);
                cb2(null, {
                    topic,
                    expires,
                    single,
                    size: data.length,
                    publisher: this._name
                });
            }));
        };

        if (Buffer.isBuffer(payload)) {
            return insert(payload);
        }

        if (typeof payload === 'string') {
            return insert(Buffer.from(payload, options.encoding || 'utf8'));
        }

        const s = new CollectStream();

        s.once('buffer', insert);

        s.once('error', function (err) {
            let called = false;
            function done() {
                if (!called) {
                    called = true;
                    cb2(err);
                }
            }
            this.removeListener('buffer', insert);
            this.once('close', done);
            this.once('finish', done);
            this.end();
        });

        return s;
    }
}

exports.QlobberPG = QlobberPG;
