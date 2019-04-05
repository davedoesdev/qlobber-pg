const { Writable } = require('stream');
const { EventEmitter } = require('events');
const { Client } = require('pg');
const { queue, asyncify } = require('async');
const iferr = require('iferr');
const { Qlobber, QlobberDedup } = require('qlobber');

// TODO:
// Call handlers inc filter
// Single messages
// Doc that name will go into SQL

class CollectStream extends Writable {
    constructor() {
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
        this._dodedup = options.dedup === undefined ? true : options.dedup;
        this._topics = new Map();

        const qoptions = Object.assign({
            cache_adds: this._topics
        }, options);

        if (this._dodedup) {
            this._matcher = new QlobberDedup(qoptions);
        } else {
            this._matcher = new Qlobber(qoptions);
        }
    }

    _warning(err) {
        if (err) {
            this.emit('warning', err);
        }
    }

    _expire() {
        this._queue.push(cb => {
            this._client.query('DELETE FROM messages WHERE expires <= NOW()', cb);
        }, err => {
            this._warning(err);
            this._expire_timeout = setTimeout(this._expire.bind(this), this._expire_interval);
        });
    }

    _message(msg) {
        const payload = JSON.parse(msg.payload);
        const handlers = this._matcher.match(payload.topic);
        console.log(payload, handlers);
    }

    start(cb) {
        if (this._client) {
            return cb();
        }
        this._client = new Client(this._db);
        this._queue = queue((task, cb) => task(cb));
        this._client.connect(iferr(cb, () => {
            this._client.on('notification', this._message.bind(this));
            this._client.query('LISTEN new_message', iferr(cb, () => {
                this._expire();
                cb();
            }));
        }));
    }

    stop(cb) {
        const client = this._client;
        delete this._client;
        if (!client) {
            return cb();
        }
        if (this._expire_timeout) {
            clearTimeout(this._expire_timeout);
            delete this._expire_timeout;
        }
        client.end(cb);
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

    _update_trigger(cb) {
        this._in_transaction(cb => {
            this._queue.unshift(asyncify(async () => {
                await this._client.query('DROP TRIGGER IF EXISTS ' + this._name + ' ON messages');
                await this._client.query('CREATE TRIGGER ' + this._name + ' AFTER INSERT ON messages FOR EACH ROW WHEN ((NEW.expires > NOW()) AND (' + Array.from(this._topics.keys()).map(t => "NEW.topic ~ '" + t +"'").join(' OR ') + ')) EXECUTE PROCEDURE new_message()');
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
