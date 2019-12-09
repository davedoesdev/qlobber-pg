'use strict';
const { EventEmitter } = require('events');
const { randomBytes } = require('crypto');
const { fork } = require('child_process');
const path = require('path');
const { cpus } = require('os');
const { times, each, eachLimit, queue, parallel } = require('async');
const { QlobberPG } = require('..');
const { expect } = require('chai');
const { argv } = require('yargs');
const cp_remote = require('cp-remote');
const config = require('config');
const iferr = require('iferr');
const rabbitmq_bindings = require('./fixtures/rabbitmq_bindings.js');

function topic_sort(a, b) {
    return parseInt(a.substr(1), 10) - parseInt(b.substr(1), 10);
}

function sum(buf) {
    let r = 0;
    for (let b of buf) {
        r += b;
    }
    return r;
}

function rabbitmq_tests(name, QCons, num_queues, rounds, msglen, retry_prob, expected, f) {
    it('should pass rabbitmq tests (' + name + ', num_queues=' + num_queues + ', rounds=' + rounds + ', msglen=' + msglen + ', retry_prob=' + retry_prob + ')', function (done) {
        const timeout = 20 * 60 * 1000;
        this.timeout(timeout);

        let total = 0;
        let expected2 = {};
        for (let e of expected) {
            total += e[1].length * rounds;
            expected2[e[0]] = [];
            for (let i = 0; i < rounds; ++i) {
                expected2[e[0]] = expected2[e[0]].concat(e[1]);
            }
            expected2[e[0]].sort(topic_sort);
        }

        let expected_result_single = [];
        for (let i = 0; i < rounds; ++i) {
            expected_result_single = expected_result_single.concat(
                Object.keys(expected2));
        }
        expected_result_single.sort();

        let subs = [];
        let single_sum = 0;
        let expected_single_sum = 0;
        let result_single = [];
        let count_single = 0;
        let result = {};
        let sums = {};
        let expected_sums = {};
        let count = 0;
        function received(qpgs, n, topic, value, data, single) {
            expect(subs[n][value], value).to.be.true;

            if (single) {
                expect(expected2[topic]).to.contain(value);
                single_sum += Buffer.isBuffer(data) ? sum(data) : data;
                result_single.push(topic);
                ++count_single;
            } else {
                result[topic] = result[topic] || [];
                result[topic].push(value);

                sums[topic] = sums[topic] || 0;
                sums[topic] += Buffer.isBuffer(data) ? sum(data) : data;

                ++count;
            }

            //console.log(result);
            //console.log(count, total, count_single, expected.length * rounds);

            if ((count === total) &&
                (count_single === expected.length * rounds)) {
                // wait a bit to catch duplicates
                return setTimeout(() => {
                    result_single.sort();
                    expect(result_single).to.eql(expected_result_single);
                    expect(single_sum).to.equal(expected_single_sum);

                    for (let t in result) {
                        if (Object.prototype.hasOwnProperty.call(result, t)) {
                            result[t].sort(topic_sort);
                        }
                    }
                    expect(result).to.eql(expected2);
                    expect(sums).to.eql(expected_sums);

                    each(qpgs, (qpg, next) => qpg.stop(next), done);
                }, 10 * 1000);
            }

            if (count_single > expected.length * rounds) {
                return done(new Error('more single messages than expected'));
            }

            if (count > total) {
                return done(new Error('more messages than expected total'));
            }
        }

        function subscribe(qpgs, n, topic, value, cb) {
            const qpg = qpgs[n];

            function handler(data, info, cb) {
                if (info.single && (Math.random() < retry_prob)) {
                    return cb('dummy retry');
                }
                
                received(qpgs, n, info.topic, value, data, info.single);
                cb();
            }

            qpg.subscribe(topic, handler, iferr(cb, () => {
                qpg.__submap = qpg.__submap || {};
                qpg.__submap[value] = handler;
                cb();
            }));
        }

        function unsubscribe(qpg, topic, value, cb) {
            //console.log("UNSUBSCRIBE", qpg._name, topic, value);
            if (value) {
                //console.log("UNSUBSCRIBE2", qpg._name, topic, value, qpg.__submap[value]);
                return qpg.unsubscribe(topic, qpg.__submap[value], iferr(cb, () => {
                    delete qpg.__submap[value];
                    cb();
                }));
            }

            qpg.unsubscribe(topic, value, cb);
        }

        times(num_queues, function (n, cb) {
            const qpg = new QCons(Object.assign({
                name: `test${n}`
            }, config), num_queues, n);

            qpg.on('start', iferr(cb, () => {
                if ((n === 0) && (QCons === QlobberPG)) {
                    return qpg._queue.push(cb => {
                        qpg._client.query('DELETE FROM messages', cb);
                    }, err => cb(err, qpg));
                }
                cb(null, qpg);
            }));
        }, iferr(done, qpgs => {
            function publish() {
                const pq = queue((task, cb) => {
                    const buf = randomBytes(msglen);
                    const s = sum(buf);
                    
                    expected_sums[task] = expected_sums[task] || 0;

                    for (let i = 0; i < expected2[task].length / rounds; ++i) {
                        expected_sums[task] += s;
                    }

                    expected_single_sum += s;

                    parallel([cb => {
                        qpgs[Math.floor(Math.random() * num_queues)].publish(
                            task,
                            buf,
                            { ttl: timeout },
                            cb);
                    }, cb => {
                        qpgs[Math.floor(Math.random() * num_queues)].publish(
                            task,
                            buf,
                            { ttl: timeout, single: true },
                            cb);
                    }], cb);
                }, num_queues * 5);

                for (let i = 0; i < rounds; ++i) {
                    for (let e of expected) {
                        pq.push(e[0], iferr(done, () => {}));
                    }
                }

                if (Object.keys(subs).length === 0) {
                    pq.drain(() => {
                        setTimeout(() => {
                            expect(count).to.equal(0);
                            expect(count_single).to.equal(0);
                            each(qpgs, (qpg, next) => qpg.stop(next), done);
                        }, 10 * 1000);
                    });
                }
            }
            
            let assigned = {};

            const q = queue((i, cb) => {
                const n = Math.floor(Math.random() * num_queues);
                const entry = rabbitmq_bindings.test_bindings[i];

                subs[n] = subs[n] || {};
                subs[n][entry[1]] = true;
                assigned[i] = n;
                assigned[entry[0]] = assigned[entry[0]] || [];
                assigned[entry[0]].push({ n, v: entry[1] });

                subscribe(qpgs, n, entry[0], entry[1], cb);
            }, num_queues * 5);

            for (let i = 0; i < rabbitmq_bindings.test_bindings.length; ++i) {
                q.push(i, iferr(done, () => {}));
            }

            q.drain(() => {
                if (f) {
                    f(qpgs, subs, assigned, unsubscribe, iferr(done, publish));
                } else {
                    publish();
                }
            });
        }));
    });
}

function rabbitmq(prefix, QCons, queues, rounds, msglen, retry_prob) {
    prefix += ', ';

    rabbitmq_tests(prefix + 'before remove', QCons, queues, rounds, msglen, retry_prob, rabbitmq_bindings.expected_results_before_remove);

    rabbitmq_tests(prefix + 'after remove', QCons, queues, rounds, msglen, retry_prob, rabbitmq_bindings.expected_results_after_remove, (qpgs, subs, assigned, unsubscribe, cb) => {
        eachLimit(rabbitmq_bindings.bindings_to_remove, qpgs.length * 5, (i, next) => {
            const n = assigned[i - 1];
            const v = rabbitmq_bindings.test_bindings[i - 1][1];

            unsubscribe(qpgs[n], rabbitmq_bindings.test_bindings[i - 1][0], v, iferr(next, () => {
                assigned[i - 1] = null;
                subs[n][v] = null;
                next();
            }));
        }, cb);
    });

    rabbitmq_tests(prefix + 'after remove_all', QCons, queues, rounds, msglen, retry_prob, rabbitmq_bindings.expected_results_after_remove_all, (qpgs, subs, assigned, unsubscribe, cb) => {
        eachLimit(rabbitmq_bindings.bindings_to_remove, qpgs.length * 5, (i, next) => {
            const topic = rabbitmq_bindings.test_bindings[i - 1][0];

            eachLimit(assigned[topic], qpgs.length * 5, (nv, next2) => {
                unsubscribe(qpgs[nv.n], topic, nv.v, iferr(next, () => {
                    subs[nv.n][nv.v] = null;
                    next2();
                }));
            }, iferr(next, () => {
                assigned[i - 1] = null;
                assigned[topic] = [];
                next();
            }));
        }, cb);
    });

    rabbitmq_tests(prefix + 'after clear', QCons, queues, rounds, msglen, retry_prob, rabbitmq_bindings.expected_results_after_clear, (qpgs, subs, assigned, unsubscribe, cb) => {
        each(qpgs, (qpg, next) => {
            unsubscribe(qpg, undefined, undefined, next);
        }, iferr(cb, () => {
            subs.length = 0;
            cb();
        }));
    });
}

function rabbitmq2(prefix, QCons, queues, rounds, msglen) {
    rabbitmq(prefix, QCons, queues, rounds, msglen, 0);
    rabbitmq(prefix, QCons, queues, rounds, msglen, 0.5);
}

function rabbitmq3(prefix, QCons, queues, rounds) {
    rabbitmq2(prefix, QCons, queues, rounds, 1);
    rabbitmq2(prefix, QCons, queues, rounds, 25 * 1024);
}

function rabbitmq4(prefix, QCons, queues) {
    rabbitmq3(prefix, QCons, queues, 1);
    rabbitmq3(prefix, QCons, queues, 50);
}

class MPQPGBase extends EventEmitter {
    constructor(child, options) {
        super();

        this._name = options.name;

        this._handlers = {};
        this._handler_count = 0;
        this._pub_cbs = {};
        this._pub_cb_count = 0;
        this._sub_cbs = {};
        this._sub_cb_count = 0;
        this._unsub_cbs = {};
        this._unsub_cb_count = 0;
        this._topics = {};
        this._send_queue = queue((msg, cb) => child.send(msg, cb));

        child.on('error', err => this.emit('error', err));
        child.on('exit', () => this.emit('stop'));

        child.on('message', msg => {
            //console.log("RECEIVED MESSAGE FROM CHILD", options.index, msg);
            if (msg.type === 'start') {
                this.emit('start', msg.err);
            } else if (msg.type === 'stop') {
                this._send_queue.push({ type: 'exit' });
            } else if (msg.type === 'received') {
                this._handlers[msg.handler](msg.sum, msg.info, err => {
                    this._send_queue.push({
                        type: 'recv_callback',
                        cb: msg.cb,
                        err
                    });
                });
            } else if (msg.type === 'sub_callback') {
                const cb = this._sub_cbs[msg.cb];
                delete this._sub_cbs[msg.cb];
                cb();
            } else if (msg.type === 'unsub_callback') {
                const cb = this._unsub_cbs[msg.cb];
                delete this._unsub_cbs[msg.cb];
                cb();
            } else if (msg.type === 'pub_callback') {
                const cb = this._pub_cbs[msg.cb];
                delete this._pub_cbs[msg.cb];
                cb(msg.err);
            }
        });
    }

    subscribe(topic, handler, cb) {
        this._handlers[this._handler_count] = handler;
        handler.__count = this._handler_count;

        this._sub_cbs[this._sub_cb_count] = cb;

        this._topics[topic] = this._topics[topic] || {};
        this._topics[topic][this._handler_count] = true;

        this._send_queue.push({
            type: 'subscribe',
            topic,
            handler: this._handler_count,
            cb: this._sub_cb_count
        });

        ++this._handler_count;
        ++this._sub_cb_count;
    }

    unsubscribe(topic, handler, cb) {
        if (topic === undefined) {
            this._unsub_cbs[this._unsub_cb_count] = () => {
                this._handlers = {};
                this._topics = {};
                cb();
            };

            this._send_queue.push({
                type: 'unsubscribe',
                cb: this._unsub_cb_count
            });

            ++this._unsub_cb_count;
        } else if (handler === undefined) {
            let n = this._topics[topic].length;

            for (let h of this._topics[topic]) {
                this._unsub_cbs[this._unsub_cb_count] = () => {
                    delete this._handlers[h];
                    if (--n === 0) {
                        delete this._topics[topic];
                        cb();
                    }
                };

                this._send_queue.push({
                    type: 'unsubscribe',
                    topic,
                    handler: h,
                    cb: this._unsub_cb_count
                });

                ++this._unsub_cb_count;
            }
        } else {
            this._unsub_cbs[this._unsub_cb_count] = () => {
                delete this._handlers[handler.__count];
                cb();
            };

            this._send_queue.push({
                type: 'unsubscribe',
                topic,
                handler: handler.__count,
                cb: this._unsub_cb_count
            });

            ++this._unsub_cb_count;
        }
    }

    publish(topic, payload, options, cb) {
        this._pub_cbs[this._pub_cb_count] = cb;

        this._send_queue.push({
            type: 'publish',
            topic,
            payload: payload.toString('base64'),
            options,
            cb: this._pub_cb_count
        });

        ++this._pub_cb_count;
    }

    stop(cb) {
        this._send_queue.push({
            type: 'stop'
        });

        if (cb) {
            this.once('stop', cb);
        }
    }
}

class MPQPG extends MPQPGBase {
    constructor(options, total, index) {
        options = Object.assign({ total, index }, options);
        super(
            fork(
                path.join(__dirname, 'fixtures', 'mpqpg.js'),
                [Buffer.from(JSON.stringify(options)).toString('hex')]),
            options);
    }
}

function make_RemoteMPQPG(hosts) {
    return class extends MPQPGBase {
        constructor(options, total, index) {
            options = Object.assign({ total, index }, options);
            super(
                cp_remote.run(
                    hosts[index],
                    path.join(__dirname, 'fixtures', 'mpqpg.js'),
                    Buffer.from(JSON.stringify(options)).toString('hex')),
                options);
            this._host = hosts[index];
        }
    };
}

describe('rabbit', function () {
    if (argv.remote) {
        let hosts;
        if (argv.remote === true) {
            hosts = ['localhost', 'localhost'];
        } else if (typeof argv.remote === 'string') {
            hosts = [argv.remote];
        } else if (argv.remote[0] === true) {
            hosts = argv.remote.slice(1);
        } else {
            hosts = argv.remote;
        }
        rabbitmq4('distributed', make_RemoteMPQPG(hosts), hosts.length);
    } else if (argv.multi) {
        rabbitmq4('multi-process', MPQPG, argv.queues || cpus().length);
    } else {
        rabbitmq4('single-process', QlobberPG, 1);
        rabbitmq4('single-process', QlobberPG, 2);

        if (!process.env.NYC_CWD) {
            rabbitmq4('single-process', QlobberPG, 5);
        }
    }
});
