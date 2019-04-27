const { randomBytes } = require('crypto');
const { times, each, queue, parallel } = require('async');
const { QlobberPG } = require('..');
const { expect } = require('chai');
const config = require('config');
const iferr = require('iferr');
const rabbitmq_bindings = require('./rabbitmq_bindings');

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

            if ((count === total) &&
                (count_single === expected.length * rounds)) {
                // wait a bit to catch duplicates
                return setTimeout(() => {
                    result_single.sort();
                    expect(result_single).to.eql(expected_result_single);
                    expect(single_sum).to.equal(expected_single_sum);

                    for (let t in result) {
                        if (result.hasOwnProperty(t)) {
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
            if (value) {
                return qpg.unsubscribe(topic, qpg.__submap[value], () => {
                    delete qpg.__submap[value];
                    cb();
                });
            }

            qpg.unsubscribe(topic, value, cb);
        }

        times(num_queues, function (n, cb) {
            const qpg = new QCons(Object.assign({
                name: `test${n}`
            }, config), num_queues, n);
            
            qpg.on('start', () => {
                if (n === 0) {
                    return qpg._queue.push(cb => {
                        qpg._client.query('DELETE FROM messages', cb);
                    }, err => cb(err, qpg));
                }
                cb(null, qpg);
            });
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
                    pq.drain = () => {
                        setTimeout(() => {
                            expect(count).to.equal(0);
                            expect(count_single).to.equal(0);

                            each(qpgs, (qpg, next) => {
                                qpg.stop(next);
                            }, done);
                        }, 10 * 1000);
                    };
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

            q.drain = () => {
                if (f) {
                    f(qpgs, subs, assigned, unsubscribe, publish);
                } else {
                    publish();
                }
            };
        }));
    });
}

function rabbitmq(prefix, QCons, queues, rounds, msglen, retry_prob) {
    prefix += ', ';

    rabbitmq_tests(prefix + 'before remove', QCons, queues, rounds, msglen, retry_prob, rabbitmq_bindings.expected_results_before_remove);

}

function rabbitmq2(prefix, QCons, queues, rounds, msglen) {
    rabbitmq(prefix, QCons, queues, rounds, msglen, 0);
    rabbitmq(prefix, QCons, queues, rounds, msglen, 0.5);
}

function rabbitmq3(prefix, QCons, queues, rounds) {
    rabbitmq2(prefix, QCons, queues, rounds, 1);
    //rabbitmq2(prefix, QCons, queues, rounds, 25 * 1024);
}

function rabbitmq4(prefix, QCons, queues) {
    rabbitmq3(prefix, QCons, queues, 1);
    //rabbitmq3(prefix, QCons, queues, 50);
}

describe('rabbit', function ()
{
    rabbitmq4('single-process', QlobberPG, 1);
    //rabbitmq4('single-process', QlobberPG, 2);
});
