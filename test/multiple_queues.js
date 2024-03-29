'use strict';
const { randomBytes } = require('crypto');
const { timesSeries, each, timesLimit } = require('async');
const { QlobberPG } = require('..');
const config = require('config');
const iferr = require('iferr');

function sum(buf) {
    let r = 0;
    for (let b of buf) {
        r += b;
    }
    return r;
}

describe('multiple queues', function () {
    let expect;
    before(async () => {
        ({ expect } = await import('chai'));
    });

    const timeout = 20 * 60 * 1000;
    this.timeout(timeout);

    let interval_handle;

    beforeEach(function () {
        interval_handle = setInterval(function () {
            console.log('still alive'); // eslint-disable-line no-console
        }, 60 * 1000);
    });

    afterEach(function () {
        clearInterval(interval_handle);
    });

    function publish_to_queues(name, num_queues, num_messages, max_message_size, get_single) {
        it(`should publish to multiple queues (${name}, num_queues=${num_queues}, num_messages=${num_messages}, max_message_size=${max_message_size})`, function (done) {
            let num_single = 0;
            let num_multi = 0;
            let count_single = 0;
            let count_multi = 0;
            let the_qpgs;
            let checksum = 0;
            let expected_checksum = 0;
            let length = 0;
            let expected_length = 0;

            timesSeries(num_queues, (n, cb) => {
                const qpg = new QlobberPG(Object.assign({
                    name: `test${n}`
                }, config));

                let qcount = 0;

                function subscribe() {
                    qpg.subscribe('foo', function (data, info, cb) {
                        expect(info.topic).to.equal('foo');

                        if (typeof get_single === 'boolean') {
                            expect(info.single).to.equal(get_single);
                        }
                        
                        checksum += sum(data);
                        length += data.length;

                        ++qcount;

                        if (get_single === false) {
                            expect(qcount).to.be.at.most(num_multi);
                        }

                        if (info.single) {
                            ++count_single;
                        } else {
                            ++count_multi;
                        }

                        //console.log('MSG', count_single, num_single, count_multi, num_multi * num_queues);

                        if (the_qpgs &&
                            (count_single === num_single) &&
                            (count_multi === num_multi * num_queues)) {
                            process.nextTick(() => {
                                each(the_qpgs, (qpg, cb) => qpg.stop(cb), iferr(done, () => {
                                    expect(length).to.be.above(0);
                                    expect(length).to.equal(expected_length);
                                    expect(checksum).to.be.above(0);
                                    expect(checksum).to.equal(expected_checksum);
                                    done();
                                }));
                            });
                            cb();
                        } else if (count_single > num_single) {
                            done(new Error('single called too many times'));
                        } else if (count_multi > num_multi * num_queues) {
                            done(new Error('multi called too many times'));
                        } else {
                            cb();
                        }
                    });

                    cb(null, qpg);
                }

                qpg.on('start', () => {
                    if (n === 0) {
                        return qpg._queue.push(cb => {
                            qpg._client.query('DELETE FROM messages', cb);
                        }, iferr(cb, subscribe));
                    }
                    subscribe();
                });
            }, iferr(done, qpgs => {
                expect(qpgs.length).to.equal(num_queues);

                timesLimit(num_messages, 50, (n, cb) => {
                    const single = typeof get_single === 'boolean' ? get_single : get_single();
                    const data = randomBytes(Math.round(Math.random() * max_message_size));
                    const check = sum(data);

                    if (single) {
                        ++num_single;
                        expected_checksum += check;
                        expected_length += data.length;
                    } else {
                        ++num_multi;
                        expected_checksum += check * num_queues;
                        expected_length += data.length * num_queues;
                    }

                    const q = Math.floor(Math.random() * num_queues);

                    qpgs[q].publish('foo', data, {
                        ttl: timeout,
                        single
                    }, cb);
                }, iferr(done, () => {
                    expect(num_single + num_multi).to.equal(num_messages);

                    //console.log('PUBLISHED');

                    if ((count_single === num_single) &&
                        (count_multi === num_multi * num_queues)) {
                        each(qpgs, (qpg, cb) => qpg.stop(cb), iferr(done, () => {
                            expect(length).to.be.above(0);
                            expect(length).to.equal(expected_length);
                            expect(checksum).to.be.above(0);
                            expect(checksum).to.equal(expected_checksum);
                            done();
                        }));
                    }

                    the_qpgs = qpgs;
                }));
            }));
        });
    }

    function publish_to_queues2(num_queues, num_messages, max_message_size) {
        publish_to_queues('multi', num_queues, num_messages, max_message_size, false);
        publish_to_queues('single', num_queues, num_messages, max_message_size, true);
        publish_to_queues('mixed', num_queues, num_messages, max_message_size, () => {
            return Math.random() < 0.5;
        });
    }

    function publish_to_queues3(num_queues, max_message_size) {
        publish_to_queues2(num_queues, 50, max_message_size);
        publish_to_queues2(num_queues, 500, max_message_size);
        if (!process.env.APPVEYOR) {
            publish_to_queues2(num_queues, 5000, max_message_size);
        }
    }

    publish_to_queues3(1, 200 * 1024);

    if (process.env.NODE_V8_COVERAGE) {
        publish_to_queues3(5, 100 * 1024);
    } else {
        publish_to_queues3(10, 100 * 1024);
        if (!process.env.CI) {
            publish_to_queues3(50, 20 * 1024);
        }
    }
});
