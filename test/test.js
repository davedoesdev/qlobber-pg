'use strict';
const path = require('path');
const { Writable } = require('stream');
const { randomBytes, createHash } = require('crypto');
const { writeFile, createReadStream } = require('fs');
const { parallel, timesSeries, eachSeries, each } = require('async');
const { QlobberPG } = require('..');
const { expect } = require('chai');
const wu = require('wu');
const config = require('config');
const iferr = require('iferr');

const orig_it = it;
it = function(s, f) { // eslint-disable-line no-global-assign
    return orig_it(s, function (done) {
        f.call(this, err => {
            if (err) {
                console.error('DONE', err); // eslint-disable-line no-console
            }
            done(err);
        });
    });
};
it.only = orig_it.only.bind(orig_it);

function read_all(s, cb) {
    const bufs = [];

    s.on('end', () => cb(Buffer.concat(bufs)));

    s.on('readable', function () {
        while (true) { // eslint-disable-line no-constant-condition
            const data = this.read();
            if (data === null) {
                break;
            }
            bufs.push(data);
        }
    });
}

function test(gopts) {
    describe(`qlobber-pq options=${JSON.stringify(gopts)}`, function () {
        let qpg;
        let random_hash;
        let random_path = path.join(__dirname, 'fixtures', 'random');

        function t(topic) {
            if (gopts) {
                if (gopts.separator) {
                    topic = topic.split('.').join(gopts.separator);
                }
                if (gopts.wildcard_one) {
                    topic = topic.split('*').join(gopts.wildcard_one);
                }
                if (gopts.wildcard_some) {
                    topic = topic.split('#').join(gopts.wildcard_some);
                }
            }
            return topic;
        }

        before(function (cb) {
            const buf = randomBytes(1024 * 1024);
            const hash = createHash('sha256');
            hash.update(buf);
            random_hash = hash.digest();
            writeFile(random_path, buf, cb);
        });

        function make_qpg(cb, options) {
            const qpg = new QlobberPG(Object.assign({
                name: 'test1' 
            }, config, gopts, options));
            qpg.on('warning', console.error); // eslint-disable-line no-console
            if (cb) {
                return qpg.on('start', () => cb(null, qpg));
            }
            return qpg;
        }

        function before_each(cb, options) {
            make_qpg(iferr(cb, the_qpg => {
                qpg = the_qpg;
                cb();
            }), options);
        }
        beforeEach(cb => {
            before_each(iferr(cb, () => {
                qpg._queue.push(cb => {
                    qpg._client.query('DELETE FROM messages', cb);
                }, cb);
            }));
        });
        
        function after_each(cb) {
            if (this && this.timeout) {
                this.timeout(5000);
            }
            if (qpg) {
                // We need to wait for tasks to complete otherwise we may
                // get 'stopped' or 'Connection terminated' errors.
                // Note there are separate tests for these cases.
                if ((qpg._queue.running() > 0) || (qpg._queue.length() > 0)) {
                    return setTimeout(() => after_each.call(this, cb), 500);
                }
                qpg.stop(cb);
            } else {
                cb();
            }
        }
        afterEach(after_each);

        function exists(id, cb) {
            qpg._queue.push(cb => qpg._client.query('SELECT EXISTS(SELECT 1 FROM messages WHERE id = $1)', [
                id
            ], cb), iferr(cb, r => cb(null, r.rows[0].exists)));
        }

        function count(qpg, cb) {
            qpg._queue.push(
                cb => qpg._client.query('SELECT id FROM messages', cb),
                iferr(cb, r => cb(null, r.rows.length)));
        }

        it('should subscribe and publish to a simple topic', function (done) {
            let pub_info, sub_info;

            function check() {
                if (pub_info && sub_info) {
                    pub_info.id = sub_info.id;
                    pub_info.data = sub_info.data;
                    expect(pub_info).to.eql(sub_info);
                    done();
                }
            }

            qpg.subscribe('foo', (data, info, cb) => {
                expect(info.topic).to.equal('foo');
                expect(info.single).to.be.false;
                expect(data.toString()).to.equal('bar');
                expect(cb.num_handlers).to.equal(1);
                sub_info = info;
                check();
            }, iferr(done, () => {
                qpg.publish('foo', 'bar', iferr(done, info => {
                    pub_info = info;
                    check();
                }));
            }));
        });

        it('should construct received data only once', function (done) {
            const the_data = { foo: 0.435, bar: 'hello' };
            let called1 = false;
            let called2 = false;
            let received_data;

            qpg.subscribe('test', (data, info, cb) => {
                expect(info.topic).to.equal('test');
                expect(JSON.parse(data)).to.eql(the_data);
                if (received_data) {
                    expect(data === received_data).to.be.true;
                } else {
                    received_data = data;
                }
                called1 = true;
                if (called1 && called2) {
                    cb(null, done);
                } else {
                    cb();
                }
            }, iferr(done, () => {
                qpg.subscribe('test', (data, info, cb) => {
                    expect(info.topic).to.equal('test');
                    expect(JSON.parse(data)).to.eql(the_data);
                    if (received_data) {
                        expect(data === received_data).to.be.true;
                    } else {
                        received_data = data;
                    }
                    called2 = true;
                    if (called1 && called2) {
                        cb(null, done);
                    } else {
                        cb();
                    }
                }, iferr(done, () => {
                    qpg.publish('test', JSON.stringify(the_data), iferr(done, () => {}));
                }));
            }));
        });

        it('should support more than 10 subscribers', function (done) {
            const the_data = { foo: 0.435, bar: 'hello' };
            let counter = 11;
            let received_data;
            let a = [];

            function subscribe(cb) {
                qpg.subscribe('test', (data, info, cb) => {
                    expect(info.topic).to.equal('test');
                    expect(JSON.parse(data)).to.eql(the_data);
                    if (received_data) {
                        expect(data == received_data).to.be.true;
                    } else {
                        received_data = data;
                    }
                    if (--counter === 0) {
                        cb(null, done);
                    } else {
                        cb();
                    }
                }, cb);
            }

            for (let i = counter; i > 0; --i) {
                a.push(subscribe);
            }

            parallel(a, iferr(done, () => {
                qpg.publish('test', JSON.stringify(the_data), iferr(done, () => {}));
            }));
        });

        it('should support more than 10 subscribers with same handler', function (done) {
            after_each(iferr(done, () => {
                before_each(iferr(done, () => {
                    const the_data = { foo: 0.435, bar: 'hello' };
                    let counter = 11;
                    let received_data;
                    let a = [];

                    function handler(data, info, cb) {
                        expect(info.topic).to.equal('test');
                        expect(JSON.parse(data)).to.eql(the_data);
                        if (received_data) {
                            expect(data === received_data).to.be.true;
                        } else {
                            received_data = data;
                        }
                        if (--counter === 0) {
                            cb(null, done);
                        } else {
                            cb();
                        }
                    }

                    function subscribe(cb) {
                        qpg.subscribe('test', handler, cb);
                    }

                    for (let i = counter; i > 0; --i) {
                        a.push(subscribe);
                    }

                    parallel(a, iferr(done, () => {
                        qpg.publish('test', JSON.stringify(the_data), iferr(done, () => {}));
                    }));
                }), {
                    dedup: false
                });
            }));
        });

        it('should subscribe to wildcards', function (done) {
            let count = 0;

            function received() {
                if (++count === 2) {
                    done();
                }
            }

            qpg.subscribe(t('*'), function (data, info) {
                expect(info.topic).to.equal('foo');
                expect(data.toString()).to.equal('bar');
                received();
            }, iferr(done, () => {
                qpg.subscribe(t('#'), function (data, info) {
                    expect(info.topic).to.equal('foo');
                    expect(data.toString()).to.equal('bar');
                    received();
                }, iferr(done, () => {
                    qpg.publish('foo', 'bar', iferr(done, () => {}));
                }));
            }));
        });

        it('should subscribe to wildcards with separator', function (done) {
            let count = 0;

            function received() {
                if (++count === 2) {
                    done();
                }
            }

            qpg.subscribe(t('foo.*'), function (data, info) {
                expect(info.topic).to.equal(t('foo.bar'));
                expect(data.toString()).to.equal('bar');
                received();
            }, iferr(done, () => {
                qpg.subscribe(t('#'), function (data, info) {
                    expect(info.topic).to.equal(t('foo.bar'));
                    expect(data.toString()).to.equal('bar');
                    received();
                }, iferr(done, () => {
                    qpg.publish(t('foo.bar'), 'bar', iferr(done, () => {}));
                }));
            }));
        });

        it('should only call each handler once', function (done) {
            const handler = function (data, info) {
                expect(info.topic).to.equal('foo');
                expect(data.toString()).to.equal('bar');
                done();
            };

            qpg.subscribe(t('*'), handler, iferr(done, () => {
                qpg.subscribe(t('#'), handler, iferr(done, () => {
                    qpg.publish('foo', 'bar', iferr(done, () => {}));    
                }));        
            }));
        });

        it('should be able to disable handler dedup', function (done) {
            after_each(iferr(done, () => {
                before_each(iferr(done, () => {
                    let count_multi = 0;
                    let count_single = 0;

                    function handler(data, info, cb) {
                        expect(info.topic).to.equal('foo');
                        expect(data.toString()).to.equal('bar');
                        cb(null, err => {
                            if (info.single) {
                                ++count_single;
                            } else {
                                ++count_multi;
                            }

                            if ((count_single === 1) && (count_multi === 2)) {
                                return done(err);
                            }

                            if ((count_single > 1) || (count_multi > 2)) {
                                done(new Error('called too many times'));
                            }
                        });
                    }

                    qpg.subscribe(t('*'), handler, iferr(done, () => {
                        qpg.subscribe(t('#'), handler, iferr(done, () => {
                            qpg.publish('foo', 'bar', iferr(done, () => {}));
                            qpg.publish('foo', 'bar', {
                                single: true
                            }, iferr(done, () => {}));
                        }));
                    }));
                }), {
                    dedup: false
                });
            }));
        });

        it('should call all handlers on a topic for pubsub', function (done) {
            let count = 0;

            function received() {
                if (++count === 2) {
                    done();
                }
            }

            function handler(data, info) {
                expect(info.topic).to.equal('foo');
                expect(data.toString()).to.equal('bar');
                received();
            }

            qpg.subscribe('foo', (...args) => handler(...args), iferr(done, () => {
                qpg.subscribe('foo', (...args) => handler(...args), iferr(done, () => {
                    qpg.publish('foo', 'bar', iferr(done, () => {}));
                }));
            }));
        });

        it('should support a work queue', function (done) {
            qpg.subscribe('foo', function (data, info, cb) {
                expect(info.topic).to.equal('foo');
                expect(info.single).to.be.true;
                cb(null, done);
            }, iferr(done, () => {
                qpg.publish('foo', 'bar', { single: true }, iferr(done, () => {}));
            }));
        });

        it('should guard against calling subscribe callback twice', function (done) {
            qpg.subscribe('foo', function (data, info, cb) {
                expect(info.single).to.be.true;
                cb(null, iferr(done, () => {
                    setTimeout(() => cb(null, done), 1000);
                }));
            }, iferr(done, () => {
                qpg.publish('foo', 'bar', { single: true }, iferr(done, () => {}));
            }));
        });

        it('should only give work to one worker', function (done) {
            this.timeout(5000);

            make_qpg(iferr(done, qpg2 => {
                let called = false;

                function handler(data, info, cb) {
                    expect(called).to.be.false;
                    called = true;

                    expect(info.topic).to.equal('foo');
                    expect(info.single).to.be.true;
                    expect(data.toString()).to.equal('bar');

                    setTimeout(
                        () => cb(null, iferr(done, () => qpg2.stop(done))),
                        2000);
                }

                parallel([
                    cb => qpg.subscribe('foo', (...args) => handler(...args), cb),
                    cb => qpg.subscribe('foo', (...args) => handler(...args), cb),
                    cb => qpg2.subscribe('foo', (...args) => handler(...args), cb),
                    cb => qpg2.subscribe('foo', (...args) => handler(...args), cb),
                    cb => qpg.publish('foo', 'bar', { single: true }, cb)
                ]);
            }), { name: 'test2' });
        });

        it('should put work back on the queue', function (done) {
            this.timeout(5000);

            let count = 0;

            qpg.subscribe('foo', function (data, info, cb) {
                if (++count === 1) {
                    return cb('dummy failure');
                }
                cb(null, done);
            }, iferr(done, () => {
                qpg.publish('foo', 'bar', { single: true }, iferr(done, () => {}));
            }));
        });

        it('should allow handlers to refuse work', function (done) {
            function handler1() {
                done(new Error('should not be called'));
            }

            function handler2(data, info, cb) {
                cb(null, done);
            }

            after_each(iferr(done, () => {
                before_each(iferr(done, () => {
                    qpg.subscribe('foo', handler1, iferr(done, () => {
                        qpg.subscribe('foo', handler2, iferr(done, () => {
                            qpg.publish('foo', 'bar', iferr(done, () => {}));
                        }));
                    }));
                }), {
                    filter: function (info, handlers, cb) {
                        expect(info.topic).to.equal('foo');
                        handlers.delete(handler1);
                        cb(null, true, handlers);
                    }
                });
            }));
        });

        it('should be able to set filter by property', function (done) {
            function handler1() {
                done(new Error('should not be called'));
            }

            function handler2(data, info, cb) {
                cb(null, done);
            }

            qpg.filters.push(function (info, handlers, cb) {
                expect(info.topic).to.equal('foo');
                handlers.delete(handler1);
                cb(null, true, handlers);
            });

            qpg.subscribe('foo', handler1, iferr(done, () => {
                qpg.subscribe('foo', handler2, iferr(done, () => {
                    qpg.publish('foo', 'bar', iferr(done, () => {}));
                }));
            }));
        });

        it('should be able to pass filtered handlers as iterator (Set)', function (done) {
            function handler1() {
                done(new Error('should not be called'));
            }

            function handler2(data, info, cb) {
                cb(null, done);
            }

            qpg.filters.push(function (info, handlers, cb) {
                expect(info.topic).to.equal('foo');
                cb(null, true, wu(handlers).filter(h => h !== handler1));
            });

            qpg.subscribe('foo', handler1, iferr(done, () => {
                qpg.subscribe('foo', handler2, iferr(done, () => {
                    qpg.publish('foo', 'bar', iferr(done, () => {}));
                }));
            }));
        });

        it('should be able to pass filtered handlers as iterator (Array)', function (done) {
            function handler1() {
                done(new Error('should not be called'));
            }

            function handler2(data, info, cb) {
                cb(null, done);
            }

            after_each(iferr(done, () => {
                before_each(iferr(done, () => {
                    qpg.subscribe('foo', handler1, iferr(done, () => {
                        qpg.subscribe('foo', handler2, iferr(done, () => {
                            qpg.publish('foo', 'bar', iferr(done, () => {}));
                        }));
                    }));
                }), {
                    filter: function (info, handlers, cb) {
                        expect(info.topic).to.equal('foo');
                        cb(null, true, wu(handlers).filter(h => h !== handler1));
                    },
                    dedup: false
                });
            }));
        });

        it('should support multiple filters', function (done) {
            function handler1() {
                done(new Error('should not be called'));
            }

            function handler2() {
                done(new Error('should not be called'));
            }

            function handler3(data, info, cb) {
                cb(null, done);
            }

            qpg.filters.push(
                function (info, handlers, cb) {
                    expect(info.topic).to.equal('foo');
                    handlers.delete(handler1);
                    cb(null, true, handlers);
                },

                function (info, handlers, cb) {
                    expect(info.topic).to.equal('foo');
                    handlers.delete(handler2);
                    cb(null, true, handlers);
                }
            );

            qpg.subscribe('foo', handler1, iferr(done, () => {
                qpg.subscribe('foo', handler2, iferr(done, () => {
                    qpg.subscribe('foo', handler3, iferr(done, () => {
                        qpg.publish('foo', 'bar', iferr(done, () => {}));
                    }));
                }));
            }));
        });

        it('should not call other filters if error', function (done) {
            this.timeout(5000);

            let called = false;

            qpg.filters.push(
                function (info, handlers, cb) {
                    expect(info.topic).to.equal('foo');
                    cb(new Error('dummy'));
                    if (called) {
                        return done();
                    }
                    called = true;
                },

                function () {
                    done(new Error('should not be called'));
                }
            );

            function handler() {
                done(new Error('should not be called'));
            }

            qpg.subscribe('foo', handler, iferr(done, () => {
                qpg.publish('foo', 'bar', iferr(done, () => {}));
            }));
        });

        it('should not call other filters if not ready', function (done) {
            let called = false;

            qpg.filters.push(
                function (info, handlers, cb) {
                    expect(info.topic).to.equal('foo');
                    cb(null, false);
                    if (called) {
                        return done();
                    }
                    called = true;
                },

                function () {
                    done(new Error('should not be called'));
                }
            );

            function handler() {
                done(new Error('should not be called'));
            }

            qpg.subscribe('foo', handler, iferr(done, () => {
                qpg.publish('foo', 'bar', iferr(done, () => {}));
            }));
        });

        it('should put work back on queue for another handler', function (done) {
            let filter_called = false;

            function handler(data, info, cb) {
                cb('dummy failure');
            }

            qpg.filters.push(
                function (info, handlers, cb) {
                    expect(info.topic).to.equal('foo');
                    expect(info.single).to.be.true;

                    if (filter_called) {
                        handlers.delete(handler);
                        return cb(null, true, handlers);
                    }

                    filter_called = true;
                    cb(null, true, handlers);
                }
            );

            qpg.subscribe('foo', handler, iferr(done, () => {
                qpg.subscribe('foo', function (data, info, cb) {
                    if (filter_called) {
                        return cb(null, done);
                    }
                    cb('dummy failure2');
                }, iferr(done, () => {
                    qpg.publish('foo', 'bar', { single: true }, iferr(done, () => {}));
                }));
            }));
        });

        it('should put work back on queue for a handler on another queue', function (done) {
            let filter_called = false;

            function handler(data, info, cb) {
                cb('dummy failure');
            }

            qpg.filters.push(
                function (info, handlers, cb) {
                    expect(info.topic).to.equal('foo');
                    expect(info.single).to.be.true;

                    if (filter_called) {
                        handlers.delete(handler);
                    }

                    filter_called = true;
                    cb(null, true, handlers);
                }
            );

            make_qpg(iferr(done, qpg2 => {
                qpg.subscribe('foo', handler, iferr(done, () => {
                    qpg2.subscribe('foo', function (data, info, cb) {
                        if (filter_called) {
                            return cb(null, iferr(done, () => qpg2.stop(done)));
                        }
                        cb('dummy failure2');
                    }, iferr(done, () => {
                        qpg.publish('foo', 'bar', { single: true }, iferr(done, () => {}));
                    }));
                }));
            }), { name: 'test2' });
        });

        it('should allow handlers to delay a message', function (done) {
            this.timeout(30000);

            let ready_multi = false;
            let ready_single = false;
            let got_multi = false;
            let got_single = false;
            let count = 0;

            function handler(data, info, cb) {
                expect(data.toString()).to.equal('bar');
                cb(null, err => {
                    if (info.single) {
                        expect(got_single).to.be.false;
                        got_single = true;
                    } else {
                        expect(got_multi).to.be.false;
                        got_multi = true;
                    }
                    if (got_single && got_multi && ready_single && ready_multi) {
                        expect(count).to.equal(10);
                        done(err);
                    }
                });
            }

            qpg.filters.push(
                function (info, handlers, cb) {
                    expect(info.topic).to.equal('foo');
                    if (info.single) {
                        ready_single = true;
                    } else {
                        ready_multi = true;
                    }
                    cb(null, (++count % 5) === 0, handlers);
                }
            );

            qpg.subscribe('foo', handler, iferr(done, () => {
                qpg.publish('foo', 'bar', iferr(done, () => {}));
                qpg.publish('foo', 'bar', { single: true }, iferr(done, () => {}));
            }));
        });

        it('should emit stop event', function (done) {
            make_qpg(iferr(done, qpg2 => {
                qpg2.stop();
                qpg2.on('stop', done);
            }), { name: 'test2' });
        });

        it('should support per-message time-to-live', function (done) {
            qpg.subscribe('foo', function (data, info) {
                exists(info.id, iferr(done, b => {
                    expect(b).to.be.true;
                    setTimeout(() => {
                        qpg.force_refresh();
                        setTimeout(() => {
                            exists(info.id, iferr(done, b => {
                                expect(b).to.be.false;
                                done();
                            }));
                        }, 500);
                    }, 500);
                }));
            }, iferr(done, () => {
                qpg.publish('foo', 'bar', { ttl: 500 }, iferr(done, () => {}));
            }));
        });

        it('should call error function', function (done) {
            qpg.on('warning', function (err) {
                expect(err).to.equal('dummy failure');
                done();
            });

            qpg.subscribe('foo', function (data, info, cb) {
                cb('dummy failure');
            }, iferr(done, () => {
                qpg.publish('foo', 'bar', { single: true }, iferr(done, () => {}));
            }));
        });

        it('should support custom polling interval', function (done) {
            make_qpg(iferr(done, qpg2 => {
                let time = Date.now();
                let count = 0;

                qpg2.subscribe('foo', function (data, info, cb) {
                    const time2 = Date.now();
                    expect(time2 - time).to.be.below(100);
                    time = time2;
                    if (++count === 10) {
                        return cb(null, iferr(done, () => qpg2.stop(done)));
                    }
                    cb('dummy failure');
                }, iferr(done, () => {
                    qpg.publish('foo', 'bar', { single: true }, iferr(done, () => {}));
                }));
            }), {
                name: 'test2',
                poll_interval: 50
            });
        });

        it('should support unsubscribing', function (done) {
            this.timeout(5000);

            let count = 0;

            function handler(data, info, cb) {
                if (++count > 1) {
                    return done(new Error('should not be called'));
                }

                qpg.unsubscribe('foo', handler, iferr(done, () => {
                    qpg.publish('foo', 'bar', iferr(done, () => {
                        setTimeout(cb, 2000, null, done);
                    }));
                }));
            }

            qpg.subscribe('foo', handler, iferr(done, () => {
                qpg.publish('foo', 'bar', iferr(done, () => {}));
            }));
        });

        it('should support unsubscribing all handlers for a topic', function (done) {
            this.timeout(5000);

            let count = 0;

            function handler(data, info, cb) {
                if (++count > 2) {
                    return done(new Error('should not be called'));
                }

                if (count === 2) {
                    qpg.unsubscribe('foo', undefined, iferr(done, () => {
                        qpg.publish('foo', 'bar', iferr(done, () => {
                            setTimeout(cb, 2000, null, done);
                        }));
                    }));
                }
            }

            qpg.subscribe('foo', function (data, info, cb) {
                handler(data, info, cb); 
            }, iferr(done, () => {
                qpg.subscribe('foo', function (data, info, cb) {
                    handler(data, info, cb); 
                }, iferr(done, () => {
                    qpg.publish('foo', 'bar', iferr(done, () => {}));
                }));
            }));
        });

        it('should support unsubscribing all handlers', function (done) {
            this.timeout(5000);

            let count = 0;

            function handler(data, info, cb) {
                if (++count > 2) {
                    return done(new Error('should not be called'));
                }

                if (count === 2) {
                    qpg.subscribe('foo2', function () {
                        done(new Error('should not be called'));
                    }, iferr(done, () => {
                        qpg.unsubscribe(iferr(done, () => {
                            qpg.publish('foo', 'bar', iferr(done, () => {
                                qpg.publish('foo2', 'bar2', iferr(done, () => {
                                    setTimeout(cb, 2000, null, done);
                                }));
                            }));
                        }));
                    }));
                }
            }

            qpg.subscribe('foo', function (data, info, cb) {
                handler(data, info, cb); 
            }, iferr(done, () => {
                qpg.subscribe('foo', function (data, info, cb) {
                    handler(data, info, cb); 
                }, iferr(done, () => {
                    qpg.publish('foo', 'bar', iferr(done, () => {}));
                }));
            }));
        });

        it('should support changing the default time-to-live', function (done) {
            this.timeout(5000);
            after_each(iferr(done, () => {
                before_each(iferr(done, () => {
                    let got_single = false;
                    let got_multi = false;

                    qpg.subscribe('foo', function (data, info, cb) {
                        cb(null, () => {
                            if (info.single) {
                                got_single = true;
                            } else {
                                got_multi = true;
                            }

                            if (got_single && got_multi) {
                                setTimeout(() => {
                                    qpg.force_refresh();
                                    setTimeout(() => {
                                        exists(info.id, iferr(done, b => {
                                            expect(b).to.be.false;
                                            done();
                                        }));
                                    }, 1000);
                                }, 1000);
                            }
                        });
                    }, iferr(done, () => {
                        qpg.publish('foo', 'bar', iferr(done, () => {}));
                        qpg.publish('foo', 'bar', { single: true }, iferr(done, () => {}));
                    }));
                }), {
                    multi_ttl: 1000,
                    single_ttl: 1000
                });
            }));
        });

        it('should publish and receive twice', function (done) {
            let count_multi = 0;
            let count_single = 0;

            qpg.subscribe('foo', function (data, info, cb) {
                cb(null, () => {
                    if (info.single) {
                        ++count_single;
                    } else {
                        ++count_multi;
                    }
                    if ((count_single == 2) && (count_multi === 2)) {
                        done();
                    } else if ((count_single > 2) || (count_multi > 2)) {
                        done(new Error('called too many times'));
                    }
                });
            }, iferr(done, () => {
                timesSeries(2, (n, cb) => {
                    eachSeries([true, false], (single, cb) => {
                        qpg.publish('foo', 'bar', { single }, cb);
                    }, cb);
                }, iferr(done, () => {}));
            }));
        });

        it('should fail to publish and subscribe to messages with > 255 character topics', function (done) {
            const arr = [];
            arr.length = 257;
            const topic = arr.join('a');

            qpg.subscribe(topic, () => {}, err => {
                expect(err.message).to.equal(`topic too long: ${topic}`);
                qpg.publish(topic, 'bar', { ttl: 1000 }, err => {
                    expect(err.message).to.equal(`topic too long: ${topic}`);
                    done();
                });
            });
        });

        it('should publish and subscribe to messages with 255 character topics', function (done) {
            this.timeout(5000);

            const arr = [];
            arr.length = 256;
            const topic = arr.join('a');

            qpg.subscribe(topic, function (data, info) {
                expect(info.topic).to.equal(topic);
                expect(info.single).to.equal(false);
                expect(data.toString()).to.equal('bar');

                setTimeout(() => {
                    qpg.force_refresh();
                    setTimeout(() => {
                        exists(info.id, iferr(done, b => {
                            expect(b).to.be.false;
                            done();
                        }));
                    }, 500);
                }, 1000);
            }, iferr(done, () => {
                qpg.publish(topic, 'bar', { ttl: 1000 }, iferr(done, () => {}));
            }));
        });

        it('should not read multi-worker messages which already exist', function (done) {
            this.timeout(10000);

            qpg.publish('foo', 'bar', iferr(done, () => {
                after_each(iferr(done, () => {
                    before_each(iferr(done, () => {
                        qpg.subscribe('foo', function () {
                            done(new Error('should not be called'));
                        }, iferr(done, () => {
                            setTimeout(done, 5000);
                        }));
                    }));
                }));
            }));
        });

        it('should read single-worker messages which already exist', function (done) {
            qpg.publish('foo', 'bar', { single: true }, iferr(done, () => {
                after_each(iferr(done, () => {
                    before_each(iferr(done, () => {
                        qpg.subscribe('foo', function (data, info, cb) {
                            cb(null, done);
                        }, iferr(done, () => {}));
                    }));
                }));
            }));
        });

        it('should read single-worker messages which already exist (before start)', function (done) {

            qpg.publish('foo', 'bar', { single: true }, iferr(done, () => {
                after_each(iferr(done, () => {
                    const qpg = make_qpg(null, { name: 'test2' });
                    qpg.subscribe('foo', function (data, info, cb) {
                        cb(null, iferr(done, () => qpg.stop(done)));
                    }, iferr(done, () => {}));
                }));
            }));
        });

        it('should error if stopped while publishing', function (done) {
            qpg.publish('foo', 'bar', err => {
                expect(err.message).to.equal('stopped');
                done();
            });
            qpg.stop();
        });

        it('should error if connection terminated while publishing', function (done) {
            const orig_query = qpg._client.query;
            qpg._client.query = function (...args) {
                const r = orig_query.apply(this, args);
                qpg.stop();
                return r;
            };
            qpg.publish('foo', 'bar', err => {
                expect(err.message).to.equal('Connection terminated');
                done();
            });
        });

        it('should error if stopped while subscribing', function (done) {
            const orig_update_trigger = qpg._update_trigger;
            let update_trigger_cb;
            qpg._update_trigger = function (cb) {
                update_trigger_cb = cb;
            };
            qpg.subscribe('foo', function () {
                orig_update_trigger.call(this, update_trigger_cb);
                this.stop();
            }, err => {
                expect(err.message).to.equal('stopped');
                done();
            });
            // Note: publish can't error because qpg._check query won't get to
            // front of queue until publish task completes
            qpg.publish('foo', 'bar', iferr(done, () => {}));
        });

        it('should error if connection terminated while subscribing', function (done) {
            const orig_update_trigger = qpg._update_trigger;
            let update_trigger_cb;
            qpg._update_trigger = function (cb) {
                update_trigger_cb = cb;
            };
            qpg.subscribe('foo', function () {
                const orig_query = qpg._client.query;
                qpg._client.query = function (...args) {
                    const r = orig_query.apply(this, args);
                    qpg.stop();
                    return r;
                };
                orig_update_trigger.call(this, update_trigger_cb);
            }, err => {
                expect(err.message).to.equal('Connection terminated');
                done();
            });
            // Note: publish can't error because qpg._check query won't get to
            // front of queue until publish task completes
            qpg.publish('foo', 'bar', iferr(done, () => {}));
        });

        it('should error if stopped while unsubscribing', function (done) {
            const orig_update_trigger = qpg._update_trigger;
            qpg._update_trigger = function (cb) {
                this.stop();
                orig_update_trigger.call(this, cb);
            };
            qpg.unsubscribe(err => {
                expect(err.message).to.equal('stopped');
                done();
            });
        });

        it('should error if connection terminated while unsubscribing', function (done) {
            const orig_update_trigger = qpg._update_trigger;
            qpg._update_trigger = function (cb) {
                const orig_query = qpg._client.query;
                qpg._client.query = function (...args) {
                    const r = orig_query.apply(this, args);
                    qpg.stop();
                    return r;
                };
                orig_update_trigger.call(this, cb);
            };
            qpg.unsubscribe(err => {
                expect(err.message).to.equal('Connection terminated');
                done();
            });
        });

        it("should not error if done while publishing (would be stopped error if after_each didn't wait)", function (done) {
            qpg.publish('foo', 'bar', iferr(done, () => {}));
            done();
        });

        it("should not error if done while publishing (would be Connection terminated error if after_each didn't wait)", function (done) {
            const orig_query = qpg._client.query;
            qpg._client.query = function (...args) {
                qpg._client.query = orig_query;
                const r = orig_query.apply(this, args);
                done();
                return r;
            };
            qpg.publish('foo', 'bar', iferr(done, () => {}));
        });

        it("should not error if done while subscribing (would be stopped error if after_each didn't wait)", function (done) {
            const orig_update_trigger = qpg._update_trigger;
            let update_trigger_cb;
            qpg._update_trigger = function (cb) {
                update_trigger_cb = cb;
            };
            qpg.subscribe('foo', function () {
                orig_update_trigger.call(this, update_trigger_cb);
                done();
            }, iferr(done, () => {}));
            // Note: publish can't error because qpg._check query won't get to
            // front of queue until publish task completes
            qpg.publish('foo', 'bar', iferr(done, () => {}));
        });

        it("should not error if done while subscribing (would be Connection terminated error if after each didn't wait)", function (done) {
            const orig_update_trigger = qpg._update_trigger;
            let update_trigger_cb;
            qpg._update_trigger = function (cb) {
                update_trigger_cb = cb;
            };
            qpg.subscribe('foo', function () {
                const orig_query = qpg._client.query;
                qpg._client.query = function (...args) {
                    qpg._client.query = orig_query;
                    const r = orig_query.apply(this, args);
                    done();
                    return r;
                };
                orig_update_trigger.call(this, update_trigger_cb);
            }, iferr(done, () => {}));
            // Note: publish can't error because qpg._check query won't get to
            // front of queue until publish task completes
            qpg.publish('foo', 'bar', iferr(done, () => {}));
        });

        it("should not error if done while unsubscribing (would be stopped error if after_each didn't wait)", function (done) {
            const orig_update_trigger = qpg._update_trigger;
            qpg._update_trigger = function (cb) {
                done();
                orig_update_trigger.call(this, cb);
            };
            qpg.unsubscribe(iferr(done, () => {}));
        });

        it("should not error if done while unsubscribing (would be Connection terminated error if after_each didn't wait)", function (done) {
            const orig_update_trigger = qpg._update_trigger;
            qpg._update_trigger = function (cb) {
                const orig_query = qpg._client.query;
                qpg._client.query = function (...args) {
                    qpg._client.query = orig_query;
                    const r = orig_query.apply(this, args);
                    done();
                    return r;
                };
                orig_update_trigger.call(this, cb);
            };
            qpg.unsubscribe(iferr(done, () => {}));
        });

        it('should support streaming interfaces', function (done) {
            let stream_multi;
            let stream_single;
            let stream_file;
            let sub_multi_called = false;
            let sub_single_called = false;
            let pub_multi_called = false;
            let pub_single_called = false;

            function handler(stream, info, cb) {
                const hash = createHash('sha256');
                let len = 0;

                stream.on('readable', function () {
                    while (true) { // eslint-disable-line no-constant-condition
                        const chunk = stream.read();
                        if (!chunk) {
                            break;
                        }
                        len += chunk.length;
                        hash.update(chunk);
                    }
                });

                stream.on('end', function () {
                    expect(len).to.equal(1024 * 1024);
                    expect(hash.digest().equals(random_hash)).to.be.true;
                    cb(null, function () {
                        if (info.single) {
                            expect(sub_single_called).to.be.false;
                            sub_single_called = true;
                        } else {
                            expect(sub_multi_called).to.be.false;
                            sub_multi_called = true;
                        }

                        if (pub_multi_called && pub_single_called &&
                            sub_multi_called && sub_single_called) {
                            done();
                        }
                    });
                });
            }
            handler.accept_stream = true;

            function published() {
                if (pub_multi_called && pub_single_called &&
                    sub_multi_called && sub_single_called) {
                    done();
                }
            }

            qpg.subscribe('foo', handler, iferr(done, () => {
                stream_multi = qpg.publish('foo', iferr(done, () => {
                    expect(pub_multi_called).to.be.false;
                    pub_multi_called = true;
                    published();
                }));

                stream_single = qpg.publish('foo', { single: true }, iferr(done, () => {
                    expect(pub_single_called).to.be.false;
                    pub_single_called = true;
                    published();
                }));

                stream_file = createReadStream(random_path);
                stream_file.pipe(stream_multi);
                stream_file.pipe(stream_single);
            }));
        });

        it('should pipe to more than one stream', function (done) {
            let done1 = false;
            let done2 = false;

            class CheckStream extends Writable {
                constructor() {
                    super();
                    this._hash = createHash('sha256');
                    this._len = 0;

                    this.on('finish', () => {
                        this.emit('done', {
                            digest: this._hash.digest(),
                            len: this._len
                        });
                    });
                }

                _write(chunk, encoding, cb) {
                    this._len += chunk.length;
                    this._hash.update(chunk);
                    cb();
                }
            }

            function check(obj, cb) {
                expect(obj.len).to.equal(1024 * 1024);
                expect(obj.digest.equals(random_hash)).to.be.true;
                if (done1 && done2) {
                    return cb(null, done);
                }
                cb();
            }

            function handler1(stream, info, cb) {
                const cs = new CheckStream();
                cs.on('done', obj => {
                    done1 = true;
                    check(obj, cb);
                });
                stream.pipe(cs);
            }
            handler1.accept_stream = true;

            function handler2(stream, info, cb) {
                const cs = new CheckStream();
                cs.on('done', obj => {
                    done2 = true;
                    check(obj, cb);
                });
                stream.pipe(cs);
            }
            handler2.accept_stream = true;

            qpg.subscribe('foo', handler1, iferr(done, () => {
                qpg.subscribe('foo', handler2, iferr(done, () => {
                    createReadStream(random_path).pipe(
                        qpg.publish('foo', iferr(done, () => {})));
                }));
            }));
        });

        it('should not call the same handler with stream and data', function (done) {
            function handler(stream, info, cb) {
                expect(Buffer.isBuffer(stream)).to.equal(false);

                const hash = createHash('sha256');
                let len = 0;

                stream.on('readable', function () {
                    while (true) { // eslint-disable-line no-constant-condition
                        const chunk = stream.read();
                        if (!chunk) {
                            break;
                        }
                        len += chunk.length;
                        hash.update(chunk);
                    }
                });

                stream.on('end', function () {
                    expect(len).to.equal(1024 * 1024);
                    expect(hash.digest().equals(random_hash)).to.be.true;
                    cb(null, done);
                });
            }
            handler.accept_stream = true;

            qpg.subscribe('foo', handler, iferr(done, () => {
                createReadStream(random_path).pipe(
                    qpg.publish('foo', iferr(done, () => {})));
            }));
        });

        it('should use trigger to process messages straight away', function (done) {
            after_each(iferr(done, () => {
                before_each(iferr(done, () => {
                    let time;
                    qpg.subscribe('foo', function (data, info, cb) {
                        expect(Date.now() - time).to.be.below(qpg._check_interval);
                        cb(null, done);
                    }, iferr(done, () => {
                        time = Date.now();
                        qpg.publish('foo', 'bar', iferr(done, () => {}));
                    }));
                }), {
                    poll_interval: 10 * 1000,
                });
            }));
        });

        it('should be able to disable trigger', function (done) {
            this.timeout(40000);

            after_each(iferr(done, () => {
                before_each(iferr(done, () => {
                    let time;
                    qpg.subscribe('foo', function (data, info, cb) {
                        expect(Date.now() - time).to.be.at.least(qpg._check_interval - 1000);
                        cb(null, done);
                    }, iferr(done, () => {
                        time = Date.now();
                        // The countdown to the next poll has already started so
                        // from here it may not be poll_interval until the message
                        // is received - which is why we subtract a second above.
                        qpg.publish('foo', 'bar', {
                            ttl: 30 * 1000
                        }, iferr(done, () => {}));
                    }));
                }), {
                    poll_interval: 10 * 1000,
                    notify: false
                });
            }));
        });

        it('should read one message at a time by default', function (done) {
            this.timeout(60000);

            after_each(iferr(done, () => {
                before_each(iferr(done, () => {
                    let in_call = false;
                    let count = 0;

                    function handler(stream, info, cb) {
                        expect(in_call).to.equal(false);
                        in_call = true;
                        ++count;

                        stream.on('end', function () {
                            in_call = false;
                            cb(null, count === 5 ? done : null);
                        });

                        if (count === 5) {
                            return stream.on('data', () => {});
                        }

                        // Give time for other reads to start.
                        setTimeout(() => {
                            stream.on('data', () => {});
                        }, 5 * 1000);
                    }
                    handler.accept_stream = true;

                    qpg.subscribe('foo', handler, iferr(done, () => {
                        for (let i = 0; i < 5; ++i) {
                            qpg.publish('foo', 'bar', {
                                ttl: 2 * 60 * 1000,
                                single: true
                            }, iferr(done, () => {}));
                        }
                    }));
                }), {
                    poll_interval: 10 * 1000,
                    notify: false
                });
            }));
        });

        it('should be able to read more than one message at a time', function (done) {
            this.timeout(2 * 60 * 1000);

            after_each(iferr(done, () => {
                before_each(iferr(done, () => {
                    let in_call = 0;
                    let count = 0;
                    let prev_stream = null;

                    function handler(stream, info, cb) {
                        ++in_call;
                        ++count;
                        console.log(count); // eslint-disable-line no-console

                        if (count % 2 === 1) {
                            expect(prev_stream).to.be.null;
                            prev_stream = stream;
                        } else {
                            expect(prev_stream).not.to.be.null;

                            stream.on('data', () => {});
                            prev_stream.on('data', () => {});

                            prev_stream = null;
                        }

                        stream.on('end', function () {
                            --in_call;
                            cb(null, (count === 26) && (in_call === 0) ? done : null);
                        });
                    }
                    handler.accept_stream = true;

                    qpg.subscribe('foo', handler, iferr(done, () => {
                        for (let i = 0; i < 26; ++i) {
                            qpg.publish('foo', 'bar', {
                                ttl: 2 * 60 * 1000,
                                single: true
                            }, iferr(done, () => {}));
                        }
                    }));
                }), {
                    poll_interval: 1 * 1000,
                    notify: false,
                    message_concurrency: 2
                });
            }));
        });

        it('should clear up expired messages', function (done) {
            this.timeout(120000);

            const num_queues = 100; // default max 100, remember to close pgadminX
            const num_messages = 500;

            after_each(iferr(done, () => {
                timesSeries(num_queues, (n, cb) => {
                    make_qpg(cb, {
                        name: `test${n}`
                    });
                }, iferr(done, qpgs => {
                    expect(qpgs.length).to.equal(num_queues);
                    timesSeries(num_messages, (n, cb) => {
                        qpgs[0].publish('foo', 'bar', { ttl: 2 * 1000 }, cb);
                    }, iferr(done, () => {
                        setTimeout(() => {
                            each(qpgs, (qpg, next) => {
                                qpg.subscribe('foo', () => {
                                    done(new Error('should not be called'));
                                }, iferr(done, () => {
                                    qpg.force_refresh();
                                    next();
                                }));
                            }, iferr(done, () => {
                                setTimeout(() => {
                                    each(qpgs, (qpg, next) => {
                                        count(qpg, iferr(done, n => {
                                            expect(n).to.equal(0);
                                            qpg.stop(next);
                                        }));
                                    }, done);
                                }, 5000);
                            }));
                        }, 2000);
                    }));
                }));
            }));
        });

        it('should clear up expired message while worker has it locked', function (done) {
            this.timeout(60000);

            qpg.subscribe('foo', function (data, info, cb) {
                setTimeout(() => {
                    count(qpg, iferr(done, n => {
                        expect(n).to.equal(0);
                        cb(null, done);
                    }));
                }, 15000);
            }, iferr(done, () => {
                qpg.publish('foo', 'bar', { single: true, ttl: 5000 }, iferr(done, () => {}));
            }));
        });

        it('should emit an error event if an error occurs before a start event', function (done) {
            after_each(iferr(done, () => {
                const qpg = make_qpg(null, { name: 'test2' });
                const orig_query = qpg._client.query;
                qpg._client.query = async function () {
                    throw new Error('dummy error');
                };
                qpg.on('error', function (err) {
                    expect(err.message).to.equal('dummy error');
                    qpg._client.query = orig_query;
                    qpg.stop(done);
                });
            }));
        });

        it('should pass back write errors when publishing', function (done) {
            const orig_query = qpg._client.query;
            qpg._client.query = function (...args) {
                const cb = args[args.length - 1];
                cb(new Error('dummy error'));
            };
            qpg.publish('foo', 'bar', err => {
                expect(err.message).to.equal('dummy error');
                qpg._client.query = orig_query;
                done();
            });
        });

        it('should close stream if error occurs when publishing', function (done) {
            let finished = false;

            const s = qpg.publish('foo', err => {
                expect(err.message).to.equal('dummy error');
                expect(finished).to.be.true;
                done();
            });

            s.on('prefinish', () => {
                finished = true;
            });

            s.emit('error', new Error('dummy error'));
        });

        it('should support disabling work queue (single messages)', function (done) {
            this.timeout(5000);

            after_each(iferr(done, () => {
                before_each(iferr(done, () => {
                    let called = false;
                    qpg.subscribe('foo', function (data, info) {
                        expect(info.topic).to.equal('foo');
                        expect(data.toString()).to.equal('bar');
                        expect(info.single).to.be.false;
                        called = true;
                    }, iferr(done, () => {
                        qpg.publish('foo', 'bar');
                        qpg.publish('foo', 'bar', { single: true });
                    }));
                    setTimeout(() => {
                        expect(called).to.be.true;
                        done();
                    }, 3000);
                }), {
                    single: false
                });
            }));
        });

        it('should error when publishing to a topic with an invalid character', function (done) {
            qpg.publish('\0foo', 'bar', err => {
                expect(err.message).to.equal(`invalid publication topic: \0foo`);
                qpg.publish('foo@', 'bar', err => {
                    expect(err.message).to.equal(`invalid publication topic: foo@`);
                    done();
                });
            });
        });

        it('should error when publishing to a long topic', function (done) {
            const arr = [];
            arr.length = 257;
            qpg.publish(arr.join('a'), 'bar', err => {
                expect(err.message).to.equal(`topic too long: ${arr.join('a')}`);
                done();
            });
        });

        it('should not error when publishing to an empty topic', function (done) {
            qpg.publish('', 'bar', done);
        });

        it('should error when subscribing to a topic with an invalid character', function (done) {
            qpg.subscribe('foo@', () => {}, err => {
                expect(err.message).to.equal('invalid subscription topic: foo@');
                qpg.subscribe('a*.b', () => {}, err => {
                    expect(err.message).to.equal('invalid subscription topic: a*.b');
                    qpg.subscribe('a..b', () => {}, err => {
                        // ltree doesn't allow it either
                        expect(err.message).to.equal('invalid subscription topic: a..b');
                        done();
                    });
                });
            });
        });

        it('should error when unsubscribing to a topic with an invalid character', function (done) {
            qpg.unsubscribe('foo@', () => {}, err => {
                expect(err.message).to.equal('invalid subscription topic: foo@');
                qpg.unsubscribe('a*.b', () => {}, err => {
                    expect(err.message).to.equal('invalid subscription topic: a*.b');
                    qpg.unsubscribe('a..b', () => {}, err => {
                        // ltree doesn't allow it either
                        expect(err.message).to.equal('invalid subscription topic: a..b');
                        done();
                    });
                });
            });
        });

        it('should support handler concurrency', function (done) {
            after_each(iferr(done, () => {
                before_each(iferr(done, () => {
                    const streams = [];

                    function handler(s) {
                        streams.push(s);

                        if (streams.length === 1) {
                            this.publish('foo', 'bar2', iferr(done, () => {}));
                        } else if (streams.length === 2) {
                            read_all(streams[0], v => {
                                expect(v.toString()).to.equal('bar');
                                read_all(streams[1], v => {
                                    expect(v.toString()).to.equal('bar2');
                                    done();
                                });
                            });
                        } else {
                            done(new Error('called too many times'));
                        }
                    }
                    handler.accept_stream = true;

                    qpg.subscribe('foo', handler, iferr(done, () => {
                        qpg.publish('foo', 'bar', iferr(done, () => {}));
                    }));
                }), {
                    handler_concurrency: 2
                });
            }));
        });

        it('should support delivering messages in expiry order', function (done) {
            this.timeout(30000);

            after_each(iferr(done, () => {
                before_each(iferr(done, () => {
                    const n = 1000;
                    const ttls_out = [];
                    const ttls_in = [];
                    const expiries_in = [];

                    // Need to leave enough time between ttls to account for time
                    // increasing while publishing
                    for (let i = 0; i < n; ++i) {
                        ttls_out.push(
                            Math.round(
                                Math.random() * 18 * 60 // random mins up to 18 hour period
                                + 1 * 60)               // plus one hour
                            * 60 * 1000);               // convert to milliseconds
                    }

                    function num_sort(x, y) {
                        return x - y;
                    }

                    qpg.subscribe('foo', function (data, info) {
                        ttls_in.push(parseInt(data.toString()));
                        expiries_in.push(info.expires);

                        if (ttls_in.length === n) {
                            // check expiries are actually in ascending order
                            const sorted_expiries_in = expiries_in.concat();
                            sorted_expiries_in.sort(num_sort);
                            expect(expiries_in).to.eql(sorted_expiries_in);

                            // check messages are in expected order
                            ttls_out.sort(num_sort);
                            expect(ttls_in).to.eql(ttls_out);

                            done();
                        } else if (ttls_in.length > n) {
                            done(new Error('called too many times'));
                        }
                    }, iferr(done, () => {
                        eachSeries(ttls_out, (ttl, cb) => {
                            qpg.publish('foo', ttl.toString(), { ttl: ttl }, cb);
                        }, iferr(done, () => {
                            expect(ttls_in.length).to.equal(0);
                            qpg.refresh_now();
                        }));
                    }));
                }), {
                    poll_interval: 60 * 60 * 1000,
                    notify: false,
                    order_by_expiry: true,
                    multi_ttl: 24 * 60 * 60 * 1000
                });
            }));
        });

        it('should support error on stream and calling back', function (done) {
            let count = 0;

            qpg.on('warning', err => {
                expect(err.message).to.equal('dummy');
                ++count;
            });

            function handler(stream, info, cb) {
                stream.emit('error', new Error('dummy'));
                cb(new Error('dummy'), iferr(done, () => {
                    expect(count).to.equal(2);
                    done();
                }));
            }
            handler.accept_stream = true;

            qpg.subscribe('foo', handler, iferr(done, () => {
                qpg.publish('foo', { single: true }).end('bar');
            }));
        });

        it('should support calling back before stream has ended', function (done) {
            let count = 0;

            function handler(stream, info, cb) {
                if (++count === 2) {
                    cb(null, done);
                } else {
                    cb();
                }
            }
            handler.accept_stream = true;

            qpg.subscribe('foo', handler, iferr(done, () => {
                qpg.publish('foo', iferr(done, () => {})).end('bar');
                qpg.publish('foo', { single: true }, iferr(done, () => {})).end('bar');
            }));
        });

        it('should end/error stream after called back before stream has ended', function (done) {
            let count = 0;

            function handler(stream, info, cb) {
                stream.on('readable', function () {
                    const data = this.read();
                    // This is different to qlobber-fsq file streams as they
                    // don't push data initially and so data would be null.
                    expect(data.toString()).to.equal('bar');
                });

                if (++count === 2) {
                    stream.on('end', done);
                    cb();
                } else {
                    let ended = false;
                    let closed = false;
                    let msg;

                    stream.on('end', function () {
                        ended = true;
                    });

                    stream.on('close', function () {
                        closed = true;
                    });

                    stream.on('error', function (err) {
                        msg = err.message;
                    });

                    cb(new Error('dummy'), iferr(done, () => {
                        // We're in a nextTick here so close has been emitted
                        expect(closed).to.be.true;
                        expect(msg).to.equal('dummy');
                        process.nextTick(function () {
                            // end emitted in another nextTick
                            expect(ended).to.be.true;
                            qpg.publish('foo', { single: true }, iferr(done, () => {})).end('bar');
                        });
                    }));
                }
            }
            handler.accept_stream = true;

            qpg.subscribe('foo', handler, iferr(done, () => {
                qpg.publish('foo', iferr(done, () => {})).end('bar');
            }));
        });

        function existing_messages(dedup) {
            describe(`dedup=${dedup}`, function () {
                this.timeout(20000);

                it('should support delivering existing messages to subscribers', function (done) {
                    after_each(iferr(done, () => {
                        before_each(iferr(done, () => {
                            qpg.subscribe('foo', function (data, info) {
                                expect(info.existing).to.be.undefined;
                                expect(data.toString()).to.equal('bar');
                                expect(info.topic).to.equal('foo');

                                setTimeout(() => {
                                    qpg.subscribe('foo', function (data2, info2) {
                                        expect(info.existing).to.be.undefined;
                                        expect(info2.existing).to.be.true;
                                        expect(data2.toString()).to.equal('bar');
                                        expect(info2.topic).to.equal('foo');
                                        expect(info2.id).to.equal(info.id);
                                        done();
                                    }, {
                                        subscribe_to_existing: true
                                    }, iferr(done, () => {}));
                                }, 500);
                            }, iferr(done, () => {
                                qpg.publish('foo', iferr(done, () => {})).end('bar');
                            }));
                        }), {
                            ttl: 10000,
                            dedup
                        });
                    }));
                });

                it('should support delaying existing messages with filter', function (done) {
                    let count = 0;
                    let called1 = false;
                    let called2 = false;

                    function check() {
                        if (called1 && called2) {
                            done();
                        }
                    }

                    function filter(info, handlers, cb) {
                        if (++count === 1) {
                            // allow through initial pub and sub
                            cb(null, true, handlers);
                        } else if (count === 2) {
                            // delay first existing
                            cb(null, false);
                        } else if (count === 3) {
                            // subscribe again to existing messages
                            this.subscribe('foo', function (data, info) {
                                expect(info.existing).to.be.true;
                                expect(data.toString()).to.equal('bar');
                                expect(info.topic).to.equal('foo');
                                called1 = true;
                                check();
                            }, {
                                subscribe_to_existing: true
                            }, iferr(done, () => {
                                // and delay again
                                cb(null, false);
                            }));
                        } else {
                            // allow through existing
                            cb(null, true, handlers);
                        }
                    }

                    after_each(iferr(done, () => {
                        before_each(iferr(done, () => {
                            qpg.subscribe('foo', function (data, info) {
                                expect(info.existing).to.be.undefined;
                                expect(data.toString()).to.equal('bar');
                                expect(info.topic).to.equal('foo');

                                setTimeout(() => {
                                    this.subscribe('foo', function (data2, info2) {
                                        expect(info.existing).to.be.undefined;
                                        expect(info2.existing).to.be.true;
                                        expect(data2.toString()).to.equal('bar');
                                        expect(info2.topic).to.equal('foo');
                                        expect(info2.id).to.equal(info.id);
                                        called2 = true;
                                        check();
                                    }, {
                                        subscribe_to_existing: true
                                    }, iferr(done, () => {}));
                                }, 500);
                            }, iferr(done, () => {
                                qpg.publish('foo', iferr(done, () => {})).end('bar');
                            }));
                        }), {
                            ttl: 10000,
                            dedup,
                            filter
                        });
                    }));
                });

                it('should support delaying existing messages twice', function (done) {
                    let count = 0;
                    let called1 = false;
                    let called2 = false;

                    function check() {
                        if (called1 && called2) {
                            done();
                        }
                    }

                    function filter(info, handlers, cb) {
                        if (++count === 1) {
                            // allow through initial pub and sub
                            cb(null, true, handlers);
                        } else if (count === 2) {
                            // delay first existing
                            cb(null, false);
                        } else if (count === 3) {
                            // subscribe again to existing messages
                            this.subscribe('foo', function (data, info) {
                                expect(info.existing).to.be.true;
                                expect(data.toString()).to.equal('bar');
                                expect(info.topic).to.equal('foo');
                                called1 = true;
                                check();
                            }, {
                                subscribe_to_existing: true
                            }, iferr(done, () => {
                                // and delay again
                                cb(null, false);
                            }));
                        } else if (count === 4) {
                            // and delay again
                            cb(null, false);
                        } else {
                            // allow through existing
                            cb(null, true, handlers);
                        }
                    }

                    after_each(iferr(done, () => {
                        before_each(iferr(done, () => {
                            qpg.subscribe('foo', function (data, info) {
                                expect(info.existing).to.be.undefined;
                                expect(data.toString()).to.equal('bar');
                                expect(info.topic).to.equal('foo');

                                setTimeout(() => {
                                    this.subscribe('foo', function (data2, info2) {
                                        expect(info.existing).to.be.undefined;
                                        expect(info2.existing).to.be.true;
                                        expect(data2.toString()).to.equal('bar');
                                        expect(info2.topic).to.equal('foo');
                                        expect(info2.id).to.equal(info.id);
                                        called2 = true;
                                        check();
                                    }, {
                                        subscribe_to_existing: true
                                    }, iferr(done, () => {}));
                                }, 500);
                            }, iferr(done, () => {
                                qpg.publish('foo', iferr(done, () => {})).end('bar');
                            }));
                        }), {
                            ttl: 10000,
                            dedup,
                            filter
                        });
                    }));
                });

                it('should subscribe to new messages too', function (done) {
                    after_each(iferr(done, () => {
                        before_each(iferr(done, () => {
                            qpg.subscribe('foo', function (data, info) {
                                expect(info.existing).to.be.undefined;
                                expect(data.toString()).to.equal('bar');
                                expect(info.topic).to.equal('foo');
                                done();
                            }, {
                                subscribe_to_existing: true
                            }, iferr(done, () => {
                                qpg.publish('foo', iferr(done, () => {})).end('bar');
                            }));
                        }), {
                            ttl: 10000,
                            dedup
                        });
                    }));
                });

                it('should support unsubscribing from existing messages', function (done) {
                    function handler() {
                        done(new Error('should not be called'));
                    }

                    after_each(iferr(done, () => {
                        before_each(iferr(done, () => {
                            qpg.subscribe('foo', function (data, info) {
                                expect(info.existing).to.be.undefined;
                                expect(data.toString()).to.equal('bar');
                                expect(info.topic).to.equal('foo');

                                setTimeout(() => {
                                    this.subscribe('foo', handler, {
                                        subscribe_to_existing: true
                                    }, iferr(done, () => {
                                        qpg.unsubscribe('foo', handler, iferr(done, () => {
                                            setTimeout(done, 1500);
                                        }));
                                    }));
                                }, 500);
                            }, iferr(done, () => {
                                qpg.publish('foo', iferr(done, () => {})).end('bar');
                            }));
                        }), {
                            ttl: 10000,
                            dedup
                        });
                    }));
                });

                function unsub_delayed_existing(unsub, done) {
                    let count = 0;

                    function handler() {
                        done(new Error('should not be called'));
                    }

                    function handler2() {
                        done(new Error('should not be called'));
                    }

                    function filter(info, handlers, cb) {
                        if (++count === 1) {
                            // allow through initial pub and sub
                            cb(null, true, handlers);
                        } else if (count === 2) {
                            // delay first existing
                            cb(null, false);
                        } else if (count === 3) {
                            // subscribe again to existing messages
                            this.subscribe('foo', handler2, {
                                subscribe_to_existing: true
                            }, iferr(done, () => {
                                unsub(handler, handler2, iferr(done, () => {
                                    // and delay again
                                    cb(null, false);
                                    setTimeout(done, 1500);
                                }));
                            }));
                        } else {
                            // existing message doesn't call filter if there are
                            // no handlers
                            done(new Error('called too many times'));
                        }
                    }

                    after_each(iferr(done, () => {
                        before_each(iferr(done, () => {
                            qpg.subscribe('foo',  function (data, info) {
                                expect(info.existing).to.be.undefined;
                                expect(data.toString()).to.equal('bar');
                                expect(info.topic).to.equal('foo');

                                setTimeout(() => {
                                    this.subscribe('foo', handler, {
                                        subscribe_to_existing: true
                                    }, iferr(done, () => {}));
                                }, 500);
                            }, iferr(done, () => {
                                qpg.publish('foo', iferr(done, () => {})).end('bar');
                            }));
                        }), {
                            ttl: 10000,
                            dedup,
                            filter
                        });
                    }));
                }

                it('should support unsubscribing from delayed existing messages (by handler)', function (done) {
                    unsub_delayed_existing(function (handler, handler2, cb) {
                        parallel([
                            cb => qpg.unsubscribe('foo', handler, cb),
                            cb => qpg.unsubscribe('foo', handler2, cb)
                        ], cb);
                    }, done);
                });

                it('should support unsubscribing from delayed existing messages (by topic)', function (done) {
                    unsub_delayed_existing(function (handler, handler2, cb) {
                        qpg.unsubscribe('foo', undefined, cb);
                    }, done);
                });

                it('should support unsubscribing from delayed existing messages (all)', function (done) {
                    unsub_delayed_existing(function (handler, handler2, cb) {
                        qpg.unsubscribe(cb);
                    }, done);
                });
            });
        }

        describe('existing messages', function () {
            existing_messages(true);
            existing_messages(false);
        });

        it('should be able to unsubscribe while message being processed', function (done) {
            const orig_num_handlers = qpg._num_handlers;

            function handler() {
                done(new Error('should not be called'));
            }

            qpg._num_handlers = function (...args) {
                qpg._num_handlers = orig_num_handlers;
                
                qpg.unsubscribe('foo', handler, iferr(done, () => {
                    setImmediate(done);
                }));

                return orig_num_handlers.apply(this, args);
            };

            qpg.subscribe('foo', handler, iferr(done, () => {
                qpg.publish('foo', 'bar', iferr(done, () => {}));
            }));
        });

        it('should publish empty message', function (done) {
            qpg.subscribe('foo', function (data, info) {
                expect(info.topic).to.equal('foo');
                expect(data.length).to.equal(0);
                done();
            }, iferr(done, () => {
                qpg.publish('foo', iferr(done, () => {})).end();
            }));
        });
    });
}

test({});
test({
    separator: '/',
    wildcard_one: '+',
    wildcard_some: 'M'
});
