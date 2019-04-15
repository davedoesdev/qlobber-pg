const { parallel } = require('async');
const { QlobberPG } = require('..');
const { expect } = require('chai');
const wu = require('wu');
const config = require('config');
const iferr = require('iferr');

describe('qlobber-pq', function () {
    let qpg;

    function make_qpg(cb, options) {
        const qpg = new QlobberPG(Object.assign({
            name: 'test1' 
        }, config, options));
        qpg.on('warning', console.error);
        qpg.on('start', () => cb(null, qpg));
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
        qpg.stop(cb);
    }
    afterEach(after_each);

    it('should subscribe and publish to a simple topic', function (done) {
        let pub_info;

        qpg.subscribe('foo', (data, info, cb) => {
            expect(info.topic).to.equal('foo');
            expect(info.single).to.be.false;
            expect(data.toString()).to.equal('bar');
            expect(cb.num_handlers).to.equal(1);

            pub_info.id = info.id;
            pub_info.data = info.data;
            expect(info).to.eql(pub_info);

            done();
        }, iferr(done, () => {
            qpg.publish('foo', 'bar', function (err, info) {
                if (err) {
                    return done(err);
                }
                pub_info = info;
            });
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

        qpg.subscribe('*', function (data, info) {
            expect(info.topic).to.equal('foo');
            expect(data.toString()).to.equal('bar');
            received();
        }, iferr(done, () => {
            qpg.subscribe('#', function (data, info) {
                expect(info.topic).to.equal('foo');
                expect(data.toString()).to.equal('bar');
                received();
            }, iferr(done, () => {
                qpg.publish('foo', 'bar', iferr(done, () => {}));
            }));
        }));
    });

    it('should only call each handler once', function (done) {
        const handler = function (data, info) {
            expect(info.topic).to.equal('foo');
            expect(data.toString()).to.equal('bar');
            done();
        }

        qpg.subscribe('*', handler, iferr(done, () => {
            qpg.subscribe('#', handler, iferr(done, () => {
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

                qpg.subscribe('*', handler, iferr(done, () => {
                    qpg.subscribe('#', handler, iferr(done, () => {
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
    })

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

                setTimeout(() => cb(null, iferr(done, () => qpg2.stop(done))),
                           2000);
            }

            parallel([
                cb => qpg.subscribe('foo', (...args) => handler(...args), cb),
                cb => qpg.subscribe('foo', (...args) => handler(...args), cb),
                cb => qpg2.subscribe('foo', (...args) => handler(...args), cb),
                cb => qpg2.subscribe('foo', (...args) => handler(...args), cb),
                cb => qpg.publish('foo', 'bar', { single: true }, cb)
            ]);
        }));
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

            function (info, handlers, cb) {
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

            function (info, handlers, cb) {
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
                        return cb(null, () => qpg2.stop(done));
                    }
                    cb('dummy failure2');
                }, iferr(done, () => {
                    qpg.publish('foo', 'bar', { single: true }, iferr(done, () => {}));
                }));
            }));
        }));
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
        }));
    });

    it('should support per-message time-to-live', function (done) {
        qpg.subscribe('foo', function (data, info) {
            function exists(cb) {
                qpg._queue.push(cb => qpg._client.query('SELECT EXISTS(SELECT 1 FROM messages WHERE id = $1)', [
                info.id
            ], cb), iferr(done, r => cb(r.rows[0].exists)));
            }
            exists(b => {
                expect(b).to.be.true;
                setTimeout(() => {
                    qpg.force_refresh();
                    setTimeout(() => {
                        exists(b => {
                            expect(b).to.be.false;
                            done();
                        });
                    }, 500);
                }, 500);
            });
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
                    return cb(null, () => qpg2.stop(done));
                }
                cb('dummy failure');
            }, iferr(done, () => {
                qpg.publish('foo', 'bar', { single: true }, iferr(done, () => {}));
            }));
        }), {
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

    it('should support unsubscribing to all handlers for a topic', function (done) {
        this.timeout(5000);

        let count = 0;

        function handler(data, info, cb)
        {
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
});
