const { parallel } = require('async');
const { QlobberPG } = require('..');
const { expect } = require('chai');
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
});
