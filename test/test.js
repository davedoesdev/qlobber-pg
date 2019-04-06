const { parallel } = require('async');
const { QlobberPG } = require('..');
const { expect } = require('chai');
const config = require('config');
const iferr = require('iferr');

describe('qlobber-pq', function () {
    let qpg;

    beforeEach(function (cb) {
        qpg = new QlobberPG(Object.assign({
            name: 'test1' 
        }, config));
        qpg.on('warning', console.error);
        qpg.start(cb);
    });

    afterEach(function (cb) {
        qpg.stop(cb);
    });

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
});
