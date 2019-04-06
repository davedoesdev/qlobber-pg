const { QlobberPG } = require('..');
const { expect } = require('chai');
const config = require('config');

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

        qpg.subscribe('foo', function (data, info, cb) {
            expect(info.topic).to.equal('foo');
            expect(info.single).to.be.false;
            expect(data.toString()).to.equal('bar');
            expect(cb.num_handlers).to.equal(1);

            pub_info.id = info.id;
            pub_info.data = info.data;
            expect(info).to.eql(pub_info);

            done();
        }, function (err) {
            if (err) {
                done(err);
            }

            qpg.publish('foo', 'bar', function (err, info) {
                if (err) {
                    return done(err);
                }
                // topic
                // expires
                // single
                // size
                pub_info = info;
            });
        });
    });
});
