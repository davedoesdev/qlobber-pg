const { QlobberPG } = require('..');
const config = require('config');

describe('qlobber-pq', function () {
    let qpg;

    beforeEach(function (cb) {
        qpg = new QlobberPG(Object.assign({
            name: 'test1' 
        }, config));
        qpg.start(cb);
    });

    afterEach(function (cb) {
        qpg.stop(cb);
    });

    it('should subscribe and publish to a simple topic', function (done) {
        let pub_info;

        qpg.subscribe('foo', function (data, info, cb) {
            console.log(data, info);

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
