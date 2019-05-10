const { QlobberPG } = require('..');
const qpg = new QlobberPG({
    name: 'example1', 
    db: {
        host: '/var/run/postgresql',
        database: 'qlobber-pg'
    }
});
qpg.subscribe('foo.*', (data, info) => {
    console.log(info.topic, data.toString());
    const assert = require('assert');
    assert.equal(info.topic, 'foo.bar');
    assert.equal(data, 'hello');
});
qpg.publish('foo.bar', 'hello');
