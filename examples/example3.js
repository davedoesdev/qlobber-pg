const { QlobberPG } = require('..');
const qpg = new QlobberPG({
    name: 'example3',
    db: {
        host: '/var/run/postgresql',
        database: 'qlobber-pg'
    }
});
function handler(stream, info) {
    const data = [];
    stream.on('readable', function () {
        let chunk;
        while (chunk = this.read()) {
            data.push(chunk);
        }
    });
    stream.on('end', function () {
        const s = Buffer.concat(data).toString();
        console.log(info.topic, s);
        const assert = require('assert');
        assert.equal(info.topic, 'foo.bar');
        assert.equal(s, 'hello');
    });
}
handler.accept_stream = true;
qpg.subscribe('foo.*', handler);
qpg.publish('foo.bar').end('hello');
