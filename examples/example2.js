const { QlobberPG } = require('..');
const qpg = new QlobberPG({
    name: 'example2',
    db: { 
        host: '/var/run/postgresql',
        database: 'qlobber-pg'
    }
});
qpg.publish('foo.bar', 'hello', qpg.stop);
