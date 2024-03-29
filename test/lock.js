'use strict';
const { Client } = require('pg');
const config = require('config');
const iferr = require('iferr');

describe('locking', function () {
    let expect;
    before(async () => {
        ({ expect } = await import('chai'));
    });

    it('should try lock', async function () {
        const client = new Client(config.db);
        await client.connect();

        const client2 = new Client(config.db);
        await client2.connect();

        let r = await client.query('SELECT pg_try_advisory_lock($1)', [ 0 ]);
        expect(r.rows[0].pg_try_advisory_lock).to.equal(true);
        r = await client2.query('SELECT pg_try_advisory_lock($1)', [ 0 ]);
        expect(r.rows[0].pg_try_advisory_lock).to.equal(false);

        r = await client.query('SELECT pg_advisory_unlock($1)', [ 0 ]);
        expect(r.rows[0].pg_advisory_unlock).to.equal(true);
        r = await client2.query('SELECT pg_try_advisory_lock($1)', [ 0 ]);
        expect(r.rows[0].pg_try_advisory_lock).to.equal(true);

        r = await client.query('SELECT pg_try_advisory_lock($1)', [ 0 ]);
        expect(r.rows[0].pg_try_advisory_lock).to.equal(false);
        await client2.end();
        r = await client.query('SELECT pg_try_advisory_lock($1)', [ 0 ]);
        expect(r.rows[0].pg_try_advisory_lock).to.equal(true);

        await client.end();
    });

    it('should block lock', function (done) {
        this.timeout(5000);

        const client = new Client(config.db);
        client.connect(iferr(done, () => {
            const client2 = new Client(config.db);
            client2.connect(iferr(done, () => {
                client.query('SELECT pg_advisory_lock($1)', [ 0 ], iferr(done, r => {
                    expect(r.rows[0].pg_advisory_lock).to.equal('');
                    let locked1 = true;
                    let locked2 = false;
                    client2.query('SELECT pg_advisory_lock($1)', [ 0 ], iferr(done, r => {
                        expect(r.rows[0].pg_advisory_lock).to.equal('');
                        expect(locked1).to.be.false;
                        locked2 = true;
                        setTimeout(() => client.end(iferr(done, () => {
                            client2.end(done);
                        })), 1000);
                    }));
                    setTimeout(() => {
                        expect(locked2).to.be.false;
                        locked1 = false;
                        client.query('SELECT pg_advisory_unlock($1)', [ 0 ], iferr(done, () => {}));
                    }, 1000);
                }));
            }));
        }));
    });
});
