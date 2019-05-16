/* global before */
'use strict';
const child_process = require('child_process');
const path = require('path');
const os = require('os');
const cp_remote = require('cp-remote');
const async = require('async');
const config = require('config');
const yargs = require('yargs');
const argv = yargs(JSON.parse(Buffer.from(yargs.argv.data, 'hex')))
    .demand('rounds')
    .demand('size')
    .demand('ttl')
    .check(argv => {
        if (!(argv.queues || argv.remote)) {
            throw new Error('missing --queues or --remote');
        }
        if (argv.queues && argv.remote) {
            throw new Error("can't specify --queues and --remote");
        }
        return true;
    })
    .argv;

function error(err) {
    throw err;
}

if (argv.remote) {
    if (typeof argv.remote == 'string') {
        argv.remote = [argv.remote];
    }
    argv.queues = argv.remote.length;
}

// b doesn't pass down env and node-postgres defaults user to $USERNAME
config.db.user = os.userInfo().username;

let queues;

before((times, done) => {
    async.times(argv.queues, (n, cb) => {
        const bench_pg = path.join(__dirname, 'bench-pg', 'bench-pg.js');
        const opts = Buffer.from(JSON.stringify(Object.assign({
            name: `bench${n}`,
            n: n,
            queues: argv.queues,
            rounds: argv.rounds,
            size: argv.size,
            ttl: argv.ttl * 1000
        }, config))).toString('hex');

        let child;
        if (argv.remote) {
            child = cp_remote.run(argv.remote[n], bench_pg, opts);
        } else {
            child = child_process.fork(bench_pg, [opts]);
        }

        child.on('error', error);
        child.on('exit', error);

        child.on('message', () => cb(null, child));
    }, (err, qs) => {
        queues = qs;
        done(err);
    });
});

exports.publish = function (done) {
    async.each(queues, (q, cb) => {
        q.removeListener('exit', error);
        q.on('exit', (code, signal) => {
            cb(code || signal);
        });
        q.send({ type: 'start' });
    }, done);
};
