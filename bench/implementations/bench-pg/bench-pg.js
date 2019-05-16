'use strict';
const async = require('async');
const crypto = require('crypto');
const rabbitmq_bindings = require('../../../test/fixtures/rabbitmq_bindings.js');

const options = JSON.parse(Buffer.from(process.argv[2], 'hex'));

const QlobberPG = require('../../..').QlobberPG;
const qpg = new QlobberPG(options);
const payload = crypto.randomBytes(options.size);
let expected = 0;
let published = false;

function handler() {
    if ((--expected === 0) && published) {
        process.exit();
    }
}

qpg.on('start', () => {
    const sub_topics = [];

    for (let i = 0; i < rabbitmq_bindings.test_bindings; ++i) {
        if (i % options.queues === options.n) {
            const b = rabbitmq_bindings.test_bindings[i];
            qpg.subscribe(b[0], handler);
            sub_topics.push(b[1]);
        }
    }

    for (let i = 0; i < rabbitmq_bindings.expected_results_before_remove; ++i) {
        for (let j = 0; j < sub_topics.length; ++j) {
            if (rabbitmq_bindings.expected_results_before_remove[i][1].indexOf(sub_topics[j]) > 0 ) {
                ++expected;
                break;
            }
        }
    }

    expected *= options.rounds;

    process.send({ type: 'ready' });
});

process.on('message', () => {
    async.timesSeries(options.rounds, (r, cb) => {
        async.times(rabbitmq_bindings.expected_results_before_remove.length, (i, cb) => {
            if (i % options.queues === options.n) {
                qpg.publish(
                    rabbitmq_bindings.expected_results_before_remove[i][0],
                    payload,
                    { ttl: options.ttl },
                    cb);
            } else {
                cb();
            }
        }, cb);
    }, () => {
        published = true;
        if (expected === 0) {
            process.exit();
        }
    });
});
