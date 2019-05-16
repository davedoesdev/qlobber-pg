'use strict';
const options = JSON.parse(Buffer.from(process.argv[2], 'hex'));
const { queue } = require('async');
const { QlobberPG } = require('../..');
const host = require('os').hostname();
const cbs = {};
let cb_count = 0;
let handlers = {};

//console.log(options);
const qpg = new QlobberPG(options);

function sum(buf) {
    let r = 0;
    for (let b of buf) {
        r += b;
    }
    return r;
}

const send_queue = queue((msg, cb) => process.send(msg, cb));

function send(msg) {
    send_queue.push(msg);
}

qpg.on('start', function () {
    if (options.index === 0) {
        return qpg._queue.push(cb => {
            qpg._client.query('DELETE FROM messages', cb);
        }, err => {
            send({
                type: 'start',
                err
            });
        });
    }
    send({ type: 'start' });
});

qpg.on('stop', function () {
    send({ type: 'stop' });
});

process.on('message', function (msg) {
    //console.log("RECEIVED MESSAGE FROM PARENT", options.index, msg);
    if (msg.type === 'subscribe') {
        handlers[msg.handler] = function (data, info, cb) {
            cbs[cb_count] = cb;
            info.id = Number(info.id);
            send({
                type: 'received',
                handler: msg.handler,
                sum: sum(data),
                info,
                cb: cb_count,
                host,
                pid: process.pid
            });
            ++cb_count;
        };

        qpg.subscribe(msg.topic, handlers[msg.handler], function () {
            send({
                type: 'sub_callback',
                cb: msg.cb
            });
        });
    } else if (msg.type === 'recv_callback') {
        cbs[msg.cb](msg.err);
        delete cbs[msg.cb];
    } else if (msg.type === 'publish') {
        qpg.publish(msg.topic, Buffer.from(msg.payload, 'base64'), msg.options, err => {
            send({
                type: 'pub_callback',
                cb: msg.cb,
                err
            });
        });
    } else if (msg.type === 'stop') {
        qpg.stop();
    } else if (msg.type === 'exit') {
        process.exit();
    } else if (msg.type === 'unsubscribe') {
        if (msg.topic) {
            qpg.unsubscribe(msg.topic, handlers[msg.handler], function () {
                delete handlers[msg.handler];
                send({
                    type: 'unsub_callback',
                    cb: msg.cb
                });
            });
        } else {
            qpg.unsubscribe(function () {
                handlers = {};
                send({
                    type: 'unsub_callback',
                    cb: msg.cb
                });
            });
        }
    }
});
