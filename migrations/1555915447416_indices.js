'use strict';

exports.up = pgm => {
    pgm.createIndex('messages', 'topic', { method: 'gist' });
    pgm.createIndex('messages', 'expires');
    pgm.createIndex('messages', 'publisher');
};

exports.down = pgm => {
    pgm.dropIndex('messages', 'publisher');
    pgm.dropIndex('messages', 'expires');
    pgm.dropIndex('messages', 'topic');
};
