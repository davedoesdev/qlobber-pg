exports.up = pgm => {
    pgm.createIndex('messages', 'topic', { method: 'gist' });
    pgm.createIndex('messages', 'expires');
};
