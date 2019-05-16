'use strict';
exports.up = pgm => {
    pgm.createExtension('ltree', {
        ifNotExists: true
    });

    pgm.createTable('messages', {
        id: {
            type: 'bigserial',
            primaryKey: true
        },
        topic: {
            type: 'ltree',
            notNull: true
        },
        expires: {
            type: 'timestamptz',
            notNull: true
        },
        single: {
            type: 'boolean',
            notNull: true
        },
        data: {
            type: 'bytea',
            notNull: true
        },
        publisher: {
            type: 'text',
            notNull: true
        }
    });

    pgm.createFunction('new_message', [], {
        returns: 'trigger',
        language: 'plpgsql'
    }, "BEGIN PERFORM pg_notify('new_message_' || TG_ARGV[0], ''); RETURN NULL; END;");
};

exports.down = pgm => {
    pgm.dropFunction('new_message', [], {
        cascade: true
    });

    pgm.dropTable('messages');

    pgm.dropExtension('ltree', {
        ifExists: true
    });
};
