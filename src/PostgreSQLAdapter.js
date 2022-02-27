// MOST Web Framework Copyright (c) 2017-2022 THEMOST LP All Rights Reserved
import pg from 'pg';
import async from 'async';
import util from 'util';
import _ from 'lodash';
import { QueryExpression, QueryField, SqlUtils } from "@themost/query";
import { TraceUtils } from "@themost/common";
import { PostgreSQLFormatter } from "./PostgreSQLFormatter";

pg.types.setTypeParser(20, function(val) {
    return val === null ? null : parseInt(val);
});

pg.types.setTypeParser(1700, function(val) {
    return val === null ? null : parseFloat(val);
});

/**
 * @class
 * @augments {DataAdapter}
 */


class PostgreSQLAdapter {
    /**
     * @constructor
     * @param {*} options
     */
    constructor(options) {
        this.rawConnection = null;
        /**
         * @type {*}
         */
        this.transaction = null;
        /**
         * @type {*}
         */
        this.options = options || {};
        if (typeof this.options.port === 'undefined')
            this.options.port = 5432;
        if (typeof this.options.host === 'undefined')
            this.options.host = 'localhost';
        //define connection string
        const self = this;
        Object.defineProperty(this, 'connectionString', {
            get: function () {
                return util.format('postgres://%s:%s@%s:%s/%s',
                    self.options.user,
                    self.options.password,
                    self.options.host,
                    self.options.port,
                    self.options.database);
            }, enumerable: false, configurable: false
        });
    }

    /**
     * Opens a new database connection
     * @param {function(Error=)} callback
     */
    connect(callback) {

        const self = this;
        callback = callback || function () { };
        if (self.rawConnection) {
            return callback();
        }
        self.rawConnection = new pg.Client(this.connectionString);

        let startTime;
        if (process.env.NODE_ENV === 'development') {
            startTime = new Date().getTime();
        }
        //try to connection
        self.rawConnection.connect(function (err) {
            if (err) {
                self.rawConnection = null;
                return callback(err);
            }
            if (process.env.NODE_ENV === 'development') {
                TraceUtils.log(util.format('SQL (Execution Time:%sms): Connect', (new Date()).getTime() - startTime));
            }
            //and return
            callback(err);
        });
    }

    /**
     * Opens a new database connection
     * @param {function(Error=)} callback
     */
    open(callback) {
        if (this.rawConnection) { 
            return callback();
        }
        return this.connect((err) => {
            return callback(err);
        });
    }

    /**
     * @returns Promise<void>
     */
    openAsync() {
        return new Promise((resolve, reject) => {
            return this.open((err) => {
                if (err) {
                    return reject(err);
                }
                return resolve();
            });
        });
    }

    /**
     * Closes the underlying database connection
     * @param {function(Error=)} callback
     */
    disconnect(callback) {
        if (typeof this.rawConnection === 'undefined' || this.rawConnection === null) {
            return callback();
        }
        try {
            //try to close connection
            this.rawConnection.end();
            if (this.rawConnection.connection && this.rawConnection.connection.stream) {
                if (typeof this.rawConnection.connection.stream.destroy === 'function') {
                    this.rawConnection.connection.stream.destroy();
                }
            }
            this.rawConnection = null;
            return callback();
        }
        catch (err) {
            TraceUtils.error('An error occurred while trying to close database connection.');
            TraceUtils.error(err);
            this.rawConnection = null;
            //do nothing (do not raise an error)
            return callback();
        }
    }

    /**
     * Closes the underlying database connection
     * @param {function(Error=)} callback
     */
    close(callback) {
        this.disconnect(callback);
    }

    /**
     * @returns Promise<void>
     */
     closeAsync() {
        return new Promise((resolve, reject) => {
            return this.close((err) => {
                if (err) {
                    return reject(err);
                }
                return resolve();
            });
        });
    }

    /**
     * @param {string} query
     * @param {*=} values
     */
    prepare(query, values) {
        return SqlUtils.format(query, values);
    }

    /**
     * Executes a query against the underlying database
     * @param {string|*} query
     * @param values {*=}
     * @param {function(Error=,*=)} callback
     */
    execute(query, values, callback) {
        const self = this;
        let sql = null;
        try {

            if (typeof query === 'string') {
                //get raw sql statement
                sql = query;
            }
            else {
                //format query expression or any object that may be act as query expression
                const formatter = new PostgreSQLFormatter();
                sql = formatter.format(query);
            }
            //validate sql statement
            if (typeof sql !== 'string') {
                callback.call(self, new Error('The executing command is of the wrong type or empty.'));
                return;
            }
            //ensure connection
            self.open(function (err) {
                if (err) {
                    callback.call(self, err);
                }
                else {
                    //log statement (optional)
                    let startTime;
                    const prepared = self.prepare(sql, values);
                    if (process.env.NODE_ENV === 'development') {
                        startTime = new Date().getTime();
                    }
                    //execute raw command
                    self.rawConnection.query(prepared, null, function (err, result) {
                        if (process.env.NODE_ENV === 'development') {
                            TraceUtils.log(util.format('SQL (Execution Time:%sms):%s, Parameters:%s', (new Date()).getTime() - startTime, prepared, JSON.stringify(values)));
                        }
                        if (err) {
                            //log sql
                            TraceUtils.log(util.format('SQL Error:%s', prepared));
                            callback(err);
                        }
                        else {
                            callback(null, result.rows);
                        }
                    });
                }
            });
        }
        catch (e) {
            callback.call(self, e);
        }
    }

    /**
     * @param {*} query
     * @param {*=} values
     * @returns Promise<void>
     */
     executeAsync(query, values) {
        return new Promise((resolve, reject) => {
            return this.execute(query, values, (err, results) => {
                if (err) {
                    return reject(err);
                }
                return resolve(results);
            });
        });
    }

    lastIdentity(callback) {
        const self = this;
        self.open(function (err) {
            if (err) {
                callback(err);
            }
            else {
                //execute lastval (for sequence)
                self.rawConnection.query('SELECT lastval()', null, function (err, lastval) {
                    if (err) {
                        callback(null, { insertId: null });
                    }
                    else {
                        lastval.rows = lastval.rows || [];
                        if (lastval.rows.length > 0)
                            callback(null, { insertId: lastval.rows[0]['lastval'] });

                        else
                            callback(null, { insertId: null });
                    }
                });
            }
        });
    }

    /**
     * Begins a database transaction and executes the given function
     * @param {function(Error=)} fn
     * @param {function(Error=)} callback
     */
    executeInTransaction(fn, callback) {
        const self = this;
        //ensure parameters
        fn = fn || function () { }; callback = callback || function () { };
        self.open(function (err) {
            if (err) {
                callback(err);
            }
            else {
                if (self.transaction) {
                    fn.call(self, function (err) {
                        callback(err);
                    });
                }
                else {
                    //begin transaction
                    self.rawConnection.query('BEGIN TRANSACTION;', null, function (err) {
                        if (err) {
                            callback(err);
                            return;
                        }
                        //initialize dummy transaction object (for future use)
                        self.transaction = {};
                        //execute function
                        fn.call(self, function (err) {
                            if (err) {
                                //rollback transaction
                                self.rawConnection.query('ROLLBACK TRANSACTION;', null, function () {
                                    self.transaction = null;
                                    callback(err);
                                });
                            }
                            else {
                                //commit transaction
                                self.rawConnection.query('COMMIT TRANSACTION;', null, function (err) {
                                    self.transaction = null;
                                    callback(err);
                                });
                            }
                        });
                    });
                }
            }
        });

    }

    /**
     * Produces a new identity value for the given entity and attribute.
     * @param entity {String} The target entity name
     * @param attribute {String} The target attribute
     * @param callback {Function=}
     */
    selectIdentity(entity, attribute, callback) {

        const self = this;

        const migration = {
            appliesTo: 'increment_id',
            model: 'increments',
            description: 'Increments migration (version 1.0)',
            version: '1.0',
            add: [
                { name: 'id', type: 'Counter', primary: true },
                { name: 'entity', type: 'Text', size: 120 },
                { name: 'attribute', type: 'Text', size: 120 },
                { name: 'value', type: 'Integer' }
            ]
        };
        //ensure increments entity
        self.migrate(migration, function (err) {
            //throw error if any
            if (err) { callback.call(self, err); return; }
            self.execute('SELECT * FROM increment_id WHERE entity=? AND attribute=?', [entity, attribute], function (err, result) {
                if (err) { callback.call(self, err); return; }
                if (result.length === 0) {
                    //get max value by querying the given entity
                    const q = new QueryExpression().from(entity).select([new QueryField().max(attribute)]);
                    self.execute(q, null, function (err, result) {
                        if (err) { return callback.call(self, err); }
                        let value = 1;
                        if (result.length > 0) {
                            value = parseInt(result[0][attribute]) + 1;
                        }
                        self.execute('INSERT INTO increment_id(entity, attribute, value) VALUES (?,?,?)', [entity, attribute, value], function (err) {
                            //throw error if any
                            if (err) { return callback.call(self, err); }
                            //return new increment value
                            callback.call(self, err, value);
                        });
                    });
                }
                else {
                    //get new increment value
                    const value = parseInt(result[0].value) + 1;
                    self.execute('UPDATE increment_id SET value=? WHERE id=?', [value, result[0].id], function (err) {
                        //throw error if any
                        if (err) { return callback.call(self, err); }
                        //return new increment value
                        callback.call(self, err, value);
                    });
                }
            });
        });
    }

    /**
     * Executes an operation against database and returns the results.
     * @param {DataModelBatch} batch
     * @param {Function} callback
     * @deprecated DataAdapter.executeBatch() is obsolete. Use DataAdapter.executeInTransaction() instead.
     */
    executeBatch(batch, callback) {
        callback = callback || function () { };
        callback(new Error('DataAdapter.executeBatch() is obsolete. Use DataAdapter.executeInTransaction() instead.'));
    }

    /**
     *
     * @param {*|{type:string, size:number, nullable:boolean}} field
     * @param {string=} format
     * @returns {string}
     */
    static formatType(field, format) {
        const size = parseInt(field.size);
        const scale = parseInt(field.scale);
        let s = 'varchar(512) NULL';
        const type = field.type;
        switch (type) {
            case 'Boolean':
                s = 'boolean';
                break;
            case 'Byte':
                s = 'smallint';
                break;
            case 'Number':
            case 'Float':
                s = 'real';
                break;
            case 'Counter':
                return 'SERIAL';
            case 'Currency':
            case 'Decimal':
                s = util.format('decimal(%s,%s)', (size > 0 ? size : 19), (scale > 0 ? scale : 4));
                break;
            case 'Date':
                s = 'date';
                break;
            case 'DateTime':
                s = 'timestamp';
                break;
            case 'Time':
                s = 'time';
                break;
            case 'Integer':
                s = 'int';
                break;
            case 'Duration':
                s = size > 0 ? util.format('varchar(%s)', size) : 'varchar(48)';
                break;
            case 'URL':
                if (size > 0)
                    s = util.format('varchar(%s)', size);

                else
                    s = 'varchar';
                break;
            case 'Text':
                if (size > 0)
                    s = util.format('varchar(%s)', size);

                else
                    s = 'varchar';
                break;
            case 'Note':
                if (size > 0)
                    s = util.format('varchar(%s)', size);

                else
                    s = 'text';
                break;
            case 'Image':
            case 'Binary':
                s = size > 0 ? util.format('bytea(%s)', size) : 'bytea';
                break;
            case 'Guid':
                s = 'uuid';
                break;
            case 'Short':
                s = 'smallint';
                break;
            default:
                s = 'integer';
                break;
        }
        if (format === 'alter')
            s += (typeof field.nullable === 'undefined') ? ' DROP NOT NULL' : (field.nullable ? ' DROP NOT NULL' : ' SET NOT NULL');

        else
            s += (typeof field.nullable === 'undefined') ? ' NULL' : (field.nullable ? ' NULL' : ' NOT NULL');
        return s;
    }

    refreshView(name, query, callback) {
        const formatter = new PostgreSQLFormatter();
        this.execute('REFRESH MATERIALIZED VIEW ' + formatter.escapeName(name), null, function (err) {
            callback(err);
        });
    }

    /**
     * @param query {QueryExpression}
     */
    createView(name, query, callback) {
        const self = this;
        //open database
        self.open(function (err) {
            if (err) {
                callback.call(self, err);
                return;
            }
            //begin transaction
            self.executeInTransaction(function (tr) {
                async.waterfall([
                    function (cb) {
                        self.execute('SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=\'public\' AND table_type=\'VIEW\' AND table_name=?', [name], function (err, result) {
                            if (err) { throw err; }
                            if (result.length === 0)
                                return cb(null, 0);
                            cb(null, result[0].count);
                        });
                    },
                    function (arg, cb) {
                        if (arg === 0) { cb(null, 0); return; }
                        //format query
                        const sql = util.format("DROP VIEW \"%s\"", name);
                        self.execute(sql, null, function (err) {
                            if (err) { throw err; }
                            cb(null, 0);
                        });
                    },
                    function (arg, cb) {
                        //format query
                        const formatter = new PostgreSQLFormatter();
                        const sql = util.format("CREATE VIEW \"%s\" AS %s", name, formatter.format(query));
                        self.execute(sql, null, function (err) {
                            if (err) { throw err; }
                            cb(null, 0);
                        });
                    }
                ], function (err) {
                    if (err) { tr(err); return; }
                    tr(null);
                });
            }, function (err) {
                callback(err);
            });
        });

    }

    /**
     * @class DataModelMigration
     * @property {string} name
     * @property {string} description
     * @property {string} model
     * @property {string} appliesTo
     * @property {string} version
     * @property {array} add
     * @property {array} remove
     * @property {array} change
     */
    /**
     * @param {string} name
     * @returns {{exists: Function}}
     */
    table(name) {
        const self = this;
        return {
            /**
             * @param {function(Error,Boolean=)} callback
             */
            exists: function (callback) {
                callback = callback || function () { };
                self.execute('SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=\'public\' AND table_type=\'BASE TABLE\' AND table_name=?',
                    [name], function (err, result) {
                        if (err) { callback(err); return; }
                        callback(null, (result[0].count > 0));
                    });
            },
            /**
             * @param {function(Error,string=)} callback
             */
            version: function (callback) {
                self.execute('SELECT MAX("version") AS version FROM migrations WHERE "appliesTo"=?',
                    [name], function (err, result) {
                        if (err) { callback(err); return; }
                        if (result.length === 0)
                            callback(null, '0.0');

                        else
                            callback(null, result[0].version || '0.0');
                    });
            },
            /**
             * @param {function(Error,Boolean=)} callback
             */
            has_sequence: function (callback) {
                callback = callback || function () { };
                self.execute('SELECT COUNT(*) FROM information_schema.columns WHERE table_name=? AND table_schema=\'public\' AND ("column_default" ~ \'^nextval\\((.*?)\\)$\')',
                    [name], function (err, result) {
                        if (err) { callback(err); return; }
                        callback(null, (result[0].count > 0));
                    });
            },
            /**
             * @param {function(Error,{columnName:string,ordinal:number,dataType:*, maxLength:number,isNullable:number }[]=)} callback
             */
            columns: function (callback) {
                callback = callback || function () { };
                self.execute('SELECT column_name AS "columnName", ordinal_position as "ordinal", data_type as "dataType",' +
                    'character_maximum_length as "maxLength", is_nullable AS  "isNullable", column_default AS "defaultValue"' +
                    ' FROM information_schema.columns WHERE table_name=?',
                    [name], function (err, result) {
                        if (err) { callback(err); return; }
                        callback(null, result);
                    });
            }
        };
    }

    /*
    * @param obj {DataModelMigration|*} An Object that represents the data model scheme we want to migrate
    * @param callback {Function}
    */
    migrate(obj, callback) {
        if (obj === null)
            return;
        const self = this;
        /**
         * @type {DataModelMigration|*}
         */
        const migration = obj;

        const format = function (format, obj) {
            let result = format;
            if (/%t/.test(format))
                result = result.replace(/%t/g, PostgreSQLAdapter.formatType(obj));
            if (/%f/.test(format))
                result = result.replace(/%f/g, obj.name);
            return result;
        };

        if (migration.appliesTo === null)
            throw new Error("Model name is undefined");
        self.open(function (err) {
            if (err) {
                callback.call(self, err);
            }
            else {
                async.waterfall([
                    //1. Check migrations table existence
                    function (cb) {
                        if (PostgreSQLAdapter.supportMigrations) {
                            cb(null, 1);
                            return;
                        }
                        self.table('migrations').exists(function (err, exists) {
                            if (err) { cb(err); return; }
                            cb(null, exists);
                        });
                    },
                    //2. Create migrations table if not exists
                    function (arg, cb) {
                        if (arg > 0) { cb(null, 0); return; }
                        //create migrations table
                        self.execute('CREATE TABLE migrations(id SERIAL NOT NULL, ' +
                            '"appliesTo" varchar(80) NOT NULL, "model" varchar(120) NULL, "description" varchar(512),"version" varchar(40) NOT NULL)',
                            ['migrations'], function (err) {
                                if (err) { cb(err); return; }
                                PostgreSQLAdapter.supportMigrations = true;
                                cb(null, 0);
                            });
                    },
                    //3. Check if migration has already been applied
                    function (arg, cb) {
                        self.table(migration.appliesTo).version(function (err, version) {
                            if (err) { cb(err); return; }
                            cb(null, (version >= migration.version));
                        });

                    },
                    //4a. Check table existence
                    function (arg, cb) {
                        //migration has already been applied (set migration.updated=true)
                        if (arg) {
                            obj['updated'] = true;
                            return cb(null, -1);
                        }
                        else {
                            self.table(migration.appliesTo).exists(function (err, exists) {
                                if (err) { cb(err); return; }
                                cb(null, exists ? 1 : 0);
                            });

                        }
                    },
                    //4b. Get table columns
                    function (arg, cb) {
                        //migration has already been applied
                        if (arg < 0) { cb(null, [arg, null]); return; }
                        self.table(migration.appliesTo).columns(function (err, columns) {
                            if (err) { cb(err); return; }
                            cb(null, [arg, columns]);
                        });
                    },
                    //5. Migrate target table (create or alter)
                    function (args, cb) {
                        //migration has already been applied
                        if (args[0] < 0) { cb(null, args[0]); return; }
                        const columns = args[1];
                        if (args[0] === 0) {
                            //create table and
                            const strFields = _.map(_.filter(migration.add, (x) => {
                                return !x.oneToMany;
                            }), (x) => {
                                return format('"%f" %t', x);
                            }).join(', ');
                            const key = _.find(migration.add, (x) => { return x.primary; });
                            const sql = util.format('CREATE TABLE "%s" (%s, PRIMARY KEY("%s"))', migration.appliesTo, strFields, key.name);
                            self.execute(sql, null, function (err) {
                                if (err) { return cb(err); }
                                return cb(null, 1);
                            });

                        }
                        else {
                            const expressions = [];
                            let column;
                            let fname;
                            const findColumnFunc = (name) => {
                                return _.find(columns, (x) => {
                                    return x.columnName === name;
                                });
                            };
                            //1. enumerate fields to delete
                            if (migration.remove) {
                                for (let i = 0; i < migration.remove.length; i++) {
                                    fname = migration.remove[i].name;
                                    column = findColumnFunc(fname);
                                    if (typeof column !== 'undefined') {
                                        let k = 1, deletedColumnName = util.format('xx%s1_%s', k.toString(), column.columnName);
                                        while (typeof findColumnFunc(deletedColumnName) !== 'undefined') {
                                            k += 1;
                                            deletedColumnName = util.format('xx%s_%s', k.toString(), column.columnName);
                                        }
                                        expressions.push(util.format('ALTER TABLE "%s" RENAME COLUMN "%s" TO %s', migration.appliesTo, column.columnName, deletedColumnName));
                                    }
                                }
                            }
                            //2. enumerate fields to add
                            let newSize, originalSize, fieldName, nullable;
                            if (migration.add) {
                                for (let i = 0; i < migration.add.length; i++) {
                                    //get field name
                                    fieldName = migration.add[i].name;
                                    //check if field exists or not
                                    column = findColumnFunc(fieldName);
                                    if (typeof column !== 'undefined') {
                                        //get original field size
                                        originalSize = column.maxLength;
                                        //and new field size
                                        newSize = migration.add[i].size;
                                        //add expression for modifying column (size)
                                        if ((typeof newSize !== 'undefined') && (originalSize !== newSize)) {
                                            expressions.push(util.format('UPDATE pg_attribute SET atttypmod = %s+4 WHERE attrelid = \'"%s"\'::regclass AND attname = \'%s\';', newSize, migration.appliesTo, fieldName));
                                        }
                                        //update nullable attribute
                                        nullable = (typeof migration.add[i].nullable !== 'undefined') ? migration.add[i].nullable : true;
                                        expressions.push(util.format('ALTER TABLE "%s" ALTER COLUMN "%s" %s', migration.appliesTo, fieldName, (nullable ? 'DROP NOT NULL' : 'SET NOT NULL')));
                                    }
                                    else {
                                        //add expression for adding column
                                        expressions.push(util.format('ALTER TABLE "%s" ADD COLUMN "%s" %s', migration.appliesTo, fieldName, PostgreSQLAdapter.formatType(migration.add[i])));
                                    }
                                }
                            }

                            //3. enumerate fields to update
                            if (migration.change) {
                                for (let i = 0; i < migration.change.length; i++) {
                                    const change = migration.change[i];
                                    column = findColumnFunc(change);
                                    if (typeof column !== 'undefined') {
                                        //important note: Alter column operation is not supported for column types
                                        expressions.push(util.format('ALTER TABLE "%s" ALTER COLUMN "%s" TYPE %s', migration.appliesTo, migration.add[i].name, PostgreSQLAdapter.formatType(migration.change[i])));
                                    }
                                }
                            }

                            if (expressions.length > 0) {
                                self.execute(expressions.join(';'), null, function (err) {
                                    if (err) { cb(err); return; }
                                    return cb(null, 1);
                                });
                            }

                            else
                                cb(null, 2);
                        }
                    }, function (arg, cb) {

                        if (arg > 0) {
                            //log migration to database
                            self.execute('INSERT INTO migrations("appliesTo", "model", "version", "description") VALUES (?,?,?,?)', [migration.appliesTo,
                            migration.model,
                            migration.version,
                            migration.description], function (err) {
                                if (err)
                                    throw err;
                                return cb(null, 1);
                            });
                        }
                        else {
                            migration['updated'] = true;
                            cb(null, arg);
                        }
                    }
                ], function (err, result) {
                    callback(err, result);
                });
            }
        });
    }
}

export {
    PostgreSQLAdapter
}
