// MOST Web Framework Copyright (c) 2017-2022 THEMOST LP All Rights Reserved
import pg from 'pg';
import { QueryExpression, QueryField, SqlUtils } from '@themost/query';
import { TraceUtils } from '@themost/common';
import { PostgreSQLFormatter } from './PostgreSQLFormatter';
import { sprintf } from 'sprintf-js';

pg.types.setTypeParser(20, function(val) {
    return val === null ? null : parseInt(val);
});

pg.types.setTypeParser(1700, function(val) {
    return val === null ? null : parseFloat(val);
});


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
                return sprintf('postgres://%s:%s@%s:%s/%s',
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
                TraceUtils.log(sprintf('SQL (Execution Time:%sms): Connect', (new Date()).getTime() - startTime));
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
                    if (process.env.NODE_ENV === 'development' || process.env.NODE_ENV === 'test') {
                        startTime = new Date().getTime();
                    }
                    //execute raw command
                    self.rawConnection.query(prepared, null, function (err, result) {
                        if (process.env.NODE_ENV === 'development' || process.env.NODE_ENV === 'test') {
                            TraceUtils.log(sprintf('SQL (Execution Time:%sms):%s, Parameters:%s', (new Date()).getTime() - startTime, prepared, JSON.stringify(values)));
                        }
                        if (err) {
                            //log sql
                            TraceUtils.log(sprintf('SQL Error:%s', prepared));
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
     * Begins a data transaction and executes the given function
     * @param func {Function}
     */
     executeInTransactionAsync(func) {
        return new Promise((resolve, reject) => {
            return this.executeInTransaction((callback) => {
                return func.call(this).then(res => {
                    return callback(null, res);
                }).catch(err => {
                    return callback(err);
                });
            }, (err, res) => {
                if (err) {
                    return reject(err);
                }
                return resolve(res);
            });
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
    formatType(field, format) {
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
                s = sprintf('decimal(%s,%s)', (size > 0 ? size : 19), (scale > 0 ? scale : 4));
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
                s = size > 0 ? sprintf('varchar(%s)', size) : 'varchar(48)';
                break;
            case 'URL':
                if (size > 0)
                    s = sprintf('varchar(%s)', size);

                else
                    s = 'varchar';
                break;
            case 'Text':
                if (size > 0)
                    s = sprintf('varchar(%s)', size);

                else
                    s = 'varchar';
                break;
            case 'Note':
                if (size > 0)
                    s = sprintf('varchar(%s)', size);

                else
                    s = 'text';
                break;
            case 'Image':
            case 'Binary':
                s = size > 0 ? sprintf('bytea(%s)', size) : 'bytea';
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
        if (format === '%t') {
            return s;
        }
        if (format === 'alter') {
            s += (typeof field.nullable === 'undefined') ? ' DROP NOT NULL' : (field.nullable ? ' DROP NOT NULL' : ' SET NOT NULL');
        } else {
            s += (typeof field.nullable === 'undefined') ? ' NULL' : (field.nullable ? ' NULL' : ' NOT NULL');
        }
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
        return this.view(name).create(query, (err) => {
            return callback(err);
        });
    }

    /**
     * @param {string} name
     */
    table(name) {
        const self = this;
        let schema = 'public';
        let table = name;
        const matches = /(\w+)\.(\w+)/.exec(name);
        if (matches) {
            schema = matches[1];
            table = matches[2];
        }
        return {
            /**
             * @param {function(Error,Boolean=)} callback
             */
            exists: function (callback) {
                callback = callback || function () { };
                self.execute('SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=? AND table_type=\'BASE TABLE\' AND table_name=?',
                    [
                        schema,
                        table
                    ], function (err, result) {
                        if (err) {
                            return callback(err);
                        }
                        return callback(null, (result[0].count > 0));
                    });
            },
            existsAsync: function () {
                return new Promise((resolve, reject) => {
                    this.exists((err, value) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(value);
                    });
                });
            },
            /**
             * @param {function(Error,string=)} callback
             */
            version: function (callback) {
                self.execute('SELECT MAX("version") AS "version" FROM "migrations" WHERE "appliesTo"=?',
                    [
                        name
                    ], function (err, result) {
                        if (err) { callback(err); return; }
                        if (result.length === 0)
                            callback(null, '0.0');

                        else
                            callback(null, result[0].version || '0.0');
                    });
            },
            versionAsync: function () {
                return new Promise((resolve, reject) => {
                    this.version((err, value) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(value);
                    });
                });
            },
            /**
             * @param {function(Error,Boolean=)} callback
             */
            has_sequence: function (callback) {
                callback = callback || function () { };
                self.execute('SELECT COUNT(*) FROM information_schema.columns WHERE table_name=? AND table_schema=? AND ("column_default" ~ \'^nextval\\((.*?)\\)$\')',
                    [
                        table,
                        schema
                    ], function (err, result) {
                        if (err) { callback(err); return; }
                        callback(null, (result[0].count > 0));
                    });
            },
            /**
             * @param {function(Error,{name:string, ordinal:number, type:*, size:number, nullable:boolean }[]=)} callback
             */
            columns: function (callback) {
                callback = callback || function () { };
                self.execute('SELECT column_name AS "name", ordinal_position as "ordinal", data_type as "type",' +
                    'character_maximum_length as "size", is_nullable AS  "nullable", column_default AS "defaultValue"' +
                    ' FROM information_schema.columns WHERE table_name=? AND table_schema=?',
                    [
                        table,
                        schema
                    ], function (err, result) {
                        if (err) { 
                            return callback(err);
                        }
                        // format result
                        result.forEach((column) => {
                            column.nullable = (column.nullable === 'YES');
                        });
                        return callback(null, result);
                    });
            },
            columnsAsync: function () {
                return new Promise((resolve, reject) => {
                    this.columns((err, res) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(res);
                    });
                });
            },
            /**
             * @param {Array<*>} fields
             * @param callback
             */
             create: function (fields, callback) {
                callback = callback || function () { };
                fields = fields || [];
                if (!Array.isArray(fields)) {
                    return callback(new Error('Invalid argument type. Expected Array.'));
                }
                if (fields.length === 0) {
                    return callback(new Error('Invalid argument. Fields collection cannot be empty.'));
                }
                const formatter = new PostgreSQLFormatter();
                let strFields = fields.filter((x) => {
                    return !x.oneToMany;
                }).map((field) => {
                    const escapedField = formatter.escapeName(field.name);
                    return escapedField + ' ' + self.formatType(field);
                }).join(', ');

                //add primary key constraint
                const strPKFields = fields.filter((x) => {
                    return (x.primary === true || x.primary === 1);
                }).map((field) => {
                    return formatter.escapeName(field.name);
                }).join(', ');
                if (strPKFields.length > 0) {
                    strFields += ', ';
                    strFields += sprintf('PRIMARY KEY(%s)', strPKFields);
                }
                const escapedTable = new PostgreSQLFormatter().escapeName(name);
                const sql = sprintf('CREATE TABLE %s (%s)', escapedTable, strFields);
                self.execute(sql, null, function (err) {
                    callback(err);
                });
            },
            createAsync: function (fields) {
                return new Promise((resolve, reject) => {
                    this.create(fields, (err, res) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(res);
                    });
                });
            },
            /**
             * Alters the table by adding an array of fields
             * @param {Array<*>} fields
             * @param callback
             */
             add: function (fields, callback) {
                callback = callback || function () { };
                fields = fields || [];
                if (Array.isArray(fields) === false) {
                    //invalid argument exception
                    return callback(new Error('Invalid argument type. Expected Array.'));
                }
                if (fields.length === 0) {
                    // do nothing
                    return callback();
                }
                // generate SQL statement
                const formatter = new PostgreSQLFormatter();
                const escapedTable = new PostgreSQLFormatter().escapeName(name);
                const sql = fields.map((field) => {
                    const escapedField = formatter.escapeName(field.name);
                    return sprintf('ALTER TABLE %s ADD COLUMN %s %s', escapedTable, escapedField, self.formatType(field));
                }).join(';');
                self.execute(sql, [], function (err) {
                    callback(err);
                });
            },
            addAsync: function (fields) {
                return new Promise((resolve, reject) => {
                    this.add(fields, (err, res) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(res);
                    });
                });
            },
            /**
             * Alters the table by modifying an array of fields
             * @param {Array<*>} fields
             * @param callback
             */
             change: function (fields, callback) {
                callback = callback || function () { };
                fields = fields || [];
                if (Array.isArray(fields) === false) {
                    //invalid argument exception
                    return callback(new Error('Invalid argument type. Expected Array.'));
                }
                if (fields.length === 0) {
                    //do nothing
                    return callback();
                }
                //generate SQL statement
                const formatter = new PostgreSQLFormatter();
                const escapedTable = formatter.escapeName(name);
                let sql = fields.map((field) => {
                    const escapedType = self.formatType(field, '%t');
                    const escapedField = formatter.escapeName(field.name);
                    return sprintf('ALTER TABLE %s ALTER COLUMN %s TYPE %s;', escapedTable, escapedField, escapedType);
                }).join('');
                fields.forEach((field) => {
                    const escapedField = formatter.escapeName(field.name);
                    sql += sprintf('ALTER TABLE %s ALTER COLUMN %s %s;', escapedTable, escapedField, field.nullable ? 'DROP NOT NULL': 'SET NOT NULL');
                });
                self.execute(sql, [], function (err) {
                    callback(err);
                });
            },
            changeAsync: function (fields) {
                return new Promise((resolve, reject) => {
                    this.change(fields, (err, res) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(res);
                    });
                });
            },
        };
    }

    /**
     * Initializes database view helper.
     * @param {string} name - A string that represents the view name
     * @returns {*}
     */
     view(name) {
        const self = this;
        let schema = 'public';
        let view = name;
        const matches = /(\w+)\.(\w+)/.exec(name);
        if (matches) {
            //get schema owner
            schema = matches[1];
            //get table name
            view = matches[2];
        }
        return {
            /**
             * @param {Function} callback
             */
            exists: function (callback) {
                callback = callback || function () { };
                self.execute('SELECT COUNT(*) AS "count" FROM "information_schema"."tables" WHERE "table_schema"=? AND "table_type"=\'VIEW\' AND "table_name"=?',
                [
                    schema,
                    view
                ], function (err, result) {
                    if (err) {
                        return callback(err);
                    }
                    callback(null, result[0].count === 1);
                });
            },
            existsAsync: function () {
                return new Promise((resolve, reject) => {
                    this.exists((err, value) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(value);
                    });
                });
            },
            /**
             * @param {Function} callback
             */
            drop: function (callback) {
                callback = callback || function () { };
                self.open(function (err) {
                    if (err) {
                        return callback(err);
                    }
                    self.execute('SELECT COUNT(*) AS "count" FROM "information_schema"."tables" WHERE "table_schema"=? AND "table_type"=\'VIEW\' AND "table_name"=?',
                    [
                        schema,
                        view
                    ], function (err, result) {
                        if (err) {
                            return callback(err);
                        }
                        const exists = (result[0].count > 0);
                        if (exists) {
                            const formatter = new PostgreSQLFormatter();
                            const sql = sprintf('DROP VIEW %s', formatter.escapeName(name));
                            return self.execute(sql, [], function (err) {
                                if (err) {
                                    return callback(err);
                                }
                                return callback();
                            });
                        }
                        return callback();
                    });
                });
            },
            dropAsync: function () {
                return new Promise((resolve, reject) => {
                    this.drop((err) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve();
                    });
                });
            },
            /**
             * @param {QueryExpression|*} q
             * @param {Function} callback
             */
            create: function (q, callback) {
                const thisArg = this;
                self.executeInTransaction(function (transcactionCallback) {
                    return thisArg.drop((err) => {
                        if (err) {
                            return transcactionCallback(err);
                        }
                        try {
                            const formatter = new PostgreSQLFormatter();
                            const sql = sprintf('CREATE VIEW %s AS ', formatter.escapeName(name)) + formatter.format(q);
                            return self.execute(sql, [], (err) => {
                                return transcactionCallback(err);
                            });
                        }
                        catch (error) {
                            return transcactionCallback(error);
                        }
                    });
                }, (err) => {
                    return callback(err);
                });
            },
            createAsync: function (q) {
                return new Promise((resolve, reject) => {
                    this.create(q, (err) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve();
                    });
                });
            }
        };
    }

    /*
    * @param obj {{appliesTo: string, model: string, version: string, description: string, add: Array<*>,remove: Array<*>,change: Array<*>}} An Object that represents the data model scheme we want to migrate
    * @param callback {Function}
    */
    migrate(obj, callback) {
        if (obj === null) {
            return callback();
        }
        const self = this;
        const migration = obj;
        if (migration.appliesTo === null) {
            return callback(new Error('Target data object is undefined'));
        }
        //columns to be removed (deprecated + unsupported)
        if (Array.isArray(migration.remove)) {
            if (migration.remove.length > 0) {
                return callback(new Error('Data migration remove operation is not supported by this adapter.'));
            }
        }
        //columns to be changed (deprecated + unsupported)
        if (Array.isArray(migration.change)) {
            if (migration.change.length > 0) {
                return callback(new Error('Data migration change operation is not supported by this adapter. Use add collection instead.'));
            }
        }
        self.open(function (err) {
            if (err) {
                return callback(err);
            }
            else {
                (async function () {
                    let exists = await self.table('migrations').existsAsync();
                    if (exists === false) {
                        // create migration table
                        await self.executeAsync(`
                            CREATE TABLE migrations(
                                "id" SERIAL NOT NULL,
                                "appliesTo" varchar(80) NOT NULL,
                                "model" varchar(120) NULL,
                                "description" varchar(512),
                                "version" varchar(40) NOT NULL,
                                PRIMARY KEY("id"))
                        `);
                    }
                    const version = await self.table(migration.appliesTo).versionAsync();
                    if (version >= migration.version) {
                        // nothing to do
                        Object.assign(migration, {
                            updated: true
                        });
                        // exit
                        return -1; 
                    }
                    exists = await self.table(migration.appliesTo).existsAsync();
                    if (exists == false) {
                        // get columns
                        await self.table(migration.appliesTo).createAsync(migration.add);
                    } else {
                        // get columns
                        const columns = await self.table(migration.appliesTo).createAsync(migration.add);
                        const addColumns = [];
                        const updateColumns = [];
                        for (const field of migration.add) {
                             //check if field exists or not
                             const column = columns.find((item) => item.name === field.name);
                             if (column != null) {
                                 //get original field size
                                 const originalSize = column.maxLength;
                                 //and new field size
                                 const newSize = field.size;
                                 //add expression for modifying column (size)
                                 if (newSize != null && originalSize !== newSize) {
                                     updateColumns.push(field);
                                 }
                             }
                             else {
                                addColumns.push(field);
                             }
                        }
                        if (addColumns.length > 0) {
                            self.table(migration.appliesTo).addAsync(addColumns);
                        }
                        if (updateColumns.length > 0) {
                            self.table(migration.appliesTo).changeAsync(updateColumns);
                        }
                    }
                    await self.execute('INSERT INTO migrations("appliesTo", "model", "version", "description") VALUES (?,?,?,?)',
                    [
                        migration.appliesTo,
                        migration.model,
                        migration.version,
                        migration.description
                    ]);
                    Object.assign(migration, {
                        updated: true
                    });
                    return 1; 
                })().then((result) => {
                    return callback(null, result);
                }).catch((err) => {
                    return callback(err)
                });
            }
        });
    }

    /**
     * Table indexes helper
     * @param {string} name 
     */
     indexes(name) {
        const self = this;
        let schema = 'public';
        let table = name;
        const matches = /(\w+)\.(\w+)/.exec(name);
        if (matches) {
            //get schema owner
            schema = matches[1];
            //get table name
            table = matches[2];
        }
        const formatter = new PostgreSQLFormatter();
        return {
            list: function (callback) {
                const this1 = this;
                if (Object.prototype.hasOwnProperty.call(this1, '_indexes')) {
                    return callback(null, this1._indexes);
                }
                const sqlIndexes = 'SELECT indexname as "name" FROM pg_indexes WHERE schemaname=? AND tablename=?';
                const sqlIndexColumns = `
                    with ind_cols as (
                    select
                        n.nspname as "schema_name",
                        t.relname as "table_name",
                        i.relname as "index_name",
                        a.attname as "column_name",
                        1 + array_position(ix.indkey, a.attnum) as column_position
                    from
                        pg_catalog.pg_class t
                    join pg_catalog.pg_attribute a on t.oid = a.attrelid 
                    join pg_catalog.pg_index ix on t.oid = ix.indrelid
                    join pg_catalog.pg_class i on a.attnum = any(ix.indkey)
                    and i.oid = ix.indexrelid
                    join pg_catalog.pg_namespace n on n.oid = t.relnamespace
                    where t.relkind = 'r'
                    order by
                        t.relname,
                        i.relname,
                        array_position(ix.indkey, a.attnum)
                    )
                    select * 
                    from ind_cols
                    where schema_name = ?
                    and table_name  = ?
                    order by schema_name, table_name
                `;
                    (async () => {
                        const results = [];
                        results.push(await self.executeAsync(sqlIndexes, [
                            schema,
                            table
                        ]));
                        results.push(await self.executeAsync(sqlIndexColumns, [
                            schema,
                            table
                        ]));
                        return results;
                    })().then((results) => {
                    const indexes = results[0].map(function (index) {
                        return {
                            name: index.name,
                            columns: results[1].filter((y) => index.name === y.index_name).map((y) => y.column_name)
                        };
                    });
                    this1._indexes = indexes;
                    return callback(null, indexes);
                }).catch((err) => {
                    return callback(err);
                });
            },
            listAsync: function () {
                return new Promise((resolve, reject) => {
                    this.list((err, res) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(res);
                    });
                });
            },
            /**
             * @param {string} indexName
             * @param {Array|string} columns
             * @param {Function} callback
             */
            create: function (indexName, columns, callback) {
                const cols = [];
                if (typeof columns === 'string') {
                    cols.push(columns);
                }
                else if (Array.isArray(columns)) {
                    cols.push.apply(cols, columns);
                }
                else {
                    return callback(new Error('Invalid parameter. Columns parameter must be a string or an array of strings.'));
                }
                const thisArg = this;
                thisArg.list(function (err, indexes) {
                    if (err) {
                        return callback(err);
                    }
                    const findIndex = indexes.find((x) => {
                        return x.name === indexName;
                    });
                    //format create index SQL statement
                    const escapeColumns = cols.map(function (x) {
                        return formatter.escapeName(x);
                    }).join(',');
                    const sqlCreateIndex = `CREATE INDEX ${formatter.escapeName(indexName)} ON ${formatter.escapeName(name)} (${escapeColumns})`;
                    if (findIndex == null) {
                        self.execute(sqlCreateIndex, [], (err) => {
                            if (err) {
                                return callback(err);
                            }
                            return callback(null, 1);
                        });
                    }
                    else {
                        let nCols = cols.length;
                        //enumerate existing columns
                        findIndex.columns.forEach(function (x) {
                            if (cols.indexOf(x) >= 0) {
                                //column exists in index
                                nCols -= 1;
                            }
                        });
                        if (nCols > 0) {
                            //drop index
                            thisArg.drop(name, function (err) {
                                if (err) {
                                    return callback(err);
                                }
                                //and create it
                                return self.execute(sqlCreateIndex, [], (err) => {
                                    if (err) {
                                        return callback(err);
                                    }
                                    return callback(null, 1);
                                });
                            });
                        }
                        else {
                            //do nothing
                            return callback(null, 0);
                        }
                    }
                });
            },
            /**
             * @param {string} name
             * @param {Array|string} columns
             */
            createAsync: function (name, columns) {
                return new Promise((resolve, reject) => {
                    this.create(name, columns, (err, res) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(res);
                    });
                });
            },
            drop: function (indexName, callback) {
                const thisArg = this;
                if (typeof name !== 'string') {
                    return callback(new Error('Name must be a valid string.'));
                }
                self.execute('SELECT indexname as "name" FROM pg_indexes WHERE schemaname=? AND tablename=? AND indexname=?', [
                    schema,
                    table,
                    indexName
                ], function (err, result) {
                    if (err) {
                        return callback(err);
                    }
                    if (result.length === 0) {
                        return callback(null, 0);
                    }
                    self.execute(`DROP INDEX ${formatter.escapeName(indexName)}`, null, (err) => {
                        if (err) {
                            return callback(err);
                        }
                        // cleanup indexes
                        delete thisArg._indexes;
                        // and return
                        return callback(null, 1);
                    });
                });
            },
            dropAsync: function (name) {
                return new Promise((resolve, reject) => {
                    this.drop(name, (err, res) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(res);
                    });
                });
            }
        };
    }

}

export {
    PostgreSQLAdapter
}
