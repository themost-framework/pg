// eslint-disable-next-line no-unused-vars
import {DataApplication, DataConfigurationStrategy, NamedDataContext, DataCacheStrategy, DataContext, ODataModelBuilder, ODataConventionModelBuilder} from '@themost/data';
import { createInstance, PostgreSQLFormatter } from '../src';
import {createInstance as createConnectionPool, GenericPoolAdapter} from '@themost/pool';
import { TraceUtils, LangUtils } from '@themost/common';
import { QueryExpression } from '@themost/query';
import { SqliteAdapter } from '@themost/sqlite';
import path from 'path';

const testConnectionOptions = {
    'host': process.env.DB_HOST,
    'port': parseInt(process.env.DB_PORT, 10),
    'user': process.env.DB_USER,
    'logLevel': 'debug',
    'database': 'test_db'
};

if (process.env.DB_PASSWORD) {
    Object.assign(testConnectionOptions, {
        password: process.env.DB_PASSWORD
    });
}

const masterConnectionOptions = {
    'host': process.env.DB_HOST,
    'port': parseInt(process.env.DB_PORT, 10),
    'user': process.env.DB_USER,
    'logLevel': 'debug',
    'database': 'postgres'
};

if (process.env.DB_PASSWORD) {
    Object.assign(masterConnectionOptions, {
        password: process.env.DB_PASSWORD
    });
}

const sourceConnectionOptions = {
    database: path.resolve(__dirname, 'db/local.db')
};

class CancelTransactionError extends Error {
    constructor() {
        super();
    }
}

function resetLogLevel(db, level) {
    if (typeof db.logger.level === 'function') {
        db.logger.level(level);
    } else if (typeof db.logger.setLogLevel === 'function') {
        db.logger.setLevel(level);
    }
}

/**
 * @callback TestContextFunction
 * @param {DataContext} context
 * @returns {Promise<void>}
*/

class TestApplication extends DataApplication {
    constructor(cwd) {
        super(cwd);
        const dataConfiguration = this.configuration.getStrategy(DataConfigurationStrategy);
        // add adapter type
        const name = 'PostgreSQL Data Adapter';
        const invariantName = 'postgres';
        if (dataConfiguration.adapterTypes instanceof Map) {
            dataConfiguration.adapterTypes.set(invariantName, {
                name,
                invariantName,
                createInstance
            });
            dataConfiguration.adapterTypes.set('pool', {
                'name': 'PostgreSQL Connection Pool',
                'invariantName': 'pool',
                createInstance: (options) => {
                    const db = createConnectionPool(options);
                    db.logger = TraceUtils.newLogger();
                    return db;
                }
            });
        } else {
            // backward compatibility with old versions of @themost/data
            Object.defineProperty(dataConfiguration.adapterTypes, invariantName, {
                configurable: true,
                enumerable: true,
                writable: true,
                value: {
                    name,
                    invariantName,
                    createInstance
                }
            });
            Object.defineProperty(dataConfiguration.adapterTypes, 'pool', {
                configurable: true,
                enumerable: true,
                writable: true,
                value: {
                    'name': 'PostgreSQL Connection Pool',
                    'invariantName': 'pool',
                    createInstance: (options) => {
                        const db = createConnectionPool(options);
                        db.logger = TraceUtils.newLogger();
                        return db;
                    }
                }
            });
        }


        dataConfiguration.adapters.push({
            name: 'master',
            invariantName: 'postgres',
            default: false,
            options: masterConnectionOptions
        });
        dataConfiguration.adapters.push({
            name: 'test',
            invariantName: 'postgres',
            default: true,
            options: testConnectionOptions
        });
        dataConfiguration.adapters.push({
            name: 'test+with+connection+pool',
            invariantName: 'pool',
            default: false,
            options: {
                adapter: 'test'
            }
        });
    }
    async tryCreateDatabase() {
        let context = new NamedDataContext('master');
        context.getConfiguration = () => {
            return this.configuration;
        };
        if (typeof context.db.logger.level === 'function') {
            context.db.logger.level('error');
        } else if (typeof context.db.logger.setLogLevel === 'function') {
            context.db.logger.setLevel('error');
        }
        const exists = await context.db.database(testConnectionOptions.database).existsAsync();
        if (exists === false) {
            await context.db.executeAsync(`CREATE DATABASE ${testConnectionOptions.database};`);
        }
        await context.db.closeAsync();
    }

    async finalize() {
        const service = this.getConfiguration().getStrategy(DataCacheStrategy);
        if (typeof service.finalize === 'function') {
            await service.finalize();
        }
        const connectionPool = GenericPoolAdapter.pool('test');
        if (connectionPool) {
            // noinspection JSUnresolvedReference
            await connectionPool.drain();
            // noinspection JSUnresolvedReference
            await connectionPool.clear();
        }
    }

    finalizeAsync() {
        return this.finalize();
    }

    /**
     * @param {TestContextFunction} func 
     */
    async executeInTestContext(func) {
        const context = this.createContext();
        try {
            await func(context);
        } finally {
            if (context) {
                await context.finalizeAsync();
            }
        }
    }

    createContext() {
        const context = super.createContext();
        context.finalizeAsync = async function() {
            if (this.db) {
                await this.db.closeAsync();
            }
            this.db = null;
        };
        return context;
    }

    /**
     * @param {TestContextFunction} func 
     * @returns {Promise<void>}
     */
    executeInTestTransaction(func) {
        return this.executeInTestContext((context) => {
            return new Promise((resolve, reject) => {
                // start transaction
                context = this.createContext();
                // clear cache
                const configuration = context.getConfiguration();
                Object.defineProperty(configuration, 'cache', {
                    configurable: true,
                    enumerable: true,
                    writable: true,
                    value: { }
                });
                context.db.executeInTransaction((cb) => {
                    try {
                        func(context).then(() => {
                            return cb(new CancelTransactionError());
                        }).catch( (err) => {
                            return cb(err);
                        });
                    }
                    catch (err) {
                        return cb(err);
                    }
                }, (err) => {
                    context.finalizeAsync().finally(() => {
                        // if error is an instance of CancelTransactionError
                        if (err && err instanceof CancelTransactionError) {
                            return resolve();
                        }
                        if (err) {
                            return reject(err);
                        }
                        // exit
                        return resolve();
                    });
                });
            });
        });
    }

    async tryUpgrade() {
        let context;
        try {
            this.configuration.useStrategy(ODataModelBuilder, ODataConventionModelBuilder);
            context = this.createContext();
            resetLogLevel(context.db, 'error');
            const builder = this.configuration.getStrategy(ODataModelBuilder);
            const schema = await builder.getEdm();
            const entityTypes = schema.entityType.filter((item) => {
                return item.abstract ? false : true;
            });
            await context.executeInTransactionAsync(async () => {
                for (let entityType of entityTypes) {
                    TraceUtils.debug(`Upgrading ${entityType.name}`);
                    await new Promise((resolve, reject) => {
                        const model = context.model(entityType.name);
                        if (model.abstract) {
                            return resolve();
                        }
                        model.migrate(function (err) {
                            if (err) {
                                return reject(err);
                            }
                            return resolve();
                        });
                    });
                }
            });
            await context.finalizeAsync();
        } catch (error) {
            if (context) {
                await context.finalizeAsync();
            }
            throw error;
        }
    }

    async trySetData() {
        let context;
        try {
            this.configuration.useStrategy(ODataModelBuilder, ODataConventionModelBuilder);
            context = this.createContext();
            resetLogLevel(context.db, 'error');
            // validate if the operation has been already run
            const exists1 = await context.db.table('migrations').existsAsync();
            if (exists1 === true) {
                const alreadyApplied = await context.db.executeAsync(
                    new QueryExpression().select('version').from('migrations')
                        .where('appliesTo').equal('SetData').and('version').equal('1.0')
                    );
                if (alreadyApplied.length > 0) {
                    return;
                }
            }
            const builder = this.configuration.getStrategy(ODataModelBuilder);
            const schema = await builder.getEdm();
            const entityTypes = schema.entityType.filter((item) => {
                return item.abstract ? false : true;
            });
            const sourceAdapter = new SqliteAdapter(sourceConnectionOptions);
            resetLogLevel(sourceAdapter, 'error');
            for (let entityType of entityTypes) {
                TraceUtils.log(`Upgrading ${entityType.name}`);
                await new Promise((resolve, reject) => {
                    const model = context.model(entityType.name);
                    if (model.abstract) {
                        return resolve();
                    }
                    model.migrate(function (err) {
                        if (err) {
                            return reject(err);
                        }
                        (async function () {
                            const sourceTableExists = await sourceAdapter.table(model.sourceAdapter).existsAsync();
                            if (sourceTableExists) {
                                // get source data
                                let results = await sourceAdapter.executeAsync(`SELECT * FROM "${model.sourceAdapter}"`);
                                if (results.length > 0) {
                                    await context.db.executeAsync(`DELETE FROM "${model.sourceAdapter}" WHERE 1=1`);
                                    const formatter = new PostgreSQLFormatter();
                                    // get columns of type boolean
                                    // data should be update to true/false
                                    // because of an error occurred while trying to insert an integer value to a field of type boolean
                                    const booleanAttributes = model.attributes.filter((attribute) => attribute.type === 'Boolean');
                                    for (let result of results) {
                                        // modify data
                                        booleanAttributes.forEach((attribute) => {
                                            if (Object.prototype.hasOwnProperty.call(result, attribute.name)) {
                                                result[attribute.name] = LangUtils.parseBoolean(result[attribute.name]);
                                            }
                                        });
                                        const sql = formatter.format(new QueryExpression().insert(result).into(model.sourceAdapter));
                                        // and execute
                                        await context.db.executeAsync(sql);
                                    }
                                    const key = model.getAttribute(model.primaryKey);
                                    if (key.type === 'Counter') {
                                        // reset sequence
                                        await context.db.executeAsync(`select setval(pg_get_serial_sequence('"${model.sourceAdapter}"', '${key.name}'), (select max("${key.name}") from "${model.sourceAdapter}")); `);
                                    }
                                }
                            }
                        })().then(() => {
                            return resolve();
                        }).catch((err) => {
                            return reject(err);
                        });
                    });
                });
            }
            await context.db.executeAsync(new QueryExpression().insert({
                appliesTo: 'SetData',
                version: '1.0'
            }).into('migrations'));
            await context.finalizeAsync();
        } finally {
            if (context) {
                await context.finalizeAsync();
            }
        }
    }

}

export {
    TestApplication
}