// eslint-disable-next-line no-unused-vars
import {DataApplication, DataConfigurationStrategy, NamedDataContext, DataCacheStrategy, DataContext, ODataModelBuilder, ODataConventionModelBuilder} from '@themost/data';
import { createInstance, PostgreSQLFormatter } from '../src';
import { TraceUtils, LangUtils } from '@themost/common';
import { QueryExpression } from '@themost/query';
import { SqliteAdapter } from '@themost/sqlite';
import path from 'path';

const testConnectionOptions = {
    'server': process.env.DB_HOST,
    'port': parseInt(process.env.DB_PORT, 10),
    'user': process.env.DB_USER,
    'database': 'test_db'
};

if (process.env.DB_PASSWORD) {
    Object.assign(testConnectionOptions, {
        password: process.env.DB_PASSWORD
    });
}

const masterConnectionOptions = {
    'server': process.env.DB_HOST,
    'port': parseInt(process.env.DB_PORT, 10),
    'user': process.env.DB_USER,
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
        dataConfiguration.adapterTypes.set(invariantName, {
            name,
            invariantName,
            createInstance
        });
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
    }
    async tryCreateDatabase() {
        let context = new NamedDataContext('master');
        context.getConfiguration = () => {
            return this.configuration;
        };
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
    executeInTestTranscaction(func) {
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