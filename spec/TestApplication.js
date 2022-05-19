// eslint-disable-next-line no-unused-vars
import {DataApplication, DataConfigurationStrategy, NamedDataContext, DataCacheStrategy, DataContext} from '@themost/data';
import { createInstance } from '../src';
import sqlite from '@themost/sqlite';

const testConnectionOptions = {
    'server': process.env.POSTGRES_HOST,
    'port': parseInt(process.env.POSTGRES_PORT, 10),
    'user': process.env.POSTGRES_USER,
    'database': process.env.POSTGRES_DB
};

const masterConnectionOptions = {
    'server': process.env.POSTGRES_HOST,
    'port': parseInt(process.env.POSTGRES_PORT, 10),
    'user': process.env.POSTGRES_USER,
    'database': 'postgres'
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
        Object.assign(dataConfiguration.adapterTypes, {
            postgres: {
                name,
                invariantName,
                createInstance
            }
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

    /**
     * @param {TestContextFunction} func 
     */
    async executeInTestContext(func) {
        const context = this.createContext();
        try {
            await func(context);
        } catch (err) {
            await context.finalizeAsync();
            throw err;
        }
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

}

export {
    TestApplication
}