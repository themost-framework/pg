import { PostgreSQLAdapter } from '../src/PostgreSQLAdapter';

const testConnection = {
  host: process.env.POSTGRES_HOST,
  port: process.env.POSTGRES_PORT,
  database: process.env.POSTGRES_DB
};

describe('PostgreSQLAdapter', () => {
    it('should create instance', async () => {
        const adapter = new PostgreSQLAdapter(testConnection);
        expect(adapter).toBeTruthy();
        await expect(adapter.openAsync()).resolves.toBe(undefined);
        await adapter.closeAsync();
    });
    it('should list databases', async () => {
        const adapter = new PostgreSQLAdapter(testConnection);
        expect(adapter).toBeTruthy();
        await adapter.openAsync();
        /**
         * @type {Array<{name: string}>}
         */
        const databases = await adapter.executeAsync('SELECT datname AS "name" FROM pg_database;');
        expect(databases).toBeInstanceOf(Array);
        expect(databases.length).toBeTruthy();
        await adapter.closeAsync();
    });
});