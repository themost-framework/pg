import { PostgreSQLAdapter } from '../src/PostgreSQLAdapter';
import { PostgreSQLFormatter } from '../src/PostgreSQLFormatter';

const testConnection = {
    host: process.env.POSTGRES_HOST,
    port: process.env.POSTGRES_PORT,
    database: process.env.POSTGRES_DB,
    user: process.env.POSTGRES_USER
};

describe('PostgreSQLAdapter', () => {
    /**
     * @type {PostgreSQLAdapter}
     */
    let db;
    beforeEach(async () => {
        db = new PostgreSQLAdapter(testConnection);
    });
    afterEach(async () => {
        if (db == null) {
            return;
        }
        await db.closeAsync();
    })
    it('should create instance', async () => {
        await expect(db.openAsync()).resolves.toBe(undefined);
    });
    it('should list databases', async () => {
        /**
         * @type {Array<{name: string}>}
         */
        const databases = await db.executeAsync('SELECT datname AS "name" FROM pg_database;');
        expect(databases).toBeInstanceOf(Array);
        expect(databases.length).toBeTruthy();
    });

    it('should validate table existence', async () => {
        const exists = await db.table('Table1').existsAsync();
        expect(exists).toBeFalsy();
    });

    it('should create table', async () => {
        let exists = await db.table('Table1').existsAsync();
        expect(exists).toBeFalsy();
        await db.table('Table1').createAsync([
            {
                name: 'id',
                type: 'Counter',
                primary: true,
                nullable: false
            },
            {
                name: 'name',
                type: 'Text',
                size: 255,
                nullable: false
            }
        ]);
        exists = await db.table('Table1').existsAsync();
        expect(exists).toBeTruthy();
        await db.executeAsync(`DROP TABLE ${new PostgreSQLFormatter().escapeName('Table1')}`);
    });
});