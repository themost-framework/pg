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
            },
            {
                name: 'description',
                type: 'Text',
                size: 255,
                nullable: true
            }
        ]);
        exists = await db.table('Table1').existsAsync();
        expect(exists).toBeTruthy();
        // get columns
        const columns = await db.table('Table1').columnsAsync();
        expect(columns).toBeInstanceOf(Array);
        let column = columns.find((col) => col.name === 'id' );
        expect(column).toBeTruthy();
        expect(column.nullable).toBeFalsy();
        column = columns.find((col) => col.name === 'description' );
        expect(column).toBeTruthy();
        expect(column.nullable).toBeTruthy();
        expect(column.size).toBe(255);
        await db.executeAsync(`DROP TABLE ${new PostgreSQLFormatter().escapeName('Table1')}`);
    });

    it('should alter table', async () => {
        let exists = await db.table('Table2').existsAsync();
        expect(exists).toBeFalsy();
        await db.table('Table2').createAsync([
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
        exists = await db.table('Table2').existsAsync();
        expect(exists).toBeTruthy();
        await db.table('Table2').addAsync([
            {
                name: 'description',
                type: 'Text',
                size: 255,
                nullable: true
            }
        ]);
        // get columns
        let columns = await db.table('Table2').columnsAsync();
        expect(columns).toBeInstanceOf(Array);
        let column = columns.find((col) => col.name === 'description' );
        expect(column).toBeTruthy();

        await db.table('Table2').changeAsync([
            {
                name: 'description',
                type: 'Text',
                size: 512,
                nullable: true
            }
        ]);
        columns = await db.table('Table2').columnsAsync();
        column = columns.find((col) => col.name === 'description' );
        expect(column.size).toEqual(512);
        expect(column.nullable).toBeTruthy();
        await db.executeAsync(`DROP TABLE ${new PostgreSQLFormatter().escapeName('Table2')}`);
    });
});