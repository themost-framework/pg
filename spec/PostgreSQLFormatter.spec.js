import { PostgreSQLFormatter } from '../src';
import { TestApplication } from './TestApplication';

describe('PostgreSQLFormatter', () => {
    /**
     * @type {TestApplication}
     */
    let app;
    beforeAll(async () => {
        app = new TestApplication(__dirname);
        await app.tryCreateDatabase();
        await app.trySetData();
        
    });
    beforeEach(async () => {
        //
    });
    afterAll(async () => {
        await app.finalizeAsync();
    });
    afterEach(async () => {
        //
    });

    it('should get data', async () => {
        await app.executeInTestTranscaction(async (context) => {
            const items = await context.model('ActionStatusType').silent().getItems();
            expect(items).toBeInstanceOf(Array);
            expect(items.length).toBeTruthy();
        });
    });

    it('should query data', async () => {
        await app.executeInTestTranscaction(async (context) => {
            const item = await context.model('ActionStatusType')
                .where('alternateName').equal('ActiveActionStatus').silent().getItem();
            expect(item).toBeTruthy();
            expect(item.alternateName).toEqual('ActiveActionStatus');
        });
    });

    it('should escape constant', async () => {
        const formatter = new PostgreSQLFormatter();
        expect(formatter.escapeConstant(10.45)).toEqual('10.45::float');
        expect(formatter.escapeConstant('test')).toEqual('\'test\'::text');
        expect(formatter.escapeConstant(true)).toEqual('true::bool');
        expect(formatter.escapeConstant(new Date('2019-05-15 12:45:00'))).toEqual('\'2019-05-15 12:45:00.000\'::timestamp');
    });

    it('should should use limit select', async () => {
        await app.executeInTestContext(async (context) => {
            const items = await context.model('Person').asQueryable()
                .orderBy('id')
                .take(5).silent().getItems();
            expect(items).toBeInstanceOf(Array);
            expect(items.length).toBe(5);
            const moreItems = await context.model('Person').asQueryable()
                .orderBy('id')
                .take(1).skip(5).silent().getItems();
            expect(moreItems.length).toBe(1);
            const nextItem = moreItems[0];
            for (const item of items) {
                expect(nextItem.id).toBeGreaterThan(item.id);
            }
        });        
    });

    it('should use insert', async () => {
        await app.executeInTestTranscaction(async (context) => {
            const insertUser = {
                name: 'user1@example.com',
                description: 'Test User',
                groups: [
                    {
                        name: 'Users'
                    }
                ]
            };
            await context.model('User').silent().insert(insertUser);
            let newUser = await context.model('User').where('name').equal('user1@example.com').silent().getItem();
            expect(newUser).toBeTruthy();
            expect(newUser.id).toEqual(insertUser.id);
        });
    });

    it('should use delete', async () => {
        await app.executeInTestTranscaction(async (context) => {
            const insertUser = {
                name: 'user1@example.com',
                description: 'Test User'
            };
            await context.model('User').silent().insert(insertUser);
            let newUser = await context.model('User').where('name').equal('user1@example.com').silent().getItem();
            await context.model('User').silent().remove(newUser);
            newUser = await context.model('User').where('name').equal('user1@example.com').silent().getItem();
            expect(newUser).toBeFalsy();
        });
    });

    it('should use count', async () => {
        await app.executeInTestTranscaction(async (context) => {

            await context.model('User').silent().save({
                name: 'admin1@example.com',
                groups: [
                    {
                        name: 'Administrators'
                    }
                ]
            })

            Object.assign(context, {
                user: {
                    name: 'admin1@example.com'
                }
            });
            const items = await context.model('Order').select(
                    'orderedItem/name as product',
                    'count(id) as total'
                ).groupBy('orderedItem/name')
                .getItems();
            expect(items).toBeInstanceOf(Array);
            expect(items.length).toBeGreaterThan(0);
        });
    });

});