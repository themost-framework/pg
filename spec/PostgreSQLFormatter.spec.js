import { TestApplication } from './TestApplication';

fdescribe('PostgreSQLFormatter', () => {
    /**
     * @type {TestApplication}
     */
    let app;
    beforeAll(async () => {
        app = new TestApplication(__dirname);
        await app.tryCreateDatabase();
        await app.tryUpgrade();
    });
    beforeEach(async () => {
        //
    });
    afterAll(async () => {
        await app.finalize();
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

});