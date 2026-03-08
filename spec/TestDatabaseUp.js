import {TestApplication} from './TestApplication';
(async function() {
    const app = new TestApplication(__dirname);
    await app.tryCreateDatabase();
    await app.trySetData();
})().then(() => {
    console.log('Database is up');
    process.exit(0);
}).catch((err) => {
    console.error(err);
    process.exit(1);
});