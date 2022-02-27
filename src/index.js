// MOST Web Framework Copyright (c) 2017-2022 THEMOST LP All Rights Reserved
import { PostgreSQLAdapter } from './PostgreSQLAdapter';
import { PostgreSQLFormatter } from './PostgreSQLFormatter';

function createInstance(options) {
    return new PostgreSQLAdapter(options);
}

export {
    PostgreSQLAdapter,
    PostgreSQLFormatter,
    createInstance
}