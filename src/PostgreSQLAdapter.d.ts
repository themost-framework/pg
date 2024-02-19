import { DataAdapterBase, DataAdapterBaseHelper, DataAdapterDatabase, DataAdapterIndexes, DataAdapterMigration, DataAdapterTable, DataAdapterView } from '@themost/common';

export declare interface DataAdapterTables {
    list(callback: (err: Error, result: { name: string }[]) => void): void;
    listAsync(): Promise<{ name: string, owner?: string, schema?: string }[]>;
}

export declare interface DataAdapterViews {
    list(callback: (err: Error, result: { name: string }[]) => void): void;
    listAsync(): Promise<{ name: string, owner?: string, schema?: string }[]>;
}

export declare class PostgreSQLAdapter implements DataAdapterBase, DataAdapterBaseHelper {
    constructor(options?: any);
    table(name: string): DataAdapterTable;
    view(name: string): DataAdapterView;
    indexes(name: string): DataAdapterIndexes;
    database(name: string): DataAdapterDatabase;
    rawConnection?: any;
    options?: any;
    open(callback: (err?: Error) => void): void;
    openAsync(): Promise<void>;
    close(callback: (err?: Error) => void): void;
    closeAsync(): Promise<void>;
    execute(query: any, values: any, callback: (err: Error, result?: any) => void): void;
    executeAsync(query: any, values: any): Promise<any>;
    selectIdentity(entity: string, attribute: string, callback?: (err?: Error,result?: any) => void): void;
    selectIdentityAsync(entity: string, attribute: string): Promise<any>;
    executeInTransaction(func: () => void, callback: (err?: Error) => void): void;
    executeInTransactionAsync(func: () => Promise<void>): Promise<void>;
    migrate(obj: DataAdapterMigration, callback: (err: Error, result?: any) => void): void;
    migrateAsync(obj: DataAdapterMigration): Promise<any>;
    createView(name: string, query: any, callback: (err: Error) => void): void;
    tables(): DataAdapterTables;
    views(): DataAdapterViews;
}