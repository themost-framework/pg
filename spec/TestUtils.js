class CancelTransactionError extends Error {
    constructor() {
        super();
    }
}

class TestUtils {
    /**
     * @param {{db: DataAdapter}} context
     * @param {Function} func
     * @returns {Promise<any>}
     */
    static executeInTransaction(context, func) {
        return new Promise((resolve, reject) => {
            const configuration = context.getConfiguration();
                Object.defineProperty(configuration, 'cache', {
                    configurable: true,
                    enumerable: true,
                    writable: true,
                    value: { }
                });
            context.db.executeInTransaction((cb) => {
                try {
                    func().then(() => {
                        return cb(new CancelTransactionError());
                    }).catch( err => {
                        return cb(err);
                    });
                }
                catch (err) {
                    return cb(err);
                }
            }, err => {
                if (err && err instanceof CancelTransactionError) {
                    return resolve();
                }
                if (err) {
                    return reject(err);
                }
                return resolve();
            });
        });
    }

    static cancelTransaction() {
        throw new CancelTransactionError();
    }

}

export {
    CancelTransactionError,
    TestUtils
}
