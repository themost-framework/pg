import { PostgreSQLAdapter } from "../src/PostgreSQLAdapter";

describe('PostgreSQLAdapter', () => {
    it('should create instance', () => {
        const adapter = new PostgreSQLAdapter({
            "host":"localhost",
            "post":5432,
            "user":"user",
            "password":"password",
            "database":"db"
          });
        expect(adapter).toBeTruthy();
    })
});