[![npm](https://img.shields.io/npm/v/@themost%2Fpg.svg)](https://www.npmjs.com/package/@themost%2Fpg)
![Dependency status for latest release](https://img.shields.io/librariesio/release/npm/@themost/pg)
![GitHub top language](https://img.shields.io/github/languages/top/themost-framework/pg)
[![License](https://img.shields.io/npm/l/@themost/pg)](https://github.com/themost-framework/themost/blob/master/LICENSE)
![GitHub last commit](https://img.shields.io/github/last-commit/themost-framework/pg)
![GitHub Release Date](https://img.shields.io/github/release-date/themost-framework/pg)
[![npm](https://img.shields.io/npm/dw/@themost/query)](https://www.npmjs.com/package/@themost%2Fpg)

![MOST Web Framework Logo](https://github.com/themost-framework/common/raw/master/docs/img/themost_framework_v3_128.png)

@themost/pg
===========

Most Web Framework PostgreSQL Adapter

License: [BSD-3-Clause](https://github.com/themost-framework/pg/blob/master/LICENSE)

## Installation

    npm install @themost/pg

## Usage

Register PostgreSQL adapter on app.json as follows:

    "adapterTypes": [
            ...
            { "name":"PostgreSQL Data Adapter", "invariantName": "postgres", "type":"@themost/pg" }
            ...
        ],
    adapters: {
        "postgres": { "name":"local-db", "invariantName":"postgres", "default":true,
            "options": {
              "host":"localhost",
              "post":5432,
              "user":"user",
              "password":"password",
              "database":"db"
            }
    }
} 

## Testing

Clone project and create a `.env` file to set testing environment variables

    DB_HOST=localhost
    DB_PORT=5432
    DB_USER=postgres
    DB_PASSWORD=pass
    NODE_ENV=development

(*) DB_PASSWORD is optional

If you are using [Gitpod](https://www.gitpod.io/) create a `.env` file and set `DB_USER=gitpod`

    DB_HOST=localhost
    DB_PORT=5432
    DB_USER=gitpod
    NODE_ENV=development

Execute `npm test`. 

The operation will create a new test database `test_db` with sample data that is going to be used for testing adapter.

