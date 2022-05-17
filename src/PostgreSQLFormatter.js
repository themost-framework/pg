// MOST Web Framework Copyright (c) 2017-2022 THEMOST LP All Rights Reserved
import { SqlFormatter } from '@themost/query';
import { sprintf } from 'sprintf-js';

const SINGLE_QUOTE_ESCAPE ='\'\'';
const DOUBLE_QUOTE_ESCAPE = '"';

/**
 * @class
 * @augments {SqlFormatter}
 */

class PostgreSQLFormatter extends SqlFormatter {
    /**
     * @constructor
     */
    constructor() {
        super();
        this.settings = {
            nameFormat: '"$1"'
        };
    }

    /**
     *
     * @param {QueryExpression|{$take:number=,$skip:number=}} obj
     * @returns {string}
     */
    formatLimitSelect(obj) {
        let sql = this.formatSelect(obj);
        if (obj.$take) {
            if (obj.$skip)
                //add limit and skip records
                sql = sql.concat(' LIMIT ', obj.$take.toString(), ' OFFSET ', obj.$skip.toString());

            else
                //add only limit
                sql = sql.concat(' LIMIT ', obj.$take.toString());
        }
        return sql;
    }

    escapeConstant(obj, quoted) {
        let res = this.escape(obj, quoted);
        if (typeof obj === 'undefined' || obj === null)
            res += '::text';
        else if (obj instanceof Date)
            res += '::timestamp';
        else if (typeof obj === 'number')
            res += '::float';
        else if (typeof obj === 'boolean')
            res += '::bool';

        else
            res += '::text';
        return res;
    }

    /**
     * Escapes an object or a value and returns the equivalent sql value.
     * @param {*} value - A value that is going to be escaped for SQL statements
     * @param {boolean=} unquoted - An optional value that indicates whether the resulted string will be quoted or not.
     * @returns {string} - The equivalent SQL string value
     */
    escape(value, unquoted) {
        let res = super.escape.bind(this)(value, unquoted);
        if (typeof value === 'string') {
            if (/\\'/g.test(res))
                res = res.replace(/\\'/g, SINGLE_QUOTE_ESCAPE);
            if (/\\"/g.test(res))
                res = res.replace(/\\"/g, DOUBLE_QUOTE_ESCAPE);
        }
        return res;
    }

    /**
     * Implements indexOf(str,substr) expression formatter.
     * @param {String} p0 The source string
     * @param {String} p1 The string to search for
     */
    $indexof(p0, p1) {

        return sprintf('POSITION(lower(%s) IN lower(%s::text))', this.escape(p1), this.escape(p0));
    }

    /**
     * Implements regular expression formatting.
     * @param {*} p0 - An object or string that represents the field which is going to be used in this expression.
     * @param {string|*} p1 - A string that represents the text to search for
     * @returns {string}
     */
    $regex(p0, p1) {
        //validate params
        if (Object.isNullOrUndefined(p0) || Object.isNullOrUndefined(p1))
            return '';
        return sprintf('(%s ~ \'%s\')', this.escape(p0), this.escape(p1, true));
    }

    /**
     * Implements text search expression formatting.
     * @param {*} p0 - An object or string that represents the field which is going to be used in this expression.
     * @param {string|*} p1 - A string that represents the text to search for
     * @returns {string}
     */
    $text(p0, p1) {
        return this.$regex(p0, p1);
    }

    /**
     * Implements startsWith(a,b) expression formatter.
     * @param p0 {*}
     * @param p1 {*}
     */
    $startswith(p0, p1) {
        //validate params
        if (Object.isNullOrUndefined(p0) || Object.isNullOrUndefined(p1))
            return '';
        return sprintf('(%s ~ \'^%s\')', this.escape(p0), this.escape(p1, true));
    }

    /**
     * Implements endsWith(a,b) expression formatter.
     * @param p0 {*}
     * @param p1 {*}
     */
    $endswith(p0, p1) {
        //validate params
        if (Object.isNullOrUndefined(p0) || Object.isNullOrUndefined(p1))
            return '';
        const result = sprintf('(%s ~ \'%s$$\')', this.escape(p0), this.escape(p1, true));
        return result;
    }

    /**
     * Implements substring(str,pos) expression formatter.
     * @param {String} p0 The source string
     * @param {Number} pos The starting position
     * @param {Number=} length The length of the resulted string
     * @returns {string}
     */
    $substring(p0, pos, length) {
        if (length)
            return sprintf('SUBSTRING(%s FROM %s FOR %s)', this.escape(p0), pos.valueOf() + 1, length.valueOf());

        else
            return sprintf('SUBSTRING(%s FROM %s)', this.escape(p0), pos.valueOf() + 1);
    }

    /**
     * Implements contains(a,b) expression formatter.
     * @param p0 {*}
     * @param p1 {*}
     */
    $contains(p0, p1) {
        //validate params
        if (Object.isNullOrUndefined(p0) || Object.isNullOrUndefined(p1))
            return '';
        if (p1.valueOf().toString().length === 0)
            return '';
        return sprintf('(%s ~ \'%s\')', this.escape(p0), this.escape(p1, true));
    }

    /**
     * Implements length(a) expression formatter.
     * @param p0 {*}
     */
    $length(p0) {
        return sprintf('LENGTH(%s)', this.escape(p0));
    }

    $day(p0) { return sprintf('DATE_PART(\'day\',%s)', this.escape(p0)); }
    $month(p0) { return sprintf('DATE_PART(\'month\',%s)', this.escape(p0)); }
    $year(p0) { return sprintf('DATE_PART(\'year\',%s)', this.escape(p0)); }
    $hour(p0) { return sprintf('HOUR_TZ(%s::timestamp with time zone)', this.escape(p0)); }
    $minute(p0) { return sprintf('DATE_PART(\'minute\',%s)', this.escape(p0)); }
    $second(p0) { return sprintf('DATE_PART(\'second\',%s)', this.escape(p0)); }

    $date(p0) {
        return sprintf('CAST(%s AS DATE)', this.escape(p0));
    }
}

export {
    PostgreSQLFormatter
}