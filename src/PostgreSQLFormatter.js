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
    $indexOf(p0, p1) {
        return sprintf('(POSITION(%s IN %s::text)-1)', this.escape(p1), this.escape(p0));
    }

    /**
     * Implements regular expression formatting.
     * @param {*} p0 - An object or string that represents the field which is going to be used in this expression.
     * @param {string|*} p1 - A string that represents the text to search for
     * @returns {string}
     */
    $regex(p0, p1) {
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
        return sprintf('(%s ~ \'^%s\')', this.escape(p0), this.escape(p1, true));
    }

    /**
     * Implements endsWith(a,b) expression formatter.
     * @param p0 {*}
     * @param p1 {*}
     */
    $endswith(p0, p1) {
        return sprintf('(%s ~ \'%s$$\')', this.escape(p0), this.escape(p1, true));
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

    $round(p0, p1) {
        if (p1 == null) {
            return sprintf('ROUND(%s::numeric)', this.escape(p0)); 
        }
        return sprintf('ROUND(%s::numeric, %s)', this.escape(p0), this.escape(p1)); 
    }

    $day(p0) {
        return sprintf('DATE_PART(\'day\',%s)', this.escape(p0));
    }

    $dayOfMonth(p0) {
        return sprintf('DATE_PART(\'day\',%s)', this.escape(p0));
    }

    $month(p0) {
        return sprintf('DATE_PART(\'month\',%s)', this.escape(p0));
    }

    $year(p0) {
        return sprintf('DATE_PART(\'year\',%s)', this.escape(p0));
    }
    $hour(p0) {
        return sprintf('DATE_PART(\'hour\',%s)', this.escape(p0));
    }

    $minute(p0) {
        return sprintf('DATE_PART(\'minute\',%s)', this.escape(p0));
    }

    $minutes(p0) {
        return this.$minute(p0);
    }

    $second(p0) {
        return sprintf('DATE_PART(\'second\',%s)', this.escape(p0));
    }

    $seconds(p0) {
        return this.$second(p0);
    }

    $date(p0) {
        return sprintf('CAST(%s AS DATE)', this.escape(p0));
    }

    $toString(p0) {
        return sprintf('CAST(%s AS VARCHAR)', this.escape(p0));
    }

    isComparison(obj) {
        const key = Object.key(obj);
        return (/^\$(eq|ne|lt|lte|gt|gte|in|nin|text|regex)$/g.test(key));
    }
    isLogical(obj) {
        const key = Object.key(obj);
        return (/^\$(and|or|not|nor)$/g.test(key));
    }

    $cond(ifExpr, thenExpr, elseExpr) {
        // validate ifExpr which should an instance of QueryExpression or a comparison expression
        let ifExpression;
        if (instanceOf(ifExpr, QueryExpression)) {
            ifExpression = this.formatWhere(ifExpr.$where);
        } else if (this.isComparison(ifExpr) || this.isLogical(ifExpr)) {
            ifExpression = this.formatWhere(ifExpr);
        } else {
            throw new Error('Condition parameter should be an instance of query or comparison expression');
        }
        return sprintf('(CASE %s WHEN TRUE THEN %s ELSE %s END)', ifExpression, this.escape(thenExpr), this.escape(elseExpr));
    }
}

export {
    PostgreSQLFormatter
}