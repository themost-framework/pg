// MOST Web Framework Copyright (c) 2017-2022 THEMOST LP All Rights Reserved
import { SqlFormatter, QueryExpression, QueryField } from '@themost/query';
import { sprintf } from 'sprintf-js';
import { isObjectDeep } from './isObjectDeep';

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
        // serialize array of objects as json array
        if (Array.isArray(value)) {
            // find first non-object value
            const index = value.filter((x) => {
                return x != null;
            }).findIndex((x) => {
                return isObjectDeep(x) === false;
            });
            // if all values are objects
            if (index === -1) {
                return this.escape(JSON.stringify(value)); // return as json array
            }
        }
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
        return sprintf('DATE_PART(\'day\',(%s)::date)', this.escape(p0));
    }

    $dayOfMonth(p0) {
        return sprintf('DATE_PART(\'day\',(%s)::date)', this.escape(p0));
    }

    $month(p0) {
        return sprintf('DATE_PART(\'month\',(%s)::date)', this.escape(p0));
    }

    $year(p0) {
        return sprintf('DATE_PART(\'year\',(%s)::date)', this.escape(p0));
    }
    $hour(p0) {
        return sprintf('DATE_PART(\'hour\',(%s)::timestamp)', this.escape(p0));
    }

    $minute(p0) {
        return sprintf('DATE_PART(\'minute\',(%s)::timestamp)', this.escape(p0));
    }

    $minutes(p0) {
        return this.$minute(p0);
    }

    $second(p0) {
        return sprintf('DATE_PART(\'second\',(%s)::timestamp)', this.escape(p0));
    }

    $seconds(p0) {
        return this.$second(p0);
    }

    $date(p0) {
        return sprintf('CAST(%s AS DATE)', this.escape(p0));
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
        if (ifExpr instanceof QueryExpression) {
            ifExpression = this.formatWhere(ifExpr.$where);
        } else if (this.isComparison(ifExpr) || this.isLogical(ifExpr)) {
            ifExpression = this.formatWhere(ifExpr);
        } else {
            throw new Error('Condition parameter should be an instance of query or comparison expression');
        }
        return sprintf('(CASE %s WHEN TRUE THEN %s ELSE %s END)', ifExpression, this.escape(thenExpr), this.escape(elseExpr));
    }

    /**
     * @param {*} expr
     * @return {string}
     */
    $jsonGet(expr) {
        if (typeof expr.$name !== 'string') {
            throw new Error('Invalid json expression. Expected a string');
        }
        const parts = expr.$name.split('.');
        const extract = this.escapeName(parts.splice(0, 2).join('.'));
        return `${extract}->>'${parts.join('.')}'`;
    }

    $toString(expr) {
        return sprintf('CAST(%s as VARCHAR)', this.escape(expr));
    }

    $toInt(expr) {
        return sprintf('FLOOR(CAST(%s as DECIMAL(19,8)))', this.escape(expr));
    }

    $toDouble(expr) {
        return this.$toDecimal(expr, 19, 8);
    }

    // noinspection JSCheckFunctionSignatures
    /**
     * @param {*} expr 
     * @param {number=} precision 
     * @param {number=} scale 
     * @returns 
     */
    $toDecimal(expr, precision, scale) {
        const p = typeof precision === 'number' ? Math.floor(precision) : 19;
        const s = typeof scale === 'number' ? Math.floor(scale) : 8;
        return sprintf('CAST(%s as DECIMAL(%s,%s))', this.escape(expr), p, s);
    }

    $toLong(expr) {
        return sprintf('FLOOR(CAST(%s as DECIMAL(19,8)))', this.escape(expr));
    }

    $uuid() {
        // todo::use gen_random_uuid () implemented at version 13
        // https://www.postgresql.org/docs/13/functions-uuid.html
        return 'md5(random()::text || clock_timestamp()::text)::uuid';
    }

    $toGuid(expr) {
        return sprintf('MD5(%s)::uuid', this.escape(expr));
    }

    /**
     * 
     * @param {('date'|'datetime'|'timestamp')} type 
     * @returns 
     */
    $getDate(type) {
        switch (type) {
            case 'date':
                return 'CURRENT_DATE';
            case 'datetime':
                return 'CURRENT_TIMESTAMP::timestamp';
            case 'timestamp':
                return 'CURRENT_TIMESTAMP::timestamp with time zone';
            default:
                return 'CURRENT_TIMESTAMP::timestamp';
        }
    }

    /**
     * @param {...*} expr
     */
    // eslint-disable-next-line no-unused-vars
    $jsonObject(expr) {
        // expected an array of QueryField objects
        const args = Array.from(arguments).reduce((previous, current) => {
            // get the first key of the current object
            let [name] = Object.keys(current);
            let value;
            // if the name is not a string then throw an error
            if (typeof name !== 'string') {
                throw new Error('Invalid json object expression. The attribute name cannot be determined.');
            }
            // if the given name is a dialect function (starts with $) then use the current value as is
            // otherwise create a new QueryField object
            if (name.startsWith('$')) {
                value = new QueryField(current[name]);
                name = value.getName();
            } else {
                value = current instanceof QueryField ? new QueryField(current[name]) : current[name];
            }
            // escape json attribute name and value
            previous.push(this.escape(name), this.escape(value));
            return previous;
        }, []);
        return `json_build_object(${args.join(',')})`;
    }

    /**
     * @param {{ $jsonGet: Array<*> }} expr
     */
    $jsonGroupArray(expr) {
        const [key] = Object.keys(expr);
        if (key !== '$jsonObject') {
            throw new Error('Invalid json group array expression. Expected a json object expression');
        }
        return `json_agg(${this.escape(expr)})`;
    }

    /**
     * @param {import('@themost/query').QueryExpression} expr
     */
    $jsonArray(expr) {
        if (expr == null) {
            throw new Error('The given query expression cannot be null');
        }
        if (expr instanceof QueryField) {
            // escape expr as field and waiting for parsing results as json array
            return this.escape(expr);
        }
        // trear expr as select expression
        if (expr.$select) {
            // get select fields
            const args = Object.keys(expr.$select).reduce((previous, key) => {
                previous.push.apply(previous, expr.$select[key]);
                return previous;
            }, []);
            const [key] = Object.keys(expr.$select);
            // prepare select expression to return json array   
            expr.$select[key] = [
                {
                    $jsonGroupArray: [ // use json_group_array function
                        {
                            $jsonObject: args // use json_object function
                        }
                    ]
                }
            ];
            return `(${this.format(expr)})`;
        }
        // treat expression as query field
        if (Object.prototype.hasOwnProperty.call(expr, '$name')) {
            return this.escape(expr);
        }
        // treat expression as value
        if (Object.prototype.hasOwnProperty.call(expr, '$value')) {
            if (Array.isArray(expr.$value)) {
                const values = expr.$value.map((x) => {
                    return this.escape(x);
                }).join(',');
                return `json_build_array(${values})`;
            }
            return this.escape(expr);
        }
        if (Object.prototype.hasOwnProperty.call(expr, '$literal')) {
            if (Array.isArray(expr.$literal)) {
                const values = expr.$literal.map((x) => {
                    return this.escape(x);
                }).join(',');
                return `json_build_array(${values})`;
            }
            return this.escape(expr);
        }
        throw new Error('Invalid json array expression. Expected a valid select expression');
    }

}

export {
    PostgreSQLFormatter
}
