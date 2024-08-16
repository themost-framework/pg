const { addAliases } = require('module-alias');
const path = require('path');
const jsconfig = require('../../jsconfig.json');
if (jsconfig.compilerOptions && jsconfig.compilerOptions.paths) {
    const baseUrl = path.resolve(process.cwd(), jsconfig.compilerOptions.baseUrl || '.');
    const paths = jsconfig.compilerOptions.paths;
    const aliases = Object.keys(paths).reduce(function (previous, current) {
        const moduleUrl = paths[current][0];
        previous[current] = path.resolve(baseUrl, moduleUrl);
        return previous;
    }, {});
    addAliases(aliases);
}
