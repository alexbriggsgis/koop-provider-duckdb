
const duckdb = require('../duckdb/src');

const outputs = []
const auths = []
const caches = []
const plugins = [
  {
    instance: duckdb
  },
]
module.exports = [...outputs, ...auths, ...caches, ...plugins]
