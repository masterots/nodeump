let _ = require('lodash');
let Q = require('q');
let Connection = require('./Connection');
let Request = require('tedious').Request;

let _tablesWithTriggers = [];

function getTableSchema(tableName) {
  return tableName.split('.')[0].replace('[', '').replace(']', '');
}

function getTableName(tableName) {
  return tableName.split('.')[1].replace('[', '').replace(']', '');
}

function runQuery(config, client) {
  let deferred = new Q.defer();

  let schemas = _.chain(config.dataplan).pluck('tableName').map(function(tableName) { return getTableSchema(tableName); }).uniq().value();
  let tableNames = _.chain(config.dataplan).pluck('tableName').map(function(tableName) { return getTableName(tableName); }).uniq().value();

  let query = `select DISTINCT object_schema_name(parent_id) + \'.\' + object_name(parent_id) as table_name
                FROM sys.triggers
                WHERE object_schema_name(parent_id) IN ('${schemas.join("','")}')
                AND object_name(parent_id) IN ('${tableNames.join("','")}')`;

  let request = new Request(query, (err) => {
    if (err) {
      deferred.reject(err);
    }
    console.log(_tablesWithTriggers);
    deferred.resolve(_tablesWithTriggers);
  });

  request.on('row', columns => {
    _tablesWithTriggers.push(columns[0].value);
  });

  client.execSql(request);

  return deferred.promise;
}

function loadTablesWithTriggers(config) {
  let connection = new Connection(config);

  return connection.getConnection()
    .then(client => {
      return runQuery(config, client);
    });
}

function getTablesWithTriggers() {
  return _tablesWithTriggers;
}

module.exports = {
  loadTablesWithTriggers: loadTablesWithTriggers,
  getTablesWithTriggers: getTablesWithTriggers
};
