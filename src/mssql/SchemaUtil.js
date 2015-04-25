let _ = require('lodash');
let Q = require('q');
let Connection = require('./Connection');
let Request = require('tedious').Request;
let DataTypeUtil = require('./DataTypeUtil');

function getTableSchema(tableName) {
  return tableName.split('.')[0].replace('[', '').replace(']', '');
}

function getTableName(tableName) {
  return tableName.split('.')[1].replace('[', '').replace(']', '');
}

function runQuery(config, client) {
  let deferred = new Q.defer();
  let records = [];

  let schemas = _.chain(config.dataplan).pluck('tableName').map(function(tableName) { return getTableSchema(tableName); }).uniq().value();
  let tableNames = _.chain(config.dataplan).pluck('tableName').map(function(tableName) { return getTableName(tableName); }).uniq().value();

  let query = `SELECT object_schema_name(object_id) AS table_schema,
                object_name(object_id) AS table_name,
                name as column_name,
                TYPE_NAME(user_type_id) AS data_type,
                is_nullable,
                max_length,
                is_identity,
                precision,
                scale
                FROM sys.columns
                WHERE object_schema_name(object_id) IN ('${schemas.join("','")}')
                AND object_name(object_id) IN ('${tableNames.join("','")}')`;

  let request = new Request(query, (err) => {
    if (err) {
      deferred.reject(err);
    }
    deferred.resolve(records);
  });

  request.on('row', columns => {
    let row = {};
    columns.forEach(column => {
      if (column.metadata.colName === 'data_type') {
        column.value = DataTypeUtil.setDataType(column.value);
      }
      row[column.metadata.colName] = column.value;
    });
    records.push(row);
  });

  client.execSql(request);

  return deferred.promise;
}

function getTableSchemas(config) {
  let connection = new Connection(config);

  return connection.getConnection()
    .then(client => {
      return runQuery(config, client);
    });
}

module.exports = {
  getTableSchemas: getTableSchemas
};