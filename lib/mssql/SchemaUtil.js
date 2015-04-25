"use strict";

var _ = require("lodash");
var Q = require("q");
var Connection = require("./Connection");
var Request = require("tedious").Request;
var DataTypeUtil = require("./DataTypeUtil");

function getTableSchema(tableName) {
  return tableName.split(".")[0].replace("[", "").replace("]", "");
}

function getTableName(tableName) {
  return tableName.split(".")[1].replace("[", "").replace("]", "");
}

function runQuery(config, client) {
  var deferred = new Q.defer();
  var records = [];

  var schemas = _.chain(config.dataplan).pluck("tableName").map(function (tableName) {
    return getTableSchema(tableName);
  }).uniq().value();
  var tableNames = _.chain(config.dataplan).pluck("tableName").map(function (tableName) {
    return getTableName(tableName);
  }).uniq().value();

  var query = "SELECT object_schema_name(object_id) AS table_schema,\n                object_name(object_id) AS table_name,\n                name as column_name,\n                TYPE_NAME(user_type_id) AS data_type,\n                is_nullable,\n                max_length,\n                is_identity,\n                precision,\n                scale\n                FROM sys.columns\n                WHERE object_schema_name(object_id) IN ('" + schemas.join("','") + "')\n                AND object_name(object_id) IN ('" + tableNames.join("','") + "')";

  var request = new Request(query, function (err) {
    if (err) {
      deferred.reject(err);
    }
    deferred.resolve(records);
  });

  request.on("row", function (columns) {
    var row = {};
    columns.forEach(function (column) {
      if (column.metadata.colName === "data_type") {
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
  var connection = new Connection(config);

  return connection.getConnection().then(function (client) {
    return runQuery(config, client);
  });
}

module.exports = {
  getTableSchemas: getTableSchemas
};