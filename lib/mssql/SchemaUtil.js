"use strict";

var _ = require("lodash");
var Q = require("q");
var Connection = require("./Connection");
var Request = require("tedious").Request;
var DataTypeUtil = require("./DataTypeUtil");

var _schemas = [];

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
    _schemas = records;
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

function loadTableSchemas(config) {
  var connection = new Connection(config);

  return connection.getConnection().then(function (client) {
    return runQuery(config, client);
  });
}

function getTableSchemas() {
  return _schemas;
}

function getSchemaForTable(tableName) {
  return _.filter(_schemas, function (schema) {
    var cleanTableName = tableName.replace(/\[/g, "").replace(/\]/g, "");
    return "" + schema.table_schema + "." + schema.table_name === cleanTableName;
  });
}

function getColumnInfo(tableName, columnName) {
  var tableSchema = _.filter(_schemas, function (schema) {
    var cleanTableName = tableName.replace(/\[/g, "").replace(/\]/g, "");
    return "" + schema.table_schema + "." + schema.table_name === cleanTableName;
  });
  return _.find(tableSchema, { column_name: columnName });
}

function getTableSchemaInfo(tableName) {
  var tableSchema = _.filter(_schemas, function (schema) {
    var cleanTableName = tableName.replace(/\[/g, "").replace(/\]/g, "");
    return "" + schema.table_schema + "." + schema.table_name === cleanTableName;
  });
  var schema = {
    datatypes: {},
    isNullable: {},
    scale: {},
    precision: {}
  };
  var tableDataTypes = _.forEach(tableSchema, function (item) {
    schema.datatypes[item.column_name] = item.data_type;
    schema.isNullable[item.column_name] = item.is_nullable;
    schema.scale[item.column_name] = item.scale;
    schema.precision[item.column_name] = item.precision;
  });
  return schema;
}

module.exports = {
  loadTableSchemas: loadTableSchemas,
  getTableSchemas: getTableSchemas,
  getSchemaForTable: getSchemaForTable,
  getColumnInfo: getColumnInfo,
  getTableSchemaInfo: getTableSchemaInfo
};