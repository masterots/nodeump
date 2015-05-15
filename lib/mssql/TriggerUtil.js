"use strict";

var _ = require("lodash");
var Q = require("q");
var Connection = require("./Connection");
var Request = require("tedious").Request;

var _tablesWithTriggers = [];

function getTableSchema(tableName) {
  return tableName.split(".")[0].replace("[", "").replace("]", "");
}

function getTableName(tableName) {
  return tableName.split(".")[1].replace("[", "").replace("]", "");
}

function runQuery(config, client) {
  var deferred = new Q.defer();

  var schemas = _.chain(config.dataplan).pluck("tableName").map(function (tableName) {
    return getTableSchema(tableName);
  }).uniq().value();
  var tableNames = _.chain(config.dataplan).pluck("tableName").map(function (tableName) {
    return getTableName(tableName);
  }).uniq().value();

  var query = "select DISTINCT object_schema_name(parent_id) + '.' + object_name(parent_id) as table_name\n                FROM sys.triggers\n                WHERE object_schema_name(parent_id) IN ('" + schemas.join("','") + "')\n                AND object_name(parent_id) IN ('" + tableNames.join("','") + "')";

  var request = new Request(query, function (err) {
    if (err) {
      deferred.reject(err);
    }
    deferred.resolve(_tablesWithTriggers);
  });

  request.on("row", function (columns) {
    _tablesWithTriggers.push(columns[0].value);
  });

  client.execSql(request);

  return deferred.promise;
}

function loadTablesWithTriggers(config) {
  var connection = new Connection(config);

  return connection.getConnection().then(function (client) {
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