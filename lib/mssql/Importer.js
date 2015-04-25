"use strict";

var _prototypeProperties = function (child, staticProps, instanceProps) { if (staticProps) Object.defineProperties(child, staticProps); if (instanceProps) Object.defineProperties(child.prototype, instanceProps); };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

var Q = require("q");
var fs = require("fs");
var _ = require("lodash");
var Connection = require("./Connection");
var Request = require("tedious").Request;
var chalk = require("chalk");
var SchemaUtil = require("./SchemaUtil");
var baby = require("babyparse");

var Importer = (function () {
  function Importer(config) {
    _classCallCheck(this, Importer);

    this.config = config;
  }

  _prototypeProperties(Importer, null, {
    runQuery: {
      value: function runQuery(client, table, schemas) {
        var deferred = new Q.defer();

        var columnSchemasForTable = _.filter(schemas, function (schema) {
          var cleanTableName = table.tableName.replace(/\[/g, "").replace(/\]/g, "");
          return "" + schema.table_schema + "." + schema.table_name === cleanTableName;
        });

        var fileContents = fs.readFileSync("csvs/" + table.tableName + ".csv", { encoding: "utf-8" });
        var parsed = baby.parse(fileContents, { header: true });
        var rows = parsed.data;

        var bulkLoad = client.newBulkLoad(table.tableName, function (error, rowCount) {
          if (error) {
            console.log(table.tableName);
            console.log(error);
            deferred.reject(error);
          }
          console.log("inserted rowCount rows for " + table.tableName);
          deferred.resolve(table.tableName);
        });

        columnSchemasForTable.forEach(function (column) {
          var columnOptions = {};
          if (column.is_nullable) {
            columnOptions.nullable = true;
          }
          if (column.data_type === "varchar" || column.data_type === "nvarchar") {
            columnOptions.length = column.max_length >= 0 ? column.max_length : 8001;
          }
          if (column.data_type === "decimal" || column.data_type === "numeric") {
            columnOptions.scale = column.scale;
          }
          if (column.data_type === "decimal" || column.data_type === "numeric") {
            columnOptions.precision = column.precision;
          }
          bulkLoad.addColumn(column.column_name, column.data_type, columnOptions);
        });

        rows.forEach(function (row) {
          bulkLoad.addRow(row);
        });

        client.execBulkLoad(bulkLoad);

        return deferred.promise;
      },
      writable: true,
      configurable: true
    },
    runQueriesForDataplan: {
      value: function runQueriesForDataplan() {
        var _this = this;

        return SchemaUtil.getTableSchemas(this.config).then(function (schemas) {
          var queries = _this.config.dataplan.map(function (table) {
            var connection = new Connection(_this.config);
            return connection.getConnection().then(function (client) {
              return _this.runQuery(client, table, schemas);
            });
          });
          return Q.allSettled(queries);
        });

        //let queries = this.dataplan.map(table => {
        //  return this.runQuery(this.client, table);
        //});
        //return Q.allSettled(queries);
      },
      writable: true,
      configurable: true
    }
  });

  return Importer;
})();

module.exports = Importer;