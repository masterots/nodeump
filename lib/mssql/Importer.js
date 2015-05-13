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
      value: function runQuery(client, table) {
        var deferred = new Q.defer();

        var columnSchemasForTable = SchemaUtil.getSchemaForTable(table.tableName);

        var fileContents = fs.readFileSync("data/" + table.tableName + ".csv", { encoding: "utf-8" });
        var parsed = baby.parse(fileContents, { header: true });
        var rows = parsed.data;

        if (rows.length === 0) {
          console.log("skipping table " + table.tableName);
          console.log("inserted 0 rows for " + table.tableName);
          deferred.resolve(table.tableName);
        }

        var bulkLoad = client.newBulkLoad(table.tableName, function (error, rowCount) {
          if (error) {
            console.log(table.tableName);
            console.log(error);
            deferred.reject(error);
          }
          console.log("inserted " + rowCount + " rows for " + table.tableName);
          deferred.resolve(table.tableName);
        });

        columnSchemasForTable.forEach(function (column) {
          var columnOptions = {};
          if (column.is_nullable) {
            columnOptions.nullable = true;
          }
          if (column.data_type === "varchar" || column.data_type === "nvarchar") {
            columnOptions.length = column.max_length >= 0 ? column.max_length : "max";
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
          for (var columnName in row) {
            var columnSchema = SchemaUtil.getColumnInfo(table.tableName, columnName);
            if (columnSchema.data_type.name === "TinyInt" || columnSchema.data_type.name === "SmallInt" || columnSchema.data_type.name === "BigInt" || columnSchema.data_type.name === "Int") {
              row[columnName] = parseInt(row[columnName], 10);
            }
            if (columnSchema.data_type.name === "Numeric" || columnSchema.data_type.name === "Decimal" || columnSchema.data_type.name === "SmallMoney" || columnSchema.data_type.name === "Money" || columnSchema.data_type.name === "Float") {
              row[columnName] = parseFloat(row[columnName]);
            }
          }
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

        return SchemaUtil.loadTableSchemas(this.config).then(function () {
          var queries = _this.config.dataplan.map(function (table) {
            var connection = new Connection(_this.config);
            return connection.getConnection().then(function (client) {
              return _this.runQuery(client, table);
            });
          });
          return Q.allSettled(queries);
        });
      },
      writable: true,
      configurable: true
    }
  });

  return Importer;
})();

module.exports = Importer;