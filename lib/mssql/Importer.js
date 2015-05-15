"use strict";

var _prototypeProperties = function (child, staticProps, instanceProps) { if (staticProps) Object.defineProperties(child, staticProps); if (instanceProps) Object.defineProperties(child.prototype, instanceProps); };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

var Q = require("q");
var fs = require("fs");
var _ = require("lodash");
var Connection = require("./Connection");
var Request = require("tedious").Request;
var TYPES = require("tedious").TYPES;
var chalk = require("chalk");
var SchemaUtil = require("./SchemaUtil");
var baby = require("babyparse");

function isStringType(dataType) {
  return TYPES.VarChar.name === dataType.name || TYPES.NVarChar.name === dataType.name || TYPES.Text.name === dataType.name || TYPES.NText.name === dataType.name || TYPES.Char.name === dataType.name || TYPES.NChar.name === dataType.name;
}

function isFloatingPointType(dataType) {
  return TYPES.Decimal.name === dataType.name || TYPES.Numeric.name === dataType.name || TYPES.Float.name === dataType.name || TYPES.Money.name === dataType.name || TYPES.SmallMoney.name === dataType.name || TYPES.Real.name === dataType.name;
}

function isIntegerType(dataType) {
  return TYPES.TinyInt.name === dataType.name || TYPES.SmallInt.name === dataType.name || TYPES.Int.name === dataType.name;
}

function isDateType(dataType) {
  return TYPES.Date.name === dataType.name || TYPES.Time.name === dataType.name || TYPES.DateTime.name === dataType.name || TYPES.DateTime2.name === dataType.name || TYPES.SmallDateTime.name === dataType.name || TYPES.DateTimeOffset.name === dataType.name;
}

function formatColumnData(_x2, _x3) {
  var _arguments = arguments;
  var _again = true;

  _function: while (_again) {
    _again = false;
    var rows = _x2,
        columnSchema = _x3;
    formattedRows = currentRow = columnName = undefined;
    var formattedRows = _arguments[2] === undefined ? [] : _arguments[2];

    if (rows.length === 0) {
      return formattedRows;
    }
    var currentRow = rows.shift();
    for (var columnName in currentRow) {
      if (columnSchema.isNullable[columnName] && currentRow[columnName] === "null") {
        currentRow[columnName] = null;
      }

      if (currentRow[columnName] && _.isEqual(TYPES.Bit, columnSchema.datatypes[columnName])) {
        currentRow[columnName] = currentRow[columnName] === 1;
      }

      if (currentRow[columnName] && isIntegerType(columnSchema.datatypes[columnName])) {
        currentRow[columnName] = parseInt(currentRow[columnName], 10);
      }

      if (currentRow[columnName] && isFloatingPointType(columnSchema.datatypes[columnName])) {
        currentRow[columnName] = parseFloat(currentRow[columnName]);
      }

      if (currentRow[columnName] && isDateType(columnSchema.datatypes[columnName])) {
        currentRow[columnName] = new Date(currentRow[columnName]);
      }
    }

    formattedRows.push(currentRow);

    if (rows.length > 0) {
      _arguments = [_x2 = rows, _x3 = columnSchema, formattedRows];
      _again = true;
      continue _function;
    }

    return formattedRows;
  }
}

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
        var columnDataTypes = SchemaUtil.getTableSchemaInfo(table.tableName);
        var fileContents = fs.readFileSync("data/" + table.tableName + ".csv", { encoding: "utf-8" });
        var parsed = baby.parse(fileContents, { header: true });
        var rows = parsed.data;
        rows = formatColumnData(rows, columnDataTypes);

        if (rows.length === 0) {
          console.log("inserted 0 rows for " + table.tableName + " - no data");
          deferred.resolve(table.tableName);
        } else {
          (function () {
            var bulkLoad = client.newBulkLoad(table.tableName, function (error, rowCount) {
              if (error) {
                console.log(table.tableName);
                console.log(error);
                return deferred.reject(error);
              }
              console.log("inserted " + rowCount + " rows for " + table.tableName);
              deferred.resolve(table.tableName);
            });

            columnSchemasForTable.forEach(function (column) {
              if (!_.contains(table.excludedColumns, column.column_name)) {
                var columnOptions = {};

                if (column.is_nullable) {
                  columnOptions.nullable = true;
                }

                if (isStringType(column.data_type)) {
                  columnOptions.length = column.max_length >= 0 ? column.max_length : "max";
                }

                if (isFloatingPointType(column.data_type)) {
                  columnOptions.scale = column.scale;
                  columnOptions.precision = column.precision;
                }
                bulkLoad.addColumn(column.column_name, column.data_type, columnOptions);
              }
            });

            rows.forEach(function (row) {
              bulkLoad.addRow(row);
            });

            client.execBulkLoad(bulkLoad);
          })();
        }
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
              try {
                return _this.runQuery(client, table);
              } catch (e) {
                console.log(e);
                console.log(table);
                return;
              }
            });
          });
          return Q.all(queries);
        });
      },
      writable: true,
      configurable: true
    }
  });

  return Importer;
})();

module.exports = Importer;