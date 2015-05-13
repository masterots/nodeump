"use strict";

var _prototypeProperties = function (child, staticProps, instanceProps) { if (staticProps) Object.defineProperties(child, staticProps); if (instanceProps) Object.defineProperties(child.prototype, instanceProps); };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

var Q = require("q");
var fs = require("fs");
var _ = require("lodash");
var Request = require("tedious").Request;
var Connection = require("./Connection");
var json2csv = require("json2csv");
var chalk = require("chalk");

var Exporter = (function () {
  function Exporter(config) {
    _classCallCheck(this, Exporter);

    this.config = config;
  }

  _prototypeProperties(Exporter, null, {
    getFields: {
      value: function getFields(records, excludedColumns) {
        return _.chain(records[0]).keys().difference(excludedColumns).value();
      },
      writable: true,
      configurable: true
    },
    runQuery: {
      value: function runQuery(client, table) {
        var _this = this;

        var deferred = new Q.defer();
        var records = [];
        var columnNames = [];
        var writeStream = fs.createWriteStream("data/" + table.tableName + ".csv");
        var request = new Request("SELECT * FROM " + table.tableName, function (err, rowCount) {
          if (err) {
            deferred.reject(err);
          }
          if (records.length === 0) {
            writeStream.write(_.difference(columnNames, table.excludedColumns).toString());
            writeStream.end();
            console.log(chalk.bold.yellow("" + table.tableName + ".csv written"));
            deferred.resolve("" + table.tableName + " done");
          } else {
            json2csv({ data: records, fields: _this.getFields(records, table.excludedColumns) }, function (err, csv) {
              if (err) {
                deferred.reject(err);
              }
              writeStream.write(csv);
              writeStream.end();
              console.log(chalk.bold.yellow("" + table.tableName + ".csv written"));
              deferred.resolve("" + table.tableName + " done");
            });
          }
        });

        request.on("columnMetadata", function (columns) {
          return columnNames = _.pluck(columns, "colName");
        });
        request.on("row", function (columns) {
          var row = {};
          columns.forEach(function (column) {
            row[column.metadata.colName] = column.value;
          });
          records.push(row);
        });

        client.execSql(request);

        return deferred.promise;
      },
      writable: true,
      configurable: true
    },
    runQueriesForDataplan: {
      value: function runQueriesForDataplan() {
        var _this = this;

        var queries = this.config.dataplan.map(function (table) {
          var connection = new Connection(_this.config);
          return connection.getConnection().then(function (client) {
            return _this.runQuery(client, table);
          });
        });
        return Q.allSettled(queries);
      },
      writable: true,
      configurable: true
    }
  });

  return Exporter;
})();

module.exports = Exporter;