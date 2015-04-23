"use strict";

var _prototypeProperties = function (child, staticProps, instanceProps) { if (staticProps) Object.defineProperties(child, staticProps); if (instanceProps) Object.defineProperties(child.prototype, instanceProps); };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

var Q = require("q");
var json2csv = require("json2csv");
var fs = require("fs");
var chalk = require("chalk");

var Importer = (function () {
  function Importer(client, dataplan) {
    _classCallCheck(this, Importer);

    this.client = client;
    this.dataplan = dataplan;
  }

  _prototypeProperties(Importer, null, {
    runQuery: {
      value: function runQuery(client, tableName) {
        var _this = this;

        var deferred = new Q.defer();

        var records = [];
        var query = client.query("SELECT * FROM " + tableName);

        query.on("row", function (row) {
          records.push(row);
        });
        query.on("end", function (result) {
          _this.writeToCsv(tableName, result.fields.map(function (field) {
            return field.name;
          }), records).done(function () {
            deferred.resolve(records);
          });
        });
        query.on("error", function (err) {
          deferred.reject(err);
        });

        return deferred.promise;
      },
      writable: true,
      configurable: true
    },
    writeToCsv: {
      value: function writeToCsv(filename, columns, data) {
        var deferred = new Q.defer();

        json2csv({ data: data, fields: columns }, function (err, csv) {
          if (err) {
            deferred.reject(err);
          }
          fs.writeFileSync("csvs/" + filename + ".csv", csv);
          console.log(chalk.yellow("" + filename + ".csv saved"));
          deferred.resolve(csv);
        });

        return deferred.promise;
      },
      writable: true,
      configurable: true
    },
    runQueriesForDataplan: {
      value: function runQueriesForDataplan() {
        var _this = this;

        var queries = this.dataplan.map(function (table) {
          return _this.runQuery(_this.client, table);
        });
        return Q.allSettled(queries);
      },
      writable: true,
      configurable: true
    }
  });

  return Importer;
})();

module.exports = Importer;