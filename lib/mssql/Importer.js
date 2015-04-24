"use strict";

var _prototypeProperties = function (child, staticProps, instanceProps) { if (staticProps) Object.defineProperties(child, staticProps); if (instanceProps) Object.defineProperties(child.prototype, instanceProps); };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

var Q = require("q");
var fs = require("fs");
var Request = require("tedious").Request;
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
        var deferred = new Q.defer();

        var fileData = fs.readFileSync("csvs/" + tableName + ".csv");
        console.log(fileData.toString());

        var request = new Request("BULK INSERT test_table\n                                FROM 'csvs/" + tableName + ".csv'\n                                WITH\n                                (\n                                  FIELDTERMINATOR = ',',\n                                  ROWTERMINATOR = '\n'\n                                )", function (err, rowCount) {
          if (err) {
            console.log(err);
            deferred.reject(err);
          }
          deferred.resolve(rowCount);
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