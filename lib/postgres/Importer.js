"use strict";

var _prototypeProperties = function (child, staticProps, instanceProps) { if (staticProps) Object.defineProperties(child, staticProps); if (instanceProps) Object.defineProperties(child.prototype, instanceProps); };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

var Q = require("q");
var fs = require("fs");
var copyFrom = require("pg-copy-streams").from;
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

        var stream = client.query(copyFrom("COPY " + tableName + " FROM STDIN WITH CSV HEADER"));
        var fileStream = fs.createReadStream("csvs/" + tableName + ".csv");

        fileStream.on("error", function (err) {
          deferred.reject(err);
        });

        fileStream.pipe(stream).on("finish", function () {
          deferred.resolve("done");
        }).on("error", function (err) {
          deferred.reject(err);
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