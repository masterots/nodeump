"use strict";

var _prototypeProperties = function (child, staticProps, instanceProps) { if (staticProps) Object.defineProperties(child, staticProps); if (instanceProps) Object.defineProperties(child.prototype, instanceProps); };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

var Q = require("q");
var fs = require("fs");
var Connection = require("./Connection");
var copyTo = require("pg-copy-streams").to;
var chalk = require("chalk");

var Exporter = (function () {
  function Exporter(config) {
    _classCallCheck(this, Exporter);

    this.config = config;
  }

  _prototypeProperties(Exporter, null, {
    runQuery: {
      value: function runQuery(client, tableName) {
        var deferred = new Q.defer();

        var writeStream = fs.createWriteStream("csvs/" + tableName + ".csv");
        var readStream = client.query(copyTo("COPY " + tableName + " TO STDOUT WITH CSV HEADER"));

        readStream.pipe(writeStream);
        readStream.on("error", deferred.reject);
        writeStream.on("error", deferred.reject);
        writeStream.on("finish", function () {
          console.log(chalk.bold.yellow("" + tableName + ".csv written"));
          deferred.resolve();
        });

        return deferred.promise;
      },
      writable: true,
      configurable: true
    },
    runQueriesForDataplan: {
      value: function runQueriesForDataplan() {
        var _this = this;

        var connection = new Connection(this.config);
        var queries = connection.getConnection().then(function (client) {
          return _this.config.dataplan.map(function (table) {
            return _this.runQuery(client, table.tableName);
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