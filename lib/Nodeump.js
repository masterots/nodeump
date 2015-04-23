"use strict";

var _prototypeProperties = function (child, staticProps, instanceProps) { if (staticProps) Object.defineProperties(child, staticProps); if (instanceProps) Object.defineProperties(child.prototype, instanceProps); };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

require("babel/polyfill");
var pg = require("pg");
var Q = require("q");
var Exporter = require("./postgres/Exporter");
var Importer = require("./postgres/Importer");
var client = undefined;

function createConnection(connectionString) {
  var deferred = new Q.defer();
  client = new pg.Client(connectionString);
  client.connect(function (err) {
    if (err) {
      deferred.reject(err);
    }
    deferred.resolve(client);
  });
  return deferred.promise;
}

var Nodeump = (function () {
  function Nodeump(options) {
    _classCallCheck(this, Nodeump);

    this.username = options.username;
    this.password = options.password;
    this.host = options.host;
    this.database = options.database;
    this.dataplan = options.dataplan;
  }

  _prototypeProperties(Nodeump, null, {
    getConnectionString: {
      value: function getConnectionString() {
        return "postgres://" + this.username + ":" + this.password + "@" + this.host + "/" + this.database;
      },
      writable: true,
      configurable: true
    },
    exportData: {
      value: function exportData() {
        var _this = this;

        return createConnection(this.getConnectionString()).then(function (client) {
          var exporter = new Exporter(client, _this.dataplan);
          return exporter.runQueriesForDataplan();
        });
      },
      writable: true,
      configurable: true
    },
    importData: {
      value: function importData() {
        var _this = this;

        return createConnection(this.getConnectionString()).then(function (client) {
          var importer = new Importer(client, _this.dataplan);
          return importer.runQueriesForDataplan();
        });
      },
      writable: true,
      configurable: true
    }
  });

  return Nodeump;
})();

module.exports = Nodeump;