"use strict";

var _prototypeProperties = function (child, staticProps, instanceProps) { if (staticProps) Object.defineProperties(child, staticProps); if (instanceProps) Object.defineProperties(child.prototype, instanceProps); };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

var pg = require("pg");
var Q = require("q");

var Connection = (function () {
  function Connection(config) {
    _classCallCheck(this, Connection);

    this.connectionString = "postgres://" + config.username + ":" + config.password + "@" + config.host + "/" + config.database;
  }

  _prototypeProperties(Connection, null, {
    getConnection: {
      value: function getConnection() {
        var deferred = new Q.defer();
        var client = new pg.Client(this.connectionString);
        client.connect(function (err) {
          if (err) {
            deferred.reject(err);
          }
          deferred.resolve(client);
        });
        return deferred.promise;
      },
      writable: true,
      configurable: true
    }
  });

  return Connection;
})();

module.exports = Connection;