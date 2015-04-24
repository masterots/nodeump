"use strict";

var _prototypeProperties = function (child, staticProps, instanceProps) { if (staticProps) Object.defineProperties(child, staticProps); if (instanceProps) Object.defineProperties(child.prototype, instanceProps); };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

var SqlConnection = require("tedious").Connection;
var Q = require("q");

var Connection = (function () {
  function Connection(config) {
    _classCallCheck(this, Connection);

    this.config = {
      userName: config.username,
      password: config.password,
      server: config.host,
      options: {
        database: config.database,
        rowCollectionOnDone: true
      }
    };
  }

  _prototypeProperties(Connection, null, {
    getConnection: {
      value: function getConnection() {
        var deferred = new Q.defer();
        var client = new SqlConnection(this.config);
        client.on("connect", function (err) {
          if (err) {
            deferred.reject(err);
          }
          deferred.resolve(client);
        });
        client.on("error", function (err) {
          deferred.reject(err);
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