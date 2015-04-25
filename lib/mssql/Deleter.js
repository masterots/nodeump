"use strict";

var _prototypeProperties = function (child, staticProps, instanceProps) { if (staticProps) Object.defineProperties(child, staticProps); if (instanceProps) Object.defineProperties(child.prototype, instanceProps); };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

var Connection = require("./Connection");
var Q = require("q");

function runQuery(client, table) {}

var Deleter = (function () {
  function Deleter(config) {
    _classCallCheck(this, Deleter);

    this.config = config;
  }

  _prototypeProperties(Deleter, null, {
    deleteRecords: {
      value: function deleteRecords() {
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

  return Deleter;
})();

module.exports = Deleter;