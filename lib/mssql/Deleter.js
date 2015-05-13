"use strict";

var _prototypeProperties = function (child, staticProps, instanceProps) { if (staticProps) Object.defineProperties(child, staticProps); if (instanceProps) Object.defineProperties(child.prototype, instanceProps); };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

var Connection = require("./Connection");
var Request = require("tedious").Request;
var Q = require("q");
var _ = require("lodash");
var TriggerUtil = require("./TriggerUtil");

function createQuery(dataplan) {
  var query = "";
  dataplan.reverse().forEach(function (table) {
    if (_.contains(TriggerUtil.getTablesWithTriggers(), table.tableName)) {
      query += "DISABLE TRIGGER ALL ON " + table.tableName + ";\n                DELETE FROM " + table.tableName + ";\n                ENABLE TRIGGER ALL ON " + table.tableName + ";\n";
    } else {
      query += "DELETE FROM " + table.tableName + ";\n";
    }
  });
  return query;
}

function runQuery(client, query) {
  var deferred = new Q.defer();

  var request = new Request(query, function (err, rowCount) {
    if (err) {
      deferred.reject(err);
    }
    deferred.resolve("data removed");
  });

  client.execSqlBatch(request);

  return deferred.promise;
}

var Deleter = (function () {
  function Deleter(config) {
    _classCallCheck(this, Deleter);

    this.config = config;
  }

  _prototypeProperties(Deleter, null, {
    deleteRecords: {
      value: function deleteRecords() {
        var _this = this;

        return TriggerUtil.loadTablesWithTriggers(this.config).then(function () {
          var query = createQuery(_this.config.dataplan);
          var connection = new Connection(_this.config);
          return connection.getConnection().then(function (client) {
            return runQuery(client, query);
          });
        });
      },
      writable: true,
      configurable: true
    }
  });

  return Deleter;
})();

module.exports = Deleter;