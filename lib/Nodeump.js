"use strict";

var _prototypeProperties = function (child, staticProps, instanceProps) { if (staticProps) Object.defineProperties(child, staticProps); if (instanceProps) Object.defineProperties(child.prototype, instanceProps); };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

require("babel/polyfill");
var client = undefined;

var Nodeump = (function () {
  function Nodeump(options) {
    _classCallCheck(this, Nodeump);

    this.config = {
      username: options.username,
      password: options.password,
      host: options.host,
      database: options.database,
      dataplan: options.dataplan,
      dialect: options.dialect || "postgres"
    };
    this.Exporter = require("./" + this.config.dialect + "/Exporter");
    this.Importer = require("./" + this.config.dialect + "/Importer");
  }

  _prototypeProperties(Nodeump, null, {
    exportData: {
      value: function exportData() {
        var exporter = new this.Exporter(this.config);
        return exporter.runQueriesForDataplan();
      },
      writable: true,
      configurable: true
    },
    importData: {
      value: function importData() {
        var importer = new this.Importer(this.config);
        return importer.runQueriesForDataplan();
      },
      writable: true,
      configurable: true
    }
  });

  return Nodeump;
})();

module.exports = Nodeump;