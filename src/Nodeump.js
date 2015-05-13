require("babel/polyfill");
let client;

class Nodeump {
  constructor(options) {
    this.config = {
      username: options.username,
      password: options.password,
      host: options.host,
      database: options.database,
      dataplan: options.dataplan,
      dialect: options.dialect || 'postgres'
    };
    this.Exporter = require(`./${this.config.dialect}/Exporter`);
    this.Importer = require(`./${this.config.dialect}/Importer`);
    this.Deleter = require(`./${this.config.dialect}/Deleter`);
  }

  exportData() {
    let exporter = new this.Exporter(this.config);
    return exporter.runQueriesForDataplan();
  }

  importData() {
    let importer = new this.Importer(this.config);
    return importer.runQueriesForDataplan();
  }

  deleteData() {
    let deleter = new this.Deleter(this.config);
    return deleter.deleteRecords();
  }
}

module.exports = Nodeump;
