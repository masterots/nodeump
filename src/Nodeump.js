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
  }

  exportData() {
    let exporter = new this.Exporter(this.config);
    return exporter.runQueriesForDataplan();
  }

  importData() {
    let connection = new this.Connection(this.config);
    return connection.getConnection()
      .then(client => {
        let importer = new this.Importer(client, this.config.dataplan);
        return importer.runQueriesForDataplan();
      })
  }
}

module.exports = Nodeump;