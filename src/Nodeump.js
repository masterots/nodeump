require("babel/polyfill");
let pg = require("pg");
let Q = require("q");
let Exporter = require('./Exporter');
let client;

function createConnection(connectionString) {
  let deferred = new Q.defer();
  client = new pg.Client(connectionString);
  client.connect(err => {
    if (err) {
      deferred.reject(err);
    }
    deferred.resolve(client);
  });
  return deferred.promise;
}

class Nodeump {
  constructor(options) {
    this.username = options.username;
    this.password = options.password;
    this.host = options.host;
    this.database = options.database;
    this.dataplan = options.dataplan;
  }

  getConnectionString() {
    return `postgres://${this.username}:${this.password}@${this.host}/${this.database}`;
  }

  exportData() {
    return createConnection(this.getConnectionString())
      .then(client => {
        let exporter = new Exporter(client, this.dataplan);
        return exporter.runQueriesForDataplan();
      });
  }
}

module.exports = Nodeump;