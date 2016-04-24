let pg = require("pg");
let Q = require("q");

class Connection {
  constructor(config) {
    this.connectionString = `postgres://${config.username}:${config.password}@${config.host}/${config.database}`;
  }

  getConnection() {
    let deferred = new Q.defer();
    let client = new pg.Client(this.connectionString);
    client.connect(err => {
      if (err) {
        deferred.reject(err);
      }
      deferred.resolve(client);
    });
    return deferred.promise;
  }
}

module.exports = Connection;