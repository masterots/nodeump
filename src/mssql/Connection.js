let SqlConnection = require("tedious").Connection;
let Q = require("q");

class Connection {
  constructor(config) {
    this.config = {
      userName: config.username,
      password: config.password,
      server: config.host,
      options: {
        database: config.database
      }
    };
  }

  getConnection() {
    let deferred = new Q.defer();
    let client = new SqlConnection(this.config);
    client.on("connect", err => {
      if (err) {
        deferred.reject(err);
      }
      deferred.resolve(client);
    });
    client.on("error", err => {
      deferred.reject(err);
    });
    return deferred.promise;
  }
}

module.exports = Connection;