let Connection = require('./Connection');
let Q = require('q');

function runQuery(client, table) {

}

class Deleter {
  constructor(config) {
    this.config = config;
  }

  deleteRecords() {
    let queries = this.config.dataplan.map(table => {
      let connection = new Connection(this.config);
      return connection.getConnection()
        .then(client => {
          return this.runQuery(client, table);
        });
    });
    return Q.allSettled(queries);
  }
}

module.exports = Deleter;