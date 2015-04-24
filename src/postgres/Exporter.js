let Q = require('q');
let fs = require('fs');
let Connection = require(`./Connection`);
let copyTo = require('pg-copy-streams').to;
let chalk = require('chalk');

class Exporter {
  constructor(config) {
    this.config = config;
  }

  runQuery(client, tableName) {
    let deferred = new Q.defer();

    let writeStream = fs.createWriteStream(`csvs/${tableName}.csv`);
    let readStream = client.query(copyTo(`COPY ${tableName} TO STDOUT WITH CSV HEADER`));

    readStream.pipe(writeStream);
    readStream.on("error", deferred.reject);
    writeStream.on("error", deferred.reject);
    writeStream.on("finish", () => {
      console.log(chalk.bold.yellow(`${tableName}.csv written`));
      deferred.resolve();
    });

    return deferred.promise;
  }

  runQueriesForDataplan() {
    let connection = new Connection(this.config);
    let queries = connection.getConnection().then(client => {
      return this.config.dataplan.map(table => {
        return this.runQuery(client, table.tableName);
      });
    });
    return Q.allSettled(queries);
  }
}

module.exports = Exporter;