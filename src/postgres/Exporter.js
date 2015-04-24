let Q = require('q');
let fs = require('fs');
let copyTo = require('pg-copy-streams').to;
let chalk = require('chalk');

class Exporter {
  constructor(client, dataplan) {
    this.client = client;
    this.dataplan = dataplan;
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
    let queries = this.dataplan.map(table => {
      return this.runQuery(this.client, table.tableName);
    });
    return Q.allSettled(queries);
  }
}

module.exports = Exporter;