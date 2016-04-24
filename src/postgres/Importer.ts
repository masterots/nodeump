let Q = require('q');
let fs = require('fs');
let Connection = require(`./Connection`);
let copyFrom = require('pg-copy-streams').from;
let chalk = require('chalk');

class Importer {
  constructor(config) {
    this.config = config;
  }

  runQuery(client, tableName) {
    let deferred = new Q.defer();

    let stream = client.query(copyFrom(`COPY ${tableName} FROM STDIN WITH CSV HEADER`));
    let fileStream = fs.createReadStream(`csvs/${tableName}.csv`);

    fileStream
      .on('error', err => {
        deferred.reject(err);
      });

    fileStream.pipe(stream)
      .on('finish', () => {
        console.log(chalk.bold.yellow(`${tableName}.csv imported`));
        deferred.resolve('done');
      }).on('error', err => {
        deferred.reject(err);
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

module.exports = Importer;