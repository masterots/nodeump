let Q = require('q');
let fs = require('fs');
let copyFrom = require('pg-copy-streams').from;
let chalk = require('chalk');

class Importer {
  constructor(client, dataplan) {
    this.client = client;
    this.dataplan = dataplan;
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
        deferred.resolve('done');
      }).on('error', err => {
        deferred.reject(err);
      });

    return deferred.promise;
  }

  runQueriesForDataplan() {
    let queries = this.dataplan.map(table => {
      return this.runQuery(this.client, table);
    });
    return Q.allSettled(queries);
  }
}

module.exports = Importer;