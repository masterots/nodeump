let Q = require('q');
let fs = require('fs');
let Request = require('tedious').Request;
let chalk = require('chalk');

class Importer {
  constructor(client, dataplan) {
    this.client = client;
    this.dataplan = dataplan;
  }

  runQuery(client, tableName) {
    let deferred = new Q.defer();

    let fileData = fs.readFileSync(`csvs/${tableName}.csv`);
    console.log(fileData.toString());

    let request = new Request(`BULK INSERT test_table
                                FROM 'csvs/${tableName}.csv'
                                WITH
                                (
                                  FIELDTERMINATOR = ',',
                                  ROWTERMINATOR = '\n'
                                )`, (err, rowCount) => {
      if (err) {
        console.log(err);
        deferred.reject(err);
      }
      deferred.resolve(rowCount);
    });

    client.execSql(request);

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