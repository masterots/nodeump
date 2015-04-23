let Q = require('q');
let json2csv = require('json2csv');
let fs = require('fs');

class Export {
  constructor(client, dataplan) {
    this.client = client;
    this.dataplan = dataplan;
  }

  runQuery(client, tableName) {
    let deferred = new Q.defer();

    let records = [];
    let query = client.query(`SELECT * FROM ${tableName}`);

    query.on("row", row => {
      records.push(row);
    });
    query.on("end", result => {
      this.writeToCsv(tableName, result.fields.map(field => field.name), records)
        .done(() => {
          deferred.resolve(records);
        });
    });
    query.on("error", err => {
      deferred.reject(err);
    });

    return deferred.promise;
  }

  writeToCsv(filename, columns, data) {
    let deferred = new Q.defer();

    json2csv({data: data, fields: columns}, (err, csv) => {
      if (err) {
        deferred.reject(err);
      }
      fs.writeFileSync(`${filename}.csv`, csv);
      deferred.resolve(csv);
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

module.exports = Export;