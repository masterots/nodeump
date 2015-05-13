let Q = require('q');
let fs = require('fs');
let _ = require('lodash');
let Request = require('tedious').Request;
let Connection = require('./Connection');
let json2csv = require('json2csv');
let chalk = require('chalk');

class Exporter {
  constructor(config) {
    this.config = config;
  }

  getFields(records, excludedColumns) {
    return _.chain(records[0]).keys().difference(excludedColumns).value();
  }

  runQuery(client, table) {
    let deferred = new Q.defer();
    let records = [];
    let columnNames = [];
    let writeStream = fs.createWriteStream(`data/${table.tableName}.csv`);
    let request = new Request(`SELECT * FROM ${table.tableName}`, (err, rowCount) => {
      if (err) {
        deferred.reject(err);
      }
      if (records.length === 0) {
        writeStream.write(_.difference(columnNames, table.excludedColumns).toString());
        writeStream.end();
        console.log(chalk.bold.yellow(`${table.tableName}.csv written`));
        deferred.resolve(`${table.tableName} done`);
      } else {
        json2csv({data: records, fields: this.getFields(records, table.excludedColumns)}, (err, csv) => {
          if (err) {
            deferred.reject(err);
          }
          writeStream.write(csv);
          writeStream.end();
          console.log(chalk.bold.yellow(`${table.tableName}.csv written`));
          deferred.resolve(`${table.tableName} done`);
        });
      }
    });

    request.on('columnMetadata', columns => columnNames = _.pluck(columns, 'colName'));
    request.on('row', columns => {
      let row = {};
      columns.forEach(column => {
        row[column.metadata.colName] = column.value;
      });
      records.push(row);
    });

    client.execSql(request);

    return deferred.promise;
  }

  runQueriesForDataplan() {
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

module.exports = Exporter;
