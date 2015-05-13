let Q = require('q');
let fs = require('fs');
let _ = require('lodash');
let Connection = require('./Connection');
let Request = require('tedious').Request;
let chalk = require('chalk');
let SchemaUtil = require('./SchemaUtil');
let baby = require('babyparse');

class Importer {
  constructor(config) {
    this.config = config;
  }

  runQuery(client, table) {
    let deferred = new Q.defer();

    let columnSchemasForTable = SchemaUtil.getSchemaForTable(table.tableName);

    let fileContents = fs.readFileSync(`data/${table.tableName}.csv`, {encoding: 'utf-8'});
    let parsed = baby.parse(fileContents, { header: true });
    let rows = parsed.data;

    if (rows.length === 0) {
      console.log(`skipping table ${table.tableName}`);
      console.log(`inserted 0 rows for ${table.tableName}`);
      deferred.resolve(table.tableName);
    }

    let bulkLoad = client.newBulkLoad(table.tableName, (error, rowCount) => {
      if (error) {
        console.log(table.tableName);
        console.log(error);
        deferred.reject(error);
      }
      console.log(`inserted ${rowCount} rows for ${table.tableName}`);
      deferred.resolve(table.tableName);
    });

    columnSchemasForTable.forEach(column => {
      let columnOptions = {};
      if (column.is_nullable) {
        columnOptions.nullable = true;
      }
      if (column.data_type === 'varchar' || column.data_type === 'nvarchar') {
        columnOptions.length = column.max_length >= 0 ? column.max_length : 'max';
      }
      if (column.data_type === 'decimal' || column.data_type === 'numeric') {
        columnOptions.scale = column.scale;
      }
      if (column.data_type === 'decimal' || column.data_type === 'numeric') {
        columnOptions.precision = column.precision;
      }
      bulkLoad.addColumn(column.column_name, column.data_type, columnOptions);
    });

    rows.forEach(row => {
      for (let columnName in row) {
        let columnSchema = SchemaUtil.getColumnInfo(table.tableName, columnName);
        if (columnSchema.data_type.name === 'TinyInt' ||
            columnSchema.data_type.name === 'SmallInt' ||
            columnSchema.data_type.name === 'BigInt' ||
            columnSchema.data_type.name === 'Int') {
          row[columnName] = parseInt(row[columnName], 10);
        }
        if (columnSchema.data_type.name === 'Numeric' ||
            columnSchema.data_type.name === 'Decimal' ||
            columnSchema.data_type.name === 'SmallMoney' ||
            columnSchema.data_type.name === 'Money' ||
            columnSchema.data_type.name === 'Float') {
          row[columnName] = parseFloat(row[columnName]);
        }
      }
      bulkLoad.addRow(row);
    });

    client.execBulkLoad(bulkLoad);

    return deferred.promise;
  }

  runQueriesForDataplan() {
    return SchemaUtil.loadTableSchemas(this.config)
      .then(() => {
        let queries = this.config.dataplan.map(table => {
          let connection = new Connection(this.config);
          return connection.getConnection()
            .then(client => {
              return this.runQuery(client, table);
            });
        });
        return Q.allSettled(queries);
      });
  }
}

module.exports = Importer;
