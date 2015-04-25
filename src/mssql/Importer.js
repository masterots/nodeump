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

  runQuery(client, table, schemas) {
    let deferred = new Q.defer();

    let columnSchemasForTable = _.filter(schemas, schema => {
      let cleanTableName = table.tableName.replace(/\[/g, "").replace(/\]/g, "");
      return `${schema.table_schema}.${schema.table_name}` === cleanTableName;
    });

    let fileContents = fs.readFileSync(`csvs/${table.tableName}.csv`, {encoding: 'utf-8'});
    let parsed = baby.parse(fileContents, { header: true });
    let rows = parsed.data;

    let bulkLoad = client.newBulkLoad(table.tableName, (error, rowCount) => {
      if (error) {
        console.log(table.tableName);
        console.log(error);
        deferred.reject(error);
      }
      console.log(`inserted rowCount rows for ${table.tableName}`);
      deferred.resolve(table.tableName);
    });

    columnSchemasForTable.forEach(column => {
      let columnOptions = {};
      if (column.is_nullable) {
        columnOptions.nullable = true;
      }
      if (column.data_type === 'varchar' || column.data_type === 'nvarchar') {
        columnOptions.length = column.max_length >= 0 ? column.max_length : 8001;
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
      bulkLoad.addRow(row);
    });

    client.execBulkLoad(bulkLoad);

    return deferred.promise;
  }

  runQueriesForDataplan() {
    return SchemaUtil.getTableSchemas(this.config)
      .then(schemas => {
        let queries = this.config.dataplan.map(table => {
          let connection = new Connection(this.config);
          return connection.getConnection()
            .then(client => {
              return this.runQuery(client, table, schemas);
            });
        });
        return Q.allSettled(queries);
      });

    //let queries = this.dataplan.map(table => {
    //  return this.runQuery(this.client, table);
    //});
    //return Q.allSettled(queries);
  }
}

module.exports = Importer;