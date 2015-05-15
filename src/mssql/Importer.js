let Q = require('q');
let fs = require('fs');
let _ = require('lodash');
let Connection = require('./Connection');
let Request = require('tedious').Request;
let TYPES = require('tedious').TYPES;
let chalk = require('chalk');
let SchemaUtil = require('./SchemaUtil');
let baby = require('babyparse');

function isStringType(dataType) {
  return (TYPES.VarChar.name === dataType.name || TYPES.NVarChar.name === dataType.name || TYPES.Text.name === dataType.name || TYPES.NText.name === dataType.name || TYPES.Char.name === dataType.name || TYPES.NChar.name === dataType.name);
}

function isFloatingPointType(dataType) {
  return (TYPES.Decimal.name === dataType.name || TYPES.Numeric.name === dataType.name || TYPES.Float.name === dataType.name || TYPES.Money.name === dataType.name || TYPES.SmallMoney.name === dataType.name || TYPES.Real.name === dataType.name);
}

function isIntegerType(dataType) {
  return (TYPES.TinyInt.name === dataType.name || TYPES.SmallInt.name === dataType.name || TYPES.Int.name === dataType.name);
}

function isDateType(dataType) {
  return (TYPES.Date.name === dataType.name || TYPES.Time.name === dataType.name || TYPES.DateTime.name === dataType.name || TYPES.DateTime2.name === dataType.name || TYPES.SmallDateTime.name === dataType.name || TYPES.DateTimeOffset.name === dataType.name);
}

function formatColumnData(rows, columnSchema, formattedRows = []) {
  if (rows.length === 0) {
    return formattedRows;
  }
  let currentRow = rows.shift();
  for (let columnName in currentRow) {
    if (columnSchema.isNullable[columnName] && currentRow[columnName] === 'null') {
      currentRow[columnName] = null;
    }

    if (currentRow[columnName] && _.isEqual(TYPES.Bit, columnSchema.datatypes[columnName])) {
      currentRow[columnName] = currentRow[columnName] === 1;
    }

    if (currentRow[columnName] && isIntegerType(columnSchema.datatypes[columnName])) {
      currentRow[columnName] = parseInt(currentRow[columnName], 10);
    }

    if (currentRow[columnName] && isFloatingPointType(columnSchema.datatypes[columnName])) {
      currentRow[columnName] = parseFloat(currentRow[columnName]);
    }

    if (currentRow[columnName] && isDateType(columnSchema.datatypes[columnName])) {
      currentRow[columnName] = new Date(currentRow[columnName]);
    }
  }

  formattedRows.push(currentRow);

  if (rows.length > 0) {
    return formatColumnData(rows, columnSchema, formattedRows);
  }

  return formattedRows;
}

class Importer {
  constructor(config) {
    this.config = config;
  }

  runQuery(client, table) {
    let deferred = new Q.defer();
    let columnSchemasForTable = SchemaUtil.getSchemaForTable(table.tableName);
    let columnDataTypes = SchemaUtil.getTableSchemaInfo(table.tableName);
    let fileContents = fs.readFileSync(`data/${table.tableName}.csv`, {encoding: 'utf-8'});
    let parsed = baby.parse(fileContents, {header: true});
    let rows = parsed.data;
    rows = formatColumnData(rows, columnDataTypes);

    if (rows.length === 0) {
      console.log(`inserted 0 rows for ${table.tableName} - no data`);
      deferred.resolve(table.tableName);
    } else {
      let bulkLoad = client.newBulkLoad(table.tableName, (error, rowCount) => {
        if (error) {
          console.log(table.tableName);
          console.log(error);
          return deferred.reject(error);
        }
        console.log(`inserted ${rowCount} rows for ${table.tableName}`);
        deferred.resolve(table.tableName);
      });

      columnSchemasForTable.forEach(column => {
        if (!_.contains(table.excludedColumns, column.column_name)) {
          let columnOptions = {};

          if (column.is_nullable) {
            columnOptions.nullable = true;
          }

          if (isStringType(column.data_type)) {
            columnOptions.length = column.max_length >= 0 ? column.max_length : 'max';
          }

          if (isFloatingPointType(column.data_type)) {
            columnOptions.scale = column.scale;
            columnOptions.precision = column.precision;
          }
          bulkLoad.addColumn(column.column_name, column.data_type, columnOptions);
        }
      });

      rows.forEach(row => {
        bulkLoad.addRow(row);
      });

      client.execBulkLoad(bulkLoad);
    }
    return deferred.promise;
  }

  runQueriesForDataplan() {
    return SchemaUtil.loadTableSchemas(this.config)
      .then(() => {
        let queries = this.config.dataplan.map(table => {
          let connection = new Connection(this.config);
          return connection.getConnection()
            .then(client => {
              try {
                return this.runQuery(client, table);
              } catch(e) {
                console.log(e);
                console.log(table);
                return;
              }
            });
        });
        return Q.all(queries);
      });
  }
}

module.exports = Importer;
