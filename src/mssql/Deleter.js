let Connection = require('./Connection');
let Request = require('tedious').Request;
let Q = require('q');
let _ = require('lodash');
let TriggerUtil = require('./TriggerUtil');

function createQuery(dataplan) {
  let query = '';
  dataplan.reverse().forEach(table => {
    if (_.contains(TriggerUtil.getTablesWithTriggers(), table.tableName)) {
      query += `DISABLE TRIGGER ALL ON ${table.tableName};
                DELETE FROM ${table.tableName};
                ENABLE TRIGGER ALL ON ${table.tableName};\n`;
    } else {
      query += `DELETE FROM ${table.tableName};\n`;
    }
  });
  return query;
}

function runQuery(client, query) {
  let deferred = new Q.defer();

  let request = new Request(query, (err, rowCount) => {
    if (err) {
      deferred.reject(err);
    }
    deferred.resolve('data removed');
  });

  client.execSqlBatch(request);

  return deferred.promise;
}

class Deleter {
  constructor(config) {
    this.config = config;
  }

  deleteRecords() {
    return TriggerUtil.loadTablesWithTriggers(this.config)
      .then(() => {
        let query = createQuery(this.config.dataplan);
        let connection = new Connection(this.config);
        return connection.getConnection()
          .then(client => {
            return runQuery(client, query);
          });
      });
  }
}

module.exports = Deleter;
