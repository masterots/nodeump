var program = require('commander');

program
  .version('0.0.1')
  .option('--sql', 'Use SQL Server')
  .option('--psql', 'Use PostgreSQL')
  .option('--mongo', 'Use MongoDB')
  .parse(process.argv);

console.log('You\'re using the database type:');
if (program.sql) console.log(' - SQL Server');
if (program.psql) console.log(' - PostgreSQL');
if (program.mongo) console.log(' - MongoDB');
