var Nodeump = require("../lib/Nodeump");

describe("Nodeump", function () {
  it("should take a config object", function () {
    var config = {
      host: 'localhost',
      database: 'nodeump',
      username: 'nodeump',
      password: 'nodeump',
      dataplan: {},
      dialect: 'mssql'
    };
    var nodeump = new Nodeump(config);
    expect(nodeump.config).toEqual(config);
  });
});
