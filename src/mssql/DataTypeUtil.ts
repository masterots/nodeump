let TYPES = require('tedious').TYPES;

function setDataType(dataType) {
  if (dataType === 'bit') { return TYPES.Bit; }
  if (dataType === 'tinyint') { return TYPES.TinyInt; }
  if (dataType === 'smallint') { return TYPES.SmallInt; }
  if (dataType === 'int') { return TYPES.Int; }
  if (dataType === 'bigint') { return TYPES.BigInt; }
  if (dataType === 'numeric') { return TYPES.Numeric; }
  if (dataType === 'decimal') { return TYPES.Decimal; }
  if (dataType === 'smallmoney') { return TYPES.SmallMoney; }
  if (dataType === 'money') { return TYPES.Money; }
  if (dataType === 'float') { return TYPES.Float; }
  if (dataType === 'real') { return TYPES.Real; }
  if (dataType === 'smalldatetime') { return TYPES.SmallDateTime; }
  if (dataType === 'datetime') { return TYPES.DateTime; }
  if (dataType === 'datetime2') { return TYPES.DateTime2; }
  if (dataType === 'datetimeoffset') { return TYPES.DateTimeOffset; }
  if (dataType === 'time') { return TYPES.Time; }
  if (dataType === 'date') { return TYPES.Date; }
  if (dataType === 'varchar') { return TYPES.VarChar; }
  if (dataType === 'text') { return TYPES.Text; }
  if (dataType === 'nchar') { return TYPES.NChar; }
  if (dataType === 'nvarchar') { return TYPES.NVarChar; }
  if (dataType === 'ntext') { return TYPES.NText; }
  if (dataType === 'binary') { return TYPES.Binary; }
  if (dataType === 'varbinary') { return TYPES.VarBinary; }
  if (dataType === 'image') { return TYPES.Image; }
  if (dataType === 'null') { return TYPES.Null; }
  if (dataType === 'TVP') { return TYPES.TVP; }
  if (dataType === 'UDT') { return TYPES.UDT; }
  if (dataType === 'uniqueidentifier') { return TYPES.UniqueIdentifier; }
  if (dataType === 'xml') { return TYPES.xml; }
  if (dataType === 'char') { return TYPES.Char; }
}

module.exports = {
  setDataType: setDataType
};