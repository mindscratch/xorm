package xorm

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/go-xorm/core"
)

// from https://crate.io/docs/crate/reference/en/latest/sql/general/lexical-structure.html#key-words-and-identifiers
//
// copied and pasted the values in that table to Vi and saved it to a file, then ran: tr '\t' '\n' < input.txt > output.txt
// that put each word on its own line to make it easier to paste into here.
var (
	crateReservedWords = map[string]bool{
		"ABS ":                   true,
		"ABSOLUTE ":              true,
		"ACTION ":                true,
		"ADD ":                   true,
		"AFTER ":                 true,
		"ALL ":                   true,
		"ALLOCATE ":              true,
		"ALTER ":                 true,
		"AND ":                   true,
		"ANY ":                   true,
		"ARE ":                   true,
		"ARRAY ":                 true,
		"ARRAY_AGG ":             true,
		"ARRAY_MAX_CARDINALITY ": true,
		"AS ":                true,
		"ASC ":               true,
		"ASENSITIVE ":        true,
		"ASSERTION ":         true,
		"ASYMMETRIC ":        true,
		"AT ":                true,
		"ATOMIC ":            true,
		"AUTHORIZATION ":     true,
		"AVG ":               true,
		"BEFORE ":            true,
		"BEGIN ":             true,
		"BEGIN_FRAME ":       true,
		"BEGIN_PARTITION ":   true,
		"BETWEEN ":           true,
		"BIGINT ":            true,
		"BINARY ":            true,
		"BIT ":               true,
		"BIT_LENGTH ":        true,
		"BOOLEAN ":           true,
		"BOTH ":              true,
		"BREADTH ":           true,
		"BY ":                true,
		"BYTE":               true,
		"CALL ":              true,
		"CALLED ":            true,
		"CARDINALITY ":       true,
		"CASCADE ":           true,
		"CASCADED ":          true,
		"CASE ":              true,
		"CAST ":              true,
		"CATALOG ":           true,
		"CEIL ":              true,
		"CEILING ":           true,
		"CHAR ":              true,
		"CHARACTER ":         true,
		"CHARACTER_LENGTH ":  true,
		"CHAR_LENGTH ":       true,
		"CHECK ":             true,
		"CLOB ":              true,
		"CLOSE ":             true,
		"COALESCE ":          true,
		"COLLATE ":           true,
		"COLLATION ":         true,
		"COLLECT ":           true,
		"COLUMN ":            true,
		"COMMIT ":            true,
		"CONDITION ":         true,
		"CONNECT ":           true,
		"CONNECTION ":        true,
		"CONSTRAINT ":        true,
		"CONSTRAINTS ":       true,
		"CONSTRUCTOR ":       true,
		"CONTAINS ":          true,
		"CONTINUE ":          true,
		"CONVERT ":           true,
		"CORR ":              true,
		"CORRESPONDING ":     true,
		"COUNT ":             true,
		"COVAR_POP ":         true,
		"COVAR_SAMP ":        true,
		"CREATE ":            true,
		"CROSS ":             true,
		"CUBE ":              true,
		"CUME_DIST ":         true,
		"CURRENT ":           true,
		"CURRENT_CATALOG ":   true,
		"CURRENT_DATE ":      true,
		"CURRENT_PATH ":      true,
		"CURRENT_ROLE ":      true,
		"CURRENT_ROW ":       true,
		"CURRENT_SCHEMA ":    true,
		"CURRENT_TIME ":      true,
		"CURRENT_TIMESTAMP ": true,
		"CURRENT_USER ":      true,
		"CURSOR ":            true,
		"CYCLE ":             true,
		"DATA ":              true,
		"DATE ":              true,
		"DAY ":               true,
		"DEALLOCATE ":        true,
		"DEC ":               true,
		"DECIMAL ":           true,
		"DECLARE ":           true,
		"DEFAULT ":           true,
		"DEFERRABLE ":        true,
		"DEFERRED ":          true,
		"DELETE ":            true,
		"DENSE_RANK ":        true,
		"DEPTH":              true,
		"DEREF ":             true,
		"DESC ":              true,
		"DESCRIBE ":          true,
		"DESCRIPTOR ":        true,
		"DETERMINISTIC ":     true,
		"DIAGNOSTICS ":       true,
		"DIRECTORY ":         true,
		"DISCONNECT ":        true,
		"DISTINCT ":          true,
		"DO ":                true,
		"DOMAIN ":            true,
		"DOUBLE ":            true,
		"DROP ":              true,
		"DYNAMIC ":           true,
		"EACH ":              true,
		"ELEMENT ":           true,
		"ELSE ":              true,
		"ELSEIF ":            true,
		"END ":               true,
		"END_EXEC ":          true,
		"END_FRAME ":         true,
		"END_PARTITION ":     true,
		"EQUALS ":            true,
		"ESCAPE ":            true,
		"EVERY ":             true,
		"EXCEPT ":            true,
		"EXCEPTION ":         true,
		"EXEC ":              true,
		"EXECUTE ":           true,
		"EXISTS ":            true,
		"EXIT ":              true,
		"EXTERNAL ":          true,
		"EXTRACT ":           true,
		"FALSE ":             true,
		"FETCH ":             true,
		"FILTER ":            true,
		"FIRST ":             true,
		"FIRST_VALUE ":       true,
		"FLOAT ":             true,
		"FOR ":               true,
		"FOREIGN ":           true,
		"FOUND ":             true,
		"FRAME_ROW ":         true,
		"FREE ":              true,
		"FROM ":              true,
		"FULL ":              true,
		"FUNCTION ":          true,
		"FUSION ":            true,
		"GENERAL ":           true,
		"GET ":               true,
		"GLOBAL ":            true,
		"GO ":                true,
		"GOTO ":              true,
		"GRANT ":             true,
		"GROUP ":             true,
		"GROUPING ":          true,
		"GROUPS ":            true,
		"HANDLER ":           true,
		"HAVING ":            true,
		"HOLD ":              true,
		"HOUR ":              true,
		"IDENTITY ":          true,
		"IF ":                true,
		"IMMEDIATE ":         true,
		"IN ":                true,
		"INDEX":              true,
		"INDICATOR ":         true,
		"INITIALLY ":         true,
		"INNER ":             true,
		"INOUT ":             true,
		"INPUT ":             true,
		"INSENSITIVE ":       true,
		"INSERT ":            true,
		"INT ":               true,
		"INTEGER ":           true,
		"INTERSECT ":         true,
		"INTERSECTION ":      true,
		"INTERVAL ":          true,
		"INTO ":              true,
		"IP":                 true,
		"IS ":                true,
		"ISOLATION ":         true,
		"ITERATE ":           true,
		"JOIN ":              true,
		"KEY ":               true,
		"LANGUAGE ":          true,
		"LARGE ":             true,
		"LAST ":              true,
		"LAST_VALUE ":        true,
		"LATERAL ":           true,
		"LEAD ":              true,
		"LEADING ":           true,
		"LEAVE ":             true,
		"LEFT ":              true,
		"LEVEL ":             true,
		"LIKE ":              true,
		"LIKE_REGEX ":        true,
		"LIMIT ":             true,
		"LN ":                true,
		"LOCAL ":             true,
		"LOCALTIME ":         true,
		"LOCALTIMESTAMP ":    true,
		"LOCATOR ":           true,
		"LONG":               true,
		"LOOP ":              true,
		"LOWER ":             true,
		"MAP ":               true,
		"MATCH ":             true,
		"MAX":                true,
		"MEMBER ":            true,
		"MERGE ":             true,
		"METHOD ":            true,
		"MIN ":               true,
		"MINUTE ":            true,
		"MOD ":               true,
		"MODIFIES ":          true,
		"MODULE ":            true,
		"MONTH ":             true,
		"MULTISET ":          true,
		"NAMES ":             true,
		"NATIONAL ":          true,
		"NATURAL ":           true,
		"NCHAR ":             true,
		"NCLOB ":             true,
		"NEW ":               true,
		"NEXT ":              true,
		"NO ":                true,
		"NONE ":              true,
		"NORMALIZE ":         true,
		"NOT ":               true,
		"NTH_VALUE ":         true,
		"NTILE ":             true,
		"NULL ":              true,
		"NULLIF ":            true,
		"NULLS":              true,
		"NUMERIC ":           true,
		"OBJECT ":            true,
		"OCTET_LENGTH ":      true,
		"OF ":                true,
		"OFFSET ":            true,
		"OLD ":               true,
		"ON ":                true,
		"ONLY ":              true,
		"OPEN ":              true,
		"OPTION ":            true,
		"OR ":                true,
		"ORDER ":             true,
		"ORDINALITY ":        true,
		"OUT ":               true,
		"OUTER ":             true,
		"OUTPUT ":            true,
		"OVER ":              true,
		"OVERLAPS ":          true,
		"OVERLAY ":           true,
		"PAD ":               true,
		"PARAMETER ":         true,
		"PARTIAL ":           true,
		"PARTITION ":         true,
		"PATH ":              true,
		"PERCENT ":           true,
		"PERCENTILE_CONT ":   true,
		"PERCENTILE_DISC ":   true,
		"PERCENT_RANK ":      true,
		"PERIOD ":            true,
		"PERSISTENT ":        true,
		"PORTION ":           true,
		"POSITION ":          true,
		"POSITION_REGEX ":    true,
		"POWER ":             true,
		"PRECEDES ":          true,
		"PRECISION ":         true,
		"PREPARE ":           true,
		"PRESERVE ":          true,
		"PRIMARY ":           true,
		"PRIOR ":             true,
		"PRIVILEGES ":        true,
		"PROCEDURE ":         true,
		"PUBLIC ":            true,
		"RANGE ":             true,
		"RANK ":              true,
		"READ ":              true,
		"READS ":             true,
		"REAL ":              true,
		"RECURSIVE ":         true,
		"REF ":               true,
		"REFERENCES ":        true,
		"REFERENCING ":       true,
		"REGR_AVGX ":         true,
		"REGR_AVGY ":         true,
		"REGR_COUNT ":        true,
		"REGR_INTERCEPT ":    true,
		"REGR_R2 ":           true,
		"REGR_SLOPE ":        true,
		"REGR_SXX ":          true,
		"REGR_SXYREGR_SYY ":  true,
		"RELATIVE ":          true,
		"RELEASE ":           true,
		"REPEAT ":            true,
		"RESET":              true,
		"RESIGNAL ":          true,
		"RESTRICT ":          true,
		"RESULT ":            true,
		"RETURN ":            true,
		"RETURNS ":           true,
		"REVOKE ":            true,
		"RIGHT ":             true,
		"ROLE ":              true,
		"ROLLBACK ":          true,
		"ROLLUP ":            true,
		"ROUTINE ":           true,
		"ROW ":               true,
		"ROWS ":              true,
		"ROW_NUMBER ":        true,
		"SAVEPOINT ":         true,
		"SCHEMA ":            true,
		"SCOPE":              true,
		"SCROLL":             true,
		"SEARCH":             true,
		"SECOND":             true,
		"SECTION":            true,
		"SELECT":             true,
		"SENSITIVE":          true,
		"SESSION":            true,
		"SESSION_USER":       true,
		"SET":                true,
		"SETS":               true,
		"SHORT":              true,
		"SIGNAL":             true,
		"SIMILAR":            true,
		"SIZE":               true,
		"SMALLINT":           true,
		"SOME":               true,
		"SPACE":              true,
		"SPECIFIC":           true,
		"SPECIFICTYPE":       true,
		"SQL":                true,
		"SQLCODE":            true,
		"SQLERROR":           true,
		"SQLEXCEPTION":       true,
		"SQLSTATE":           true,
		"SQLWARNING":         true,
		"SQRT":               true,
		"START":              true,
		"STATE ":             true,
		"STATIC":             true,
		"STDDEV_POP":         true,
		"STDDEV_SAMP":        true,
		"STRATIFY":           true,
		"STRATIFY ":          true,
		"STRING ":            true,
		"SUBMULTISET":        true,
		"SUBSTRING":          true,
		"SUBSTRING_REGEX":    true,
		"SUCCEEDSBLOB":       true,
		"SUM ":               true,
		"SYMMETRIC":          true,
		"SYSTEM":             true,
		"SYSTEM_TIME":        true,
		"SYSTEM_USER":        true,
		"TABLE":              true,
		"TABLESAMPLE":        true,
		"TEMPORARY":          true,
		"THEN":               true,
		"TIME":               true,
		"TIMESTAMP":          true,
		"TIMEZONE_HOUR":      true,
		"TIMEZONE_MINUTE":    true,
		"TO":                 true,
		"TRAILING":           true,
		"TRANSACTION":        true,
		"TRANSIENT":          true,
		"TRANSLATE":          true,
		"TRANSLATE_REGEX":    true,
		"TRANSLATION":        true,
		"TREAT":              true,
		"TRIGGER":            true,
		"TRIM":               true,
		"TRIM_ARRAY":         true,
		"TRUE":               true,
		"TRUNCATE":           true,
		"TRY_CAST ":          true,
		"UESCAPE":            true,
		"UNBOUNDED":          true,
		"UNDER":              true,
		"UNDO":               true,
		"UNION":              true,
		"UNIQUE":             true,
		"UNKNOWN":            true,
		"UNNEST":             true,
		"UNTIL":              true,
		"UPDATE":             true,
		"UPPER":              true,
		"USAGE":              true,
		"USER":               true,
		"USING":              true,
		"VALUE":              true,
		"VALUES":             true,
		"VALUE_OF":           true,
		"VARBINARY":          true,
		"VARCHAR":            true,
		"VARYING":            true,
		"VAR_POP":            true,
		"VAR_SAMP":           true,
		"VERSIONING":         true,
		"VIEW":               true,
		"WHEN":               true,
		"WHENEVER":           true,
		"WHERE":              true,
		"WHILE":              true,
		"WIDTH_BUCKET":       true,
		"WINDOW":             true,
		"WITH":               true,
		"WITHIN":             true,
		"WITHOUT":            true,
		"WORK":               true,
		"WRITE":              true,
		"YEAR ":              true,
		"ZONE ":              true,
	}
)

type crate struct {
	core.Base
}

func (db *crate) Init(d *core.DB, uri *core.Uri, drivername, dataSourceName string) error {
	return db.Base.Init(d, db, uri, drivername, dataSourceName)
}

func (db *crate) SqlType(c *core.Column) string {
	var res string
	switch t := c.SQLType.Name; t {
	case core.Varchar:
		return "string"
	case core.TinyInt:
		res = core.SmallInt
		return res
	case core.Bit:
		res = core.Boolean
		return res
	case core.MediumInt, core.Int, core.Integer:
		if c.IsAutoIncrement {
			return core.Long
		}
		return core.Integer
	case core.BigInt:
		if c.IsAutoIncrement {
			return "long"
		}
		return core.BigInt
	case core.Serial, core.BigSerial:
		c.IsAutoIncrement = true
		c.Nullable = false
		res = t
	case core.Binary, core.VarBinary:
		return core.Bytea
	case core.DateTime:
		res = core.TimeStamp
	case core.TimeStampz:
		// return "timestamp with time zone"
		res = core.TimeStamp
	case core.Float:
		res = "float" //core.Real
	case core.TinyText, core.MediumText, core.LongText:
		res = core.Text
	case core.NVarchar:
		res = "string"
	case core.Uuid:
		res = core.Uuid
	case core.Blob, core.TinyBlob, core.MediumBlob, core.LongBlob:
		return core.Bytea
	case core.Double:
		return "DOUBLE PRECISION"
	default:
		if c.IsAutoIncrement {
			return core.Serial
		}
		res = t
	}

	hasLen1 := (c.Length > 0)
	hasLen2 := (c.Length2 > 0)

	if hasLen2 {
		res += "(" + strconv.Itoa(c.Length) + "," + strconv.Itoa(c.Length2) + ")"
	} else if hasLen1 {
		res += "(" + strconv.Itoa(c.Length) + ")"
	}
	return res
}

func (db *crate) SupportInsertMany() bool {
	return true
}

func (db *crate) IsReserved(name string) bool {
	_, ok := crateReservedWords[name]
	return ok
}

func (db *crate) Quote(name string) string {
	name = strings.Replace(name, ".", `"."`, -1)
	return "\"" + name + "\""
}

func (db *crate) QuoteStr() string {
	return "\""
}

func (db *crate) AutoIncrStr() string {
	return ""
}

func (db *crate) SupportEngine() bool {
	return false
}

func (db *crate) SupportCharset() bool {
	return false
}

func (db *crate) IndexOnTable() bool {
	return false
}

func (db *crate) IndexCheckSql(tableName, idxName string) (string, []interface{}) {
	args := []interface{}{tableName, idxName}
	return `SELECT indexname FROM pg_indexes ` +
		`WHERE tablename = ? AND indexname = ?`, args
}

func (db *crate) TableCheckSql(tableName string) (string, []interface{}) {
	args := []interface{}{tableName}
	return `SELECT tablename FROM pg_tables WHERE tablename = ?`, args
}

func (db *crate) ModifyColumnSql(tableName string, col *core.Column) string {
	return fmt.Sprintf("alter table %s ALTER COLUMN %s TYPE %s",
		tableName, col.Name, db.SqlType(col))
}

func (db *crate) DropIndexSql(tableName string, index *core.Index) string {
	//var unique string
	quote := db.Quote
	idxName := index.Name

	if !strings.HasPrefix(idxName, "UQE_") &&
		!strings.HasPrefix(idxName, "IDX_") {
		if index.Type == core.UniqueType {
			idxName = fmt.Sprintf("UQE_%v_%v", tableName, index.Name)
		} else {
			idxName = fmt.Sprintf("IDX_%v_%v", tableName, index.Name)
		}
	}
	return fmt.Sprintf("DROP INDEX %v", quote(idxName))
}

func (db *crate) IsColumnExist(tableName, colName string) (bool, error) {
	args := []interface{}{tableName, colName}
	query := "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = $1" +
		" AND column_name = $2"
	db.LogSQL(query, args)

	rows, err := db.DB().Query(query, args...)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	return rows.Next(), nil
}

func (db *crate) ShowCreateNull() bool {
	return false
}

func (db *crate) GetColumns(tableName string) ([]string, map[string]*core.Column, error) {
	schemaName := "doc"
	rawTableName := tableName
	if strings.Contains(tableName, ".") {
		parts := strings.Split(tableName, ".")
		schemaName = parts[0]
		rawTableName = strings.Join(parts[1:], ".")
	}
	fmt.Println()
	fmt.Println("TABLENAME", rawTableName, schemaName)
	fmt.Println()
	fmt.Println()
	fmt.Println("====================")
	// > crate 2.1 use "information_schema.key_column_usage" to get primary key info
	// <= 2.1 use "information_schema.table_constraints"
	// EXTRA --> autoIncrement
	s := `SELECT c.column_name, c.is_nullable, c.column_default, UPPER(c.data_type), tc.constraint_name, tc.constraint_type FROM information_schema.columns c 
	LEFT JOIN information_schema.table_constraints tc 
	ON c.column_name = ANY(tc.constraint_name) AND c.table_schema = tc.table_schema AND c.table_name = tc.table_name
	WHERE c.table_schema = ? and c.table_name = ?`
	args := []interface{}{schemaName, rawTableName}
	db.LogSQL(s, args)

	rows, err := db.DB().Query(s, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	cols := make(map[string]*core.Column)
	colSeq := make([]string, 0)

	for rows.Next() {
		col := new(core.Column)
		col.Indexes = make(map[string]int)

		var colName, dataType string
		var colDefaultVal interface{}
		var isNullable bool
		var constraintNamesVal, constraintTypeVal interface{}
		err = rows.Scan(&colName, &isNullable, &colDefaultVal, &dataType, &constraintNamesVal, &constraintTypeVal)
		if err != nil {
			return nil, nil, err
		}

		col.Name = colName
		if _, ok := core.SqlTypes[dataType]; ok {
			col.SQLType = core.SQLType{Name: dataType, DefaultLength: 0, DefaultLength2: 0}
		} else {
			return nil, nil, fmt.Errorf("Unknown colType %v", dataType)
		}
		if colDefaultVal != nil {
			col.Default = colDefaultVal.(string)
		}
		if constraintNamesVal != nil && constraintTypeVal != nil {
			if constraintTypeVal.(string) == "PRIMARY_KEY" {
				constraintNames := constraintNamesVal.([]interface{})
				if len(constraintNames) > 0 {
					for _, name := range constraintNames {
						if name.(string) == colName {
							col.IsPrimaryKey = true
							break
						}
					}
				}
			}
		}

		// fmt.Printf("ROW: %s %s %v %t %v\n", colName, dataType, colDefaultVal, isNullable, constraintNamesVal)
		cols[col.Name] = col
		colSeq = append(colSeq, col.Name)
	}

	return colSeq, cols, nil
}

func (db *crate) GetTables() ([]*core.Table, error) {
	s := "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema NOT IN ('blob', 'information_schema', 'sys', 'pg_catalog')"
	db.LogSQL(s, []interface{}{})

	rows, err := db.DB().Query(s)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := make([]*core.Table, 0)
	for rows.Next() {
		table := core.NewEmptyTable()
		var tableSchema string
		var tableName string
		err = rows.Scan(&tableSchema, &tableName)
		if err != nil {
			return nil, err
		}
		table.Name = fmt.Sprintf("%s.%s", tableSchema, tableName)
		tables = append(tables, table)
	}
	return tables, nil
}

func (db *crate) GetIndexes(tableName string) (map[string]*core.Index, error) {
	return make(map[string]*core.Index, 0), nil
}

func (db *crate) Filters() []core.Filter {
	return []core.Filter{&core.IdFilter{}, &core.QuoteFilter{}, &core.SeqFilter{Prefix: "$", Start: 1}}
}

// func (db *crate) DropTableSql(tableName string) string {
// 	fmt.Println()
// 	fmt.Println()
// 	fmt.Println("----------------->>>> DROP TABLE: ", tableName)
// 	fmt.Println()
// 	fmt.Println()
// 	return ""
// }

type crateDriver struct {
}

func (p *crateDriver) Parse(driverName, dataSourceName string) (*core.Uri, error) {
	db := &core.Uri{DbType: core.CRATE}

	if strings.HasPrefix(dataSourceName, "http://") {
		u, err := url.Parse(dataSourceName)
		if err != nil {
			return db, err
		}
		db.Host = u.Host
		db.Port = u.Port()
	}

	return db, nil
}
