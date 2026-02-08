package engine

import (
	"fmt"

	"github.com/codecrafters-io/sqlite-starter-go/internal/db"
	"github.com/xwb1989/sqlparser"
)

func ExtractTableColumnIndices(schemaPage *db.Page, table string) (map[string]int, error) {
	sql, err := db.MetadataLookup[string](schemaPage, table, "sql")
	if err != nil {
		return nil, err
	}

	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parse query: %w", err)
	}

	switch stmt := stmt.(type) {
	case *sqlparser.DDL:
		colMap := make(map[string]int, len(stmt.TableSpec.Columns))
		for i, col := range stmt.TableSpec.Columns {
			colMap[col.Name.CompliantName()] = i
		}
		return colMap, nil
	}

	return nil, fmt.Errorf("unsupported query type: %T", stmt)
}

func TableNameFromQuery(s *sqlparser.Select) (string, error) {
	for _, expr := range s.From {
		ate, ok := expr.(*sqlparser.AliasedTableExpr)
		if !ok {
			continue
		}

		tbl, ok := ate.Expr.(sqlparser.TableName)
		if !ok {
			continue
		}

		// NOTE: Gets the first table name in a FROM clause
		return tbl.Name.String(), nil
	}
	return "", fmt.Errorf("select query missing table")
}

func RowCount(path, tableName string) (uint16, error) {
	_, schemaPage, err := db.LoadPage(path, 1)
	if err != nil {
		return 0, err
	}

	rootPageNum, err := db.MetadataLookup[uint32](schemaPage, tableName, "rootpage")
	if err != nil {
		return 0, err
	}

	_, rootPage, err := db.LoadPage(path, rootPageNum)
	if err != nil {
		return 0, err
	}

	return rootPage.CellCount, nil
}
