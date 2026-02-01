package engine

import (
	"fmt"

	"github.com/codecrafters-io/sqlite-starter-go/internal/db"
	"github.com/xwb1989/sqlparser"
)

func TableNameFromQuery(query string) (string, error) {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return "", fmt.Errorf("parse query: %w", err)
	}

	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		for _, expr := range stmt.From {
			ate, ok := expr.(*sqlparser.AliasedTableExpr)
			if !ok {
				continue
			}

			tbl, ok := ate.Expr.(sqlparser.TableName)
			if !ok {
				continue
			}

			return tbl.Name.String(), nil
		}
		return "", fmt.Errorf("select query missing table")
	}

	return "", fmt.Errorf("unsupported query type: %T", stmt)
}

func RowCount(path, tableName string) (uint16, error) {
	_, schemaPage, err := db.LoadPage(path, 1)
	if err != nil {
		return 0, err
	}

	rootPageNum, err := db.RootPageLookup(tableName, schemaPage)
	if err != nil {
		return 0, err
	}

	_, rootPage, err := db.LoadPage(path, rootPageNum)
	if err != nil {
		return 0, err
	}

	return rootPage.CellCount, nil
}
