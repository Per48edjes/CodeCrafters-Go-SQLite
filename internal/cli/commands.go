package cli

import (
	"fmt"

	"github.com/codecrafters-io/sqlite-starter-go/internal/db"
	"github.com/codecrafters-io/sqlite-starter-go/internal/engine"
	"github.com/xwb1989/sqlparser"
)

func HandleDBInfo(path string) error {
	dbHeader, schemaPage, err := db.LoadPage(path, 1)
	if err != nil {
		return err
	}

	fmt.Printf("database page size: %d\n", dbHeader.PageSize)
	fmt.Printf("number of tables: %d", schemaPage.CellCount)
	return nil
}

func HandleTables(path string) error {
	_, schemaPage, err := db.LoadPage(path, 1)
	if err != nil {
		return err
	}

	names, err := db.ExtractTableMetadata[string](schemaPage, "tbl_name")
	if err != nil {
		return err
	}

	for _, name := range names {
		fmt.Println(name)
	}
	return nil
}

func HandleQuery(path, query string) error {
	_, schemaPage, err := db.LoadPage(path, 1)
	if err != nil {
		return err
	}

	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return fmt.Errorf("parse query: %w", err)
	}

	s, ok := stmt.(*sqlparser.Select)
	if !ok {
		return fmt.Errorf("unsupported query type: %T", stmt)
	}

	tableName, err := engine.TableNameFromQuery(s)
	if err != nil {
		return err
	}

	colMap, err := engine.ExtractTableColumnIndices(schemaPage, tableName)
	if err != nil {
		return err
	}

	// we only run this line if selecting columns

	// TODO: if we see COUNT(*)
	sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if fn, ok := node.(*sqlparser.FuncExpr); ok {
			if fn.Name.Lowered() == "count" {
				sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
				}, fn.Exprs...)
				return false, nil
			}
		}
	}, s.SelectExprs...)

	count, err := engine.RowCount(path, tableName)
	if err != nil {
		return err
	}

	fmt.Println(count)
	return nil
}
