package main

import (
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/codecrafters-io/sqlite-starter-go/dbobjects"
	"github.com/xwb1989/sqlparser"
)

// Usage: your_program.sh sample.db <command>
func main() {
	if len(os.Args) < 3 {
		log.Fatalf("usage: %s <database> <command>", os.Args[0])
	}

	databaseFilePath := os.Args[1]
	command := os.Args[2]

	var err error

	switch command {
	case ".dbinfo":
		err = handleDBInfo(databaseFilePath)
	case ".tables":
		err = handleTables(databaseFilePath)
	default:
		err = handleQuery(databaseFilePath, command)
	}

	if err != nil {
		log.Fatal(err)
	}
}

func handleDBInfo(path string) error {
	dbHeader, schemaPage, err := loadSchemaPage(path)
	if err != nil {
		return err
	}

	fmt.Printf("database page size: %d\n", dbHeader.PageSize)
	fmt.Printf("number of tables: %d", schemaPage.CellCount)
	return nil
}

func handleTables(path string) error {
	_, schemaPage, err := loadSchemaPage(path)
	if err != nil {
		return err
	}

	names, err := extractTableNames(schemaPage)
	if err != nil {
		return err
	}

	for _, name := range names {
		fmt.Println(name)
	}
	return nil
}

func handleQuery(path, query string) error {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return fmt.Errorf("parse query: %w", err)
	}

	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		var tableName string
		for _, expr := range stmt.From {
			ate, ok := expr.(*sqlparser.AliasedTableExpr)
			if !ok {
				continue
			}

			tbl, ok := ate.Expr.(sqlparser.TableName)
			if !ok {
				continue
			}

			tableName = tbl.Name.String()
		}
		err = getRowCount(path, tableName)
		if err != nil {
			return err
		}
		return nil
	}

	return fmt.Errorf("unsupported query type: %T", stmt)
}

func getRowCount(path string, tableName string) error {
	_, schemaPage, err := loadSchemaPage(path)
	if err != nil {
		return err
	}

	rootPageNum, err := rootPageLookup(tableName, schemaPage)
	if err != nil {
		return err
	}

	_, rootPage, err := loadPage(path, rootPageNum)
	if err != nil {
		return err
	}

	fmt.Println(rootPage.CellCount)
	return nil
}

func loadSchemaPage(path string) (*dbobjects.DatabaseHeader, *dbobjects.Page, error) {
	return loadPage(path, 1)
}

func loadPage(path string, pageNum uint32) (*dbobjects.DatabaseHeader, *dbobjects.Page, error) {
	if pageNum == 0 {
		return nil, nil, errors.New("page numbers start at 1")
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("open database: %w", err)
	}
	defer file.Close()

	dbFile := &dbobjects.DatabaseFile{File: file}
	header, err := dbFile.NewDatabaseHeader()
	if err != nil {
		return nil, nil, fmt.Errorf("read database header: %w", err)
	}

	page, err := dbFile.NewPage(header, pageNum)
	if err != nil {
		return nil, nil, fmt.Errorf("read schema page: %w", err)
	}

	return header, page, nil
}

func extractTableNames(schemaPage *dbobjects.Page) ([]string, error) {
	rows, err := dbobjects.ReadAllRows(schemaPage)
	if err != nil {
		return nil, fmt.Errorf("read schema rows: %w", err)
	}

	tblNameIdx := dbobjects.SqliteSchemaCol("tbl_name")
	names := make([]string, 0, len(rows))

	for _, row := range rows {
		if tblNameIdx >= len(row.Columns) {
			return nil, errors.New("tbl_name column missing in schema row")
		}

		name, ok := row.Columns[tblNameIdx].DecodedValue.(string)
		if !ok {
			return nil, fmt.Errorf("rowid %d: tbl_name is not text", row.RowID)
		}
		names = append(names, name)
	}

	return names, nil
}

func rootPageLookup(tableName string, schemaPage *dbobjects.Page) (uint32, error) {
	rows, err := dbobjects.ReadAllRows(schemaPage)
	if err != nil {
		return 0, fmt.Errorf("read schema rows: %w", err)
	}

	tblNameIdx := dbobjects.SqliteSchemaCol("tbl_name")
	rootPageIdx := dbobjects.SqliteSchemaCol("rootpage")

	for _, row := range rows {
		if tblNameIdx >= len(row.Columns) {
			return 0, errors.New("tbl_name column missing in schema row")
		}

		name, ok := row.Columns[tblNameIdx].DecodedValue.(string)
		if !ok {
			return 0, fmt.Errorf("rowid %d: tbl_name is not text", row.RowID)
		}

		if name == tableName {
			rootPage, ok := row.Columns[rootPageIdx].DecodedValue.(int64)
			if !ok {
				return 0, fmt.Errorf("rowid %d: rootpage is not int64", row.RowID)
			}
			return uint32(rootPage), nil
		}
	}

	return 0, fmt.Errorf("table %s not found in schema", tableName)
}
