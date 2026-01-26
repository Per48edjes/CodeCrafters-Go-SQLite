package main

import (
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/codecrafters-io/sqlite-starter-go/dbobjects"
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
		log.Fatalf("unknown command %s", command)
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

func loadSchemaPage(path string) (*dbobjects.DatabaseHeader, *dbobjects.Page, error) {
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

	page, err := dbFile.NewPage(header, 1)
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
