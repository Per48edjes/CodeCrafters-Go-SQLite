package cli

import (
	"fmt"

	"github.com/codecrafters-io/sqlite-starter-go/internal/db"
	"github.com/codecrafters-io/sqlite-starter-go/internal/engine"
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

	names, err := db.ExtractTableNames(schemaPage)
	if err != nil {
		return err
	}

	for _, name := range names {
		fmt.Println(name)
	}
	return nil
}

func HandleQuery(path, query string) error {
	tableName, err := engine.TableNameFromQuery(query)
	if err != nil {
		return err
	}

	count, err := engine.RowCount(path, tableName)
	if err != nil {
		return err
	}

	fmt.Println(count)
	return nil
}
