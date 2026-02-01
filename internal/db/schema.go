package db

import (
	"errors"
	"fmt"
)

func SqliteSchemaCol(name string) int {
	switch name {
	case "type":
		return 0
	case "name":
		return 1
	case "tbl_name":
		return 2
	case "rootpage":
		return 3
	case "sql":
		return 4
	default:
		panic("unknown sqlite_schema column: " + name)
	}
}

func ExtractTableNames(schemaPage *Page) ([]string, error) {
	rows, err := ReadAllRows(schemaPage)
	if err != nil {
		return nil, fmt.Errorf("read schema rows: %w", err)
	}

	tblNameIdx := SqliteSchemaCol("tbl_name")
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

func RootPageLookup(tableName string, schemaPage *Page) (uint32, error) {
	rows, err := ReadAllRows(schemaPage)
	if err != nil {
		return 0, fmt.Errorf("read schema rows: %w", err)
	}

	tblNameIdx := SqliteSchemaCol("tbl_name")
	rootPageIdx := SqliteSchemaCol("rootpage")

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
