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

func ExtractTableMetadata[T string | uint32](schemaPage *Page, col string) ([]T, error) {
	rows, err := ReadAllRows(schemaPage)
	if err != nil {
		return nil, fmt.Errorf("read schema rows: %w", err)
	}

	metadataColIdx := SqliteSchemaCol(col)
	var metadata []T
	metadata = make([]T, 0, len(rows))

	for _, row := range rows {
		if metadataColIdx >= len(row.Columns) {
			return nil, fmt.Errorf("%s column missing in schema row", col)
		}

		md, ok := row.Columns[metadataColIdx].DecodedValue.(T)
		if !ok {
			return nil, fmt.Errorf("rowid %d: is not text", row.RowID)
		}
		metadata = append(metadata, md)
	}

	return metadata, nil
}

func MetadataLookup[T string | uint32](schemaPage *Page, tableName string, col string) (T, error) {
	var zero T
	rows, err := ReadAllRows(schemaPage)
	if err != nil {
		return zero, fmt.Errorf("read schema rows: %w", err)
	}

	tblNameIdx := SqliteSchemaCol("tbl_name")
	metadataColIdx := SqliteSchemaCol(col)

	for _, row := range rows {
		if tblNameIdx >= len(row.Columns) {
			return zero, errors.New("tbl_name column missing in schema row")
		}

		name, ok := row.Columns[tblNameIdx].DecodedValue.(string)
		if !ok {
			return zero, fmt.Errorf("rowid %d: tbl_name is not text", row.RowID)
		}

		if name == tableName {
			md, ok := row.Columns[metadataColIdx].DecodedValue.(T)
			if !ok {
				return zero, fmt.Errorf("rowid %d: %s is not string or uint32", row.RowID, col)
			}
			return md, nil
		}
	}

	return zero, fmt.Errorf("table %s not found in schema", tableName)
}
