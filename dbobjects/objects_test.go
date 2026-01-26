package dbobjects

import (
	"os"
	"path/filepath"
	"testing"
)

func sampleDatabasePath() string {
	if path := os.Getenv("SAMPLE_DB_PATH"); path != "" {
		return path
	}
	return filepath.Join("..", "sample.db")
}

func openSampleDatabase(t *testing.T) (*DatabaseFile, *DatabaseHeader) {
	t.Helper()

	file, err := os.Open(sampleDatabasePath())
	if err != nil {
		t.Fatalf("opening sample database: %v", err)
	}

	t.Cleanup(func() {
		if cerr := file.Close(); cerr != nil {
			t.Errorf("closing sample database: %v", cerr)
		}
	})

	dbFile := &DatabaseFile{File: file}
	header, err := dbFile.NewDatabaseHeader()
	if err != nil {
		t.Fatalf("reading database header: %v", err)
	}

	return dbFile, header
}

func TestNewDatabaseHeaderReadsPageSize(t *testing.T) {
	_, header := openSampleDatabase(t)

	const expectedPageSize = 4096
	if int(header.PageSize) != expectedPageSize {
		t.Fatalf("unexpected page size: got %d, want %d", header.PageSize, expectedPageSize)
	}
}

func TestNewPageParsesLeafTable(t *testing.T) {
	dbFile, header := openSampleDatabase(t)

	page, err := dbFile.NewPage(header, 2)
	if err != nil {
		t.Fatalf("reading page: %v", err)
	}

	if page.PageType != LeafTable {
		t.Fatalf("unexpected page type: got %d, want %d", page.PageType, LeafTable)
	}

	const expectedCells = 4
	if int(page.CellCount) != expectedCells {
		t.Fatalf("unexpected cell count: got %d, want %d", page.CellCount, expectedCells)
	}

	if len(page.CellAddresses) != expectedCells {
		t.Fatalf("unexpected cell addresses count: got %d, want %d", len(page.CellAddresses), expectedCells)
	}

	if len(page.Data) != int(header.PageSize) {
		t.Fatalf("unexpected page data length: got %d, want %d", len(page.Data), header.PageSize)
	}
}

func TestReadAllRowsFromSampleApples(t *testing.T) {
	dbFile, header := openSampleDatabase(t)

	page, err := dbFile.NewPage(header, 2)
	if err != nil {
		t.Fatalf("reading page: %v", err)
	}

	rows, err := ReadAllRows(page)
	if err != nil {
		t.Fatalf("reading rows: %v", err)
	}

	const expectedRows = 4
	if len(rows) != expectedRows {
		t.Fatalf("unexpected row count: got %d, want %d", len(rows), expectedRows)
	}

	const expectedColumns = 3
	for i, row := range rows {
		if len(row.Columns) != expectedColumns {
			t.Fatalf("row %d unexpected column count: got %d, want %d", i, len(row.Columns), expectedColumns)
		}
	}

	expectedNames := []string{"Granny Smith", "Fuji", "Honeycrisp", "Golden Delicious"}
	expectedColors := []string{"Light Green", "Red", "Blush Red", "Yellow"}

	for i, row := range rows {
		if row.RowID != uint64(i+1) {
			t.Fatalf("row %d unexpected rowid: got %d, want %d", i, row.RowID, i+1)
		}

		if nameValue := row.Columns[1].DecodedValue.(string); nameValue != expectedNames[i] {
			t.Fatalf("row %d unexpected name: got %q, want %q", i, nameValue, expectedNames[i])
		}

		if colorValue := row.Columns[2].DecodedValue.(string); colorValue != expectedColors[i] {
			t.Fatalf("row %d unexpected color: got %q, want %q", i, colorValue, expectedColors[i])
		}
	}
}
