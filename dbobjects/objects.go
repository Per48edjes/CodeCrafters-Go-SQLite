/*
Package dbobjects reads bytes from a SQLite database file into objects
defined herein and provides functionality to perform operations on said objects
in application code.
*/
package dbobjects

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"slices"
)

type BTreePageType uint8

type DatabaseFile struct {
	*os.File
}

const (
	InteriorIndex BTreePageType = 2
	InteriorTable BTreePageType = 5
	LeafIndex     BTreePageType = 10
	LeafTable     BTreePageType = 13
)

type DatabaseHeader struct {
	PageSize  uint16
	PageCount uint32
}

type Page struct {
	PageSize      uint16
	PageType      BTreePageType
	PageStart     int64
	CellCount     uint32
	CellAddresses []uint16
}

type Row struct {
	RecordSize       uint64
	RowID            uint64
	RecordHeaderSize uint64
	Columns          []Column
}

type Column struct {
	SerialType uint64
	Data       []byte
}

func (databaseFile *DatabaseFile) NewDatabaseHeader() (*DatabaseHeader, error) {
	// Seek to the beginning of the file
	if _, err := databaseFile.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to the beginning of the file: %w", err)
	}

	header := make([]byte, 100)
	var databaseHeader DatabaseHeader

	if n, err := databaseFile.Read(header); err != nil || n != 100 {
		return nil, fmt.Errorf("failed to read database header (bytes read: %d): %w", n, err)
	}

	// Extract the page size from database header
	if err := binary.Read(bytes.NewReader(header[16:18]), binary.BigEndian, &databaseHeader.PageSize); err != nil {
		return nil, fmt.Errorf("failed to read page size: %w", err)
	}
	return &databaseHeader, nil
}

func (databaseFile *DatabaseFile) NewPage(databaseHeader *DatabaseHeader, pageNumber uint32) (*Page, error) {
	var page Page

	switch pageNumber {
	case 0:
		return nil, fmt.Errorf("page number must be greater than 0")
	case 1:
		page.PageStart = 100 + (int64(pageNumber-1) * int64(databaseHeader.PageSize))
		page.PageSize = databaseHeader.PageSize - 100
	default:
		page.PageStart = 100 + (int64(pageNumber-1) * int64(databaseHeader.PageSize))
		page.PageSize = databaseHeader.PageSize
	}

	// Extract page type from page header
	typeFlag := make([]byte, 1)

	if _, err := databaseFile.Seek(page.PageStart, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to the beginning of the file: %w", err)
	}

	if n, err := databaseFile.Read(typeFlag); err != nil || n != 1 {
		return nil, fmt.Errorf("failed to read page type (bytes read: %d): %w", n, err)
	}

	// Decode the page header type & determine page header length in bytes
	var header []byte
	switch BTreePageType(typeFlag[0]) {
	case InteriorIndex:
		page.PageType = InteriorIndex
		header = make([]byte, 12)
	case InteriorTable:
		page.PageType = InteriorTable
		header = make([]byte, 12)
	case LeafIndex:
		page.PageType = LeafIndex
		header = make([]byte, 8)
	case LeafTable:
		page.PageType = LeafTable
		header = make([]byte, 8)
	default:
		return nil, fmt.Errorf("unknown page type: %d", typeFlag[0])
	}

	if n, err := databaseFile.Read(header); err != nil || n != len(header) {
		return nil, fmt.Errorf("failed to read page header (bytes read: %d): %w", n, err)
	}

	// Extract cell count from page header
	if err := binary.Read(bytes.NewReader(header[3:5]), binary.BigEndian, &page.CellCount); err != nil {
		return nil, fmt.Errorf("failed to read cell count: %w", err)
	}

	// Extract cell addresses from cell pointer array
	cellPointerArr := make([]byte, 2*page.CellCount)
	if n, err := databaseFile.Read(cellPointerArr); err != nil || n != len(cellPointerArr) {
		return nil, fmt.Errorf("failed to read cell pointer array (bytes read: %d): %w", n, err)
	}
	for i := 0; i < len(cellPointerArr); i = i + 2 {
		page.CellAddresses = append(page.CellAddresses, binary.BigEndian.Uint16(cellPointerArr[i:i+2]))
	}
	slices.Sort(page.CellAddresses)

	return &page, nil
}

func (databaseFile *DatabaseFile) ReadAllRows(page *Page) ([]*Row, error) {
	rows := make([]*Row, 0, page.CellCount)

	for i := uint32(0); i < page.CellCount; i++ {
		row := &Row{}
		recordStart := page.PageStart + int64(page.CellAddresses[i])

		// Seek to cell start
		if _, err := databaseFile.Seek(recordStart, io.SeekStart); err != nil {
			return nil, fmt.Errorf("seeking to cell %d: %w", i, err)
		}
		cellReader := bufio.NewReaderSize(databaseFile, int(page.PageSize))

		// Read record header varints
		var consumed, n int
		recordSize, n, err := ReadVarint(cellReader)
		if err != nil {
			return nil, fmt.Errorf("reading record size at cell %d: %w", i, err)
		}
		row.RecordSize = recordSize
		consumed += n

		rowID, n, err := ReadVarint(cellReader)
		if err != nil {
			return nil, fmt.Errorf("reading row ID at cell %d: %w", i, err)
		}
		row.RowID = rowID
		consumed += n

		headerSize, n, err := ReadVarint(cellReader)
		if err != nil {
			return nil, fmt.Errorf("reading header size at cell %d: %w", i, err)
		}
		row.RecordHeaderSize = headerSize
		consumed += n

		// Read serial types for all columns
		remainingHeaderBytes := int64(row.RecordHeaderSize) - int64(consumed)
		serialReader := bufio.NewReader(io.LimitReader(cellReader, remainingHeaderBytes))

		for {
			serialType, _, err := ReadVarint(serialReader)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("reading serial type at cell %d: %w", i, err)
			}
			row.Columns = append(row.Columns, Column{SerialType: serialType})
		}

		// TODO: Read data for each column

		rows = append(rows, row)
	}

	return rows, nil
}
