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
	CellCount     uint16
	CellAddresses []uint16
	Data          []byte
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
	if pageNumber == 0 {
		return nil, fmt.Errorf("page number must be greater than 0")
	}

	page := &Page{}

	switch pageNumber {
	case 1:
		page.PageStart = 100
		page.PageSize = databaseHeader.PageSize - 100
	default:
		page.PageStart = int64(pageNumber-1) * int64(databaseHeader.PageSize)
		page.PageSize = databaseHeader.PageSize
	}

	page.Data = make([]byte, page.PageSize)
	sectionReader := io.NewSectionReader(databaseFile, page.PageStart, int64(page.PageSize))
	if _, err := io.ReadFull(sectionReader, page.Data); err != nil {
		return nil, fmt.Errorf("failed to read page bytes: %w", err)
	}

	if len(page.Data) == 0 {
		return nil, fmt.Errorf("page %d contains no data", pageNumber)
	}

	typeFlag := page.Data[0]
	offset := 1
	var headerLen int

	switch BTreePageType(typeFlag) {
	case InteriorIndex:
		page.PageType = InteriorIndex
		headerLen = 11
	case InteriorTable:
		page.PageType = InteriorTable
		headerLen = 11
	case LeafIndex:
		page.PageType = LeafIndex
		headerLen = 7
	case LeafTable:
		page.PageType = LeafTable
		headerLen = 7
	default:
		return nil, fmt.Errorf("unknown page type: %d", typeFlag)
	}

	if len(page.Data) < offset+headerLen {
		return nil, fmt.Errorf("page %d header truncated", pageNumber)
	}
	header := page.Data[offset : offset+headerLen]
	offset += headerLen

	page.CellCount = binary.BigEndian.Uint16(header[2:4])
	pointerBytes := int(page.CellCount) * 2
	if len(page.Data) < offset+pointerBytes {
		return nil, fmt.Errorf("page %d cell pointer array truncated", pageNumber)
	}

	page.CellAddresses = make([]uint16, 0, page.CellCount)
	for i := 0; i < pointerBytes; i += 2 {
		page.CellAddresses = append(page.CellAddresses, binary.BigEndian.Uint16(page.Data[offset+i:offset+i+2]))
	}

	return page, nil
}

func ReadRow(page *Page, cellIndex int) (*Row, error) {
	if page == nil {
		return nil, fmt.Errorf("page is nil")
	}

	cellData, err := CellData(page, cellIndex)
	if err != nil {
		return nil, err
	}

	row := &Row{}
	cellReader := bufio.NewReader(bytes.NewReader(cellData))

	recordSize, _, err := ReadVarint(cellReader)
	if err != nil {
		return nil, fmt.Errorf("reading record size at cell %d: %w", cellIndex, err)
	}
	row.RecordSize = recordSize

	rowID, _, err := ReadVarint(cellReader)
	if err != nil {
		return nil, fmt.Errorf("reading row ID at cell %d: %w", cellIndex, err)
	}
	row.RowID = rowID

	headerSize, headerBytes, err := ReadVarint(cellReader)
	if err != nil {
		return nil, fmt.Errorf("reading header size at cell %d: %w", cellIndex, err)
	}
	row.RecordHeaderSize = headerSize

	remainingHeaderBytes := int64(row.RecordHeaderSize) - int64(headerBytes)
	if remainingHeaderBytes < 0 {
		return nil, fmt.Errorf("negative header size at cell %d (headerSize=%d, headerBytes=%d)", cellIndex, row.RecordHeaderSize, headerBytes)
	}

	serialReader := bufio.NewReader(io.LimitReader(cellReader, remainingHeaderBytes))

	for {
		serialType, _, err := ReadVarint(serialReader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("reading serial type at cell %d: %w", cellIndex, err)
		}
		row.Columns = append(row.Columns, Column{SerialType: serialType})
	}

	// TODO: Read data for each column

	return row, nil
}

func (databaseFile *DatabaseFile) ReadAllRows(page *Page) ([]*Row, error) {
	rows := make([]*Row, 0, page.CellCount)

	for i := 0; i < int(page.CellCount); i++ {
		row, err := ReadRow(page, i)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}

	return rows, nil
}
