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

	databaseHeaderBytes = 100
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
	if _, err := databaseFile.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek database start: %w", err)
	}

	header := make([]byte, databaseHeaderBytes)
	var databaseHeader DatabaseHeader

	if n, err := databaseFile.Read(header); err != nil || n != databaseHeaderBytes {
		return nil, fmt.Errorf("read database header (%d bytes): %w", n, err)
	}

	databaseHeader.PageSize = binary.BigEndian.Uint16(header[16:18])
	return &databaseHeader, nil
}

func (databaseFile *DatabaseFile) NewPage(databaseHeader *DatabaseHeader, pageNumber uint32) (*Page, error) {
	start, size, err := pageBounds(databaseHeader, pageNumber)
	if err != nil {
		return nil, err
	}

	page := &Page{PageStart: start, PageSize: size}
	page.Data = make([]byte, page.PageSize)

	sectionReader := io.NewSectionReader(databaseFile, page.PageStart, int64(page.PageSize))
	if _, err := io.ReadFull(sectionReader, page.Data); err != nil {
		return nil, fmt.Errorf("page %d: read bytes: %w", pageNumber, err)
	}

	if len(page.Data) == 0 {
		return nil, fmt.Errorf("page %d: no data", pageNumber)
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
		return nil, fmt.Errorf("page %d: unknown type %d", pageNumber, typeFlag)
	}

	if len(page.Data) < offset+headerLen {
		return nil, fmt.Errorf("page %d: header truncated", pageNumber)
	}
	header := page.Data[offset : offset+headerLen]
	offset += headerLen

	page.CellCount = binary.BigEndian.Uint16(header[2:4])
	pointerBytes := int(page.CellCount) * 2
	if len(page.Data) < offset+pointerBytes {
		return nil, fmt.Errorf("page %d: cell pointer array truncated", pageNumber)
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
		return nil, fmt.Errorf("cell %d: read record size: %w", cellIndex, err)
	}
	row.RecordSize = recordSize

	rowID, _, err := ReadVarint(cellReader)
	if err != nil {
		return nil, fmt.Errorf("cell %d: read row ID: %w", cellIndex, err)
	}
	row.RowID = rowID

	headerSize, headerBytes, err := ReadVarint(cellReader)
	if err != nil {
		return nil, fmt.Errorf("cell %d: read header size: %w", cellIndex, err)
	}
	row.RecordHeaderSize = headerSize

	remainingHeaderBytes := int64(row.RecordHeaderSize) - int64(headerBytes)
	if remainingHeaderBytes < 0 {
		return nil, fmt.Errorf("cell %d: negative header size (size=%d, bytes=%d)", cellIndex, row.RecordHeaderSize, headerBytes)
	}

	serialReader := bufio.NewReader(io.LimitReader(cellReader, remainingHeaderBytes))

	for {
		serialType, _, err := ReadVarint(serialReader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("cell %d: read serial type: %w", cellIndex, err)
		}
		row.Columns = append(row.Columns, Column{SerialType: serialType})
	}

	// TODO: Read data for each column

	return row, nil
}

func (databaseFile *DatabaseFile) ReadAllRows(page *Page) ([]*Row, error) {
	rows := make([]*Row, 0, int(page.CellCount))

	for i := range int(page.CellCount) {
		row, err := ReadRow(page, i)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}

	return rows, nil
}

func pageBounds(databaseHeader *DatabaseHeader, pageNumber uint32) (int64, uint16, error) {
	if databaseHeader == nil {
		return 0, 0, fmt.Errorf("database header is nil")
	}

	if pageNumber == 0 {
		return 0, 0, fmt.Errorf("page number must be greater than 0")
	}

	if pageNumber == 1 {
		if databaseHeader.PageSize <= databaseHeaderBytes {
			return 0, 0, fmt.Errorf("page size %d too small for header", databaseHeader.PageSize)
		}
		return databaseHeaderBytes, databaseHeader.PageSize - databaseHeaderBytes, nil
	}

	return int64(pageNumber-1) * int64(databaseHeader.PageSize), databaseHeader.PageSize, nil
}
