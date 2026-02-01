package db

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

type Row struct {
	RecordSize       uint64
	RowID            uint64
	RecordHeaderSize uint64
	Columns          []Column
}

type Column struct {
	SerialType   uint64
	DecodedValue any
}

func CellOffset(page *Page, cellIndex int) (int, error) {
	if page == nil {
		return 0, fmt.Errorf("page is nil")
	}

	if cellIndex < 0 {
		return 0, fmt.Errorf("cell index %d out of range", cellIndex)
	}

	if cellIndex >= int(page.CellCount) {
		return 0, fmt.Errorf("cell index %d out of range", cellIndex)
	}

	if cellIndex >= len(page.CellAddresses) {
		return 0, fmt.Errorf("missing cell address for index %d", cellIndex)
	}

	return int(page.CellAddresses[cellIndex]), nil
}

func CellData(page *Page, cellIndex int) ([]byte, error) {
	offset, err := CellOffset(page, cellIndex)
	if err != nil {
		return nil, err
	}

	if offset >= len(page.Data) {
		return nil, fmt.Errorf("cell offset %d exceeds page data", offset)
	}

	return page.Data[offset:], nil
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

	// Read row metadata
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

	// Read serial types into each column
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

	// Read column values into each column
	for i := range row.Columns {
		length, err := columnRawValueLength(row.Columns[i].SerialType)
		if err != nil {
			return nil, fmt.Errorf("cell %d: column %d: %w", cellIndex, i, err)
		}

		var payload []byte
		if length > 0 {
			payload = make([]byte, length)
			if _, err := io.ReadFull(cellReader, payload); err != nil {
				return nil, fmt.Errorf("cell %d: read column %d payload: %w", cellIndex, i, err)
			}
		}

		value, err := decodeColumnValue(row.Columns[i].SerialType, payload)
		if err != nil {
			return nil, fmt.Errorf("cell %d: column %d: %w", cellIndex, i, err)
		}
		row.Columns[i].DecodedValue = value
	}

	return row, nil
}

func ReadAllRows(page *Page) ([]*Row, error) {
	rows := make([]*Row, 0, int(page.CellCount))

	for i := 0; i < int(page.CellCount); i++ {
		row, err := ReadRow(page, i)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}

	return rows, nil
}

func columnRawValueLength(serialType uint64) (int, error) {
	switch serialType {
	case 0, 8, 9:
		return 0, nil
	case 1:
		return 1, nil
	case 2:
		return 2, nil
	case 3:
		return 3, nil
	case 4:
		return 4, nil
	case 5:
		return 6, nil
	case 6, 7:
		return 8, nil
	case 10, 11:
		return 0, fmt.Errorf("reserved serial type %d", serialType)
	}

	if serialType >= 12 {
		if serialType%2 == 0 {
			return int((serialType - 12) / 2), nil
		}
		return int((serialType - 13) / 2), nil
	}

	return 0, fmt.Errorf("unsupported serial type %d", serialType)
}

func decodeColumnValue(serialType uint64, raw []byte) (any, error) {
	expectedLen, err := columnRawValueLength(serialType)
	if err != nil {
		return nil, err
	}
	if expectedLen != len(raw) {
		return nil, fmt.Errorf("serial type %d expects %d bytes, got %d", serialType, expectedLen, len(raw))
	}

	switch serialType {
	case 0:
		return nil, nil
	case 1, 2, 3, 4, 5, 6:
		return decodeSignedInteger(raw), nil
	case 7:
		return math.Float64frombits(binary.BigEndian.Uint64(raw)), nil
	case 8:
		return int64(0), nil
	case 9:
		return int64(1), nil
	}

	if serialType >= 12 {
		if serialType%2 == 0 {
			return append([]byte(nil), raw...), nil
		}
		return string(raw), nil
	}

	return nil, fmt.Errorf("unsupported serial type %d", serialType)
}

func decodeSignedInteger(raw []byte) int64 {
	var value int64
	for _, b := range raw {
		value = (value << 8) | int64(b)
	}
	shift := (8 - len(raw)) * 8
	return int64(uint64(value<<shift) >> shift)
}
