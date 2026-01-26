package dbobjects

import (
	"fmt"
	"io"
)

func ReadVarint(stream io.ByteReader) (uint64, int, error) {
	var result uint64
	var err error
	var raw byte
	var read int

	for range 9 {
		// Make room for "data" bits
		result <<= 7
		raw, err = stream.ReadByte()
		read += 1
		// Take 7 "data" bits
		result |= uint64(raw & 0x7f)
		// Check "continuation" bit
		if (raw & 0x80) == 0 {
			break
		}
	}
	return result, read, err
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
