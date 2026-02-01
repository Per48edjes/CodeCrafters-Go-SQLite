package db

import (
	"encoding/binary"
	"fmt"
	"io"
)

type BTreePageType uint8

const (
	InteriorIndex BTreePageType = 2
	InteriorTable BTreePageType = 5
	LeafIndex     BTreePageType = 10
	LeafTable     BTreePageType = 13
)

type Page struct {
	PageType      BTreePageType
	PageStart     int64
	ContentOffset int
	CellCount     uint16
	CellAddresses []uint16
	Data          []byte
}

func (databaseFile *DatabaseFile) NewPage(databaseHeader *DatabaseHeader, pageNumber uint32) (*Page, error) {
	start, pageSize, contentOffset, err := pageBounds(databaseHeader, pageNumber)
	if err != nil {
		return nil, err
	}

	page := &Page{PageStart: start, ContentOffset: contentOffset}
	page.Data = make([]byte, pageSize)

	sectionReader := io.NewSectionReader(databaseFile, page.PageStart, int64(pageSize))
	if _, err := io.ReadFull(sectionReader, page.Data); err != nil {
		return nil, fmt.Errorf("page %d: read bytes: %w", pageNumber, err)
	}

	if len(page.Data) == 0 {
		return nil, fmt.Errorf("page %d: no data", pageNumber)
	}

	offset := page.ContentOffset
	if offset >= len(page.Data) {
		return nil, fmt.Errorf("page %d: content offset beyond data", pageNumber)
	}

	typeFlag := page.Data[offset]
	offset++
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

func pageBounds(databaseHeader *DatabaseHeader, pageNumber uint32) (start int64, size uint16, contentOffset int, err error) {
	if databaseHeader == nil {
		return 0, 0, 0, fmt.Errorf("database header is nil")
	}

	if pageNumber == 0 {
		return 0, 0, 0, fmt.Errorf("page number must be greater than 0")
	}

	size = databaseHeader.PageSize
	if pageNumber == 1 {
		if size <= databaseHeaderBytes {
			return 0, 0, 0, fmt.Errorf("page size %d too small for header", size)
		}
		return 0, size, databaseHeaderBytes, nil
	}

	start = int64(pageNumber-1) * int64(size)
	return start, size, 0, nil
}
