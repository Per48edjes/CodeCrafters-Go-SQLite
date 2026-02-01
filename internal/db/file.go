package db

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

const databaseHeaderBytes = 100

type DatabaseFile struct {
	*os.File
}

type DatabaseHeader struct {
	PageSize  uint16
	PageCount uint32
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

func LoadPage(path string, pageNum uint32) (*DatabaseHeader, *Page, error) {
	if pageNum == 0 {
		return nil, nil, errors.New("page numbers start at 1")
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("open database: %w", err)
	}
	defer file.Close()

	dbFile := &DatabaseFile{File: file}
	header, err := dbFile.NewDatabaseHeader()
	if err != nil {
		return nil, nil, fmt.Errorf("read database header: %w", err)
	}

	page, err := dbFile.NewPage(header, pageNum)
	if err != nil {
		return nil, nil, fmt.Errorf("read schema page: %w", err)
	}

	return header, page, nil
}
