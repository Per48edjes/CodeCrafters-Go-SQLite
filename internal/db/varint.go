package db

import "io"

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
