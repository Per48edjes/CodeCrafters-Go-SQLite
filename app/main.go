package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

// Usage: your_program.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	switch command {
	case ".dbinfo":
		databaseFile, err := os.Open(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}

		databaseHeader := make([]byte, 100)

		_, err = databaseFile.Read(databaseHeader)
		if err != nil {
			log.Fatal(err)
		}

		pageSize := binary.BigEndian.Uint16(databaseHeader[16:18])

		pageHeader := make([]byte, 12)
		_, err = databaseFile.Read(pageHeader)
		if err != nil {
			log.Fatal(err)
		}

		tableCount := binary.BigEndian.Uint16(pageHeader[3:5])

		fmt.Printf("database page size: %v\n", pageSize)
		fmt.Printf("number of tables: %v", tableCount)
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
