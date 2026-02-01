package main

import (
	"log"
	"os"

	"github.com/codecrafters-io/sqlite-starter-go/internal/cli"
)

// Usage: your_program.sh sample.db <command>
func main() {
	if len(os.Args) < 3 {
		log.Fatalf("usage: %s <database> <command>", os.Args[0])
	}

	databaseFilePath := os.Args[1]
	command := os.Args[2]

	var err error

	switch command {
	case ".dbinfo":
		err = cli.HandleDBInfo(databaseFilePath)
	case ".tables":
		err = cli.HandleTables(databaseFilePath)
	default:
		err = cli.HandleQuery(databaseFilePath, command)
	}

	if err != nil {
		log.Fatal(err)
	}
}
