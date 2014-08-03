package main

import (
	"fmt"
	"os"

	"github.com/senarukana/fundb/parser"
)

func main() {
	query := "INSERT INTO T (id, name, age) VALUES (1, 'li', 25)"
	_, err := parser.ParseQuery(query)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}
