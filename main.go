package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/senarukana/fundb/core"
)

func create_table(engine *core.EngineHandler) {
	createQuery := "CREATE TABLE test INCREMENT"
	response := engine.Query(createQuery)
	if response.Error != nil {
		log.Printf("Query Error:%s\n", response.Error)
	}
}

func insert(engine *core.EngineHandler) {
	insertQuery := "INSERT INTO test (id, name, age) VALUES (1, 'li', 25), (2, 'ted', 25)"
	insertQuery2 := "INSERT INTO test (id, name, age) VALUES (3, 'hu', 25), (4, 'zheng', 25)"
	response := engine.Query(insertQuery)
	if response.Error != nil {
		log.Fatalf("Query Error:%s\n", response.Error)
	}

	response = engine.Query(insertQuery2)
	if response.Error != nil {
		log.Fatalf("Query Error:%s\n", response.Error)
	}
}

func fetch(engine *core.EngineHandler, sql string) {

	response := engine.Query(sql)

	if response.Error != nil {
		log.Fatalf("Query Error:%s\n", response.Error)
	}
	fmt.Printf("SQL : %s\n", sql)
	fmt.Printf("Rows Affected: %d\n", response.RowsAffected)
	fmt.Printf("Table: %s\n", response.Results.GetName())
	for _, field := range response.Results.Fields {
		fmt.Printf("%s\t\t", field)
	}
	fmt.Println()
	for _, record := range response.Results.GetValues() {
		for _, item := range record.GetValues() {
			fmt.Print(item.String() + "\t")
		}
		fmt.Println()
	}
	fmt.Println()
}

func main() {
	flag.Parse()
	engine, err := core.NewEngineHandler("leveldb", "data")
	if err != nil {
		log.Fatalln(err)
	}
	create_table(engine)
	insert(engine)

	fetchBetweenQuery := "SELECT _id, id, name FROM test WHERE _id between 1 and 3"
	fetchGreaterQuery := "SELECT _id, id, name FROM test WHERE _id > 2"
	fetchSmallerQuery := "SELECT _id, id, name FROM test WHERE _id < 2"
	fetchEqualQuery := "SELECT _id, id, name FROM test WHERE _id = 2"
	fetch(engine, fetchBetweenQuery)
	fetch(engine, fetchGreaterQuery)
	fetch(engine, fetchSmallerQuery)
	fetch(engine, fetchEqualQuery)
}
