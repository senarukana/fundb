package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"strings"

	"github.com/senarukana/fundb/core"
)

const (
	insertFileName = "insert.sql"
	deleteFileName = "delete.sql"
	selectFileName = "select.sql"
)

func create_table(engine *core.EngineHandler) {
	createQuery := "CREATE TABLE test INCREMENT"
	response := engine.Query(createQuery)
	if response.Error != nil {
		log.Printf("Query Error:%s\n", response.Error)
	}
}

func insertTest(engine *core.EngineHandler) {
	contents, err := ioutil.ReadFile(insertFileName)
	if err != nil {
		log.Panic(fmt.Sprintf("read insert sql file: %s error: %s", insertFileName, err.Error()))
	}
	insertSqls := strings.Split(string(contents), "\n")
	for _, insertSql := range insertSqls {
		if insertSql == "" {
			continue
		}
		response := engine.Query(insertSql)
		if response.Error != nil {
			log.Fatalf("Query Error:%s\n", response.Error)
		}
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

func delete(engine *core.EngineHandler) {
	fmt.Println("----------------DELETE----------------")
	deleteQuery := "DELETE FROM test WHERE name = 'li'"
	response := engine.Query(deleteQuery)
	if response.Error != nil {
		log.Fatalf("Query Error:%s\n", response.Error)
	}
	fmt.Printf("SQL : %s\n", deleteQuery)
	fmt.Printf("Rows Affected: %d\n", response.RowsAffected)

	fmt.Println("----------------DELETE----------------")
	fmt.Println()
}

func main() {
	flag.Parse()
	engine, err := core.NewEngineHandler("leveldb", "data")
	if err != nil {
		log.Fatalln(err)
	}
	create_table(engine)
	insertTest(engine)

	selectAll := "select * from test"
	fetch(engine, selectAll)
}
