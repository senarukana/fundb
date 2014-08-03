sql.go: grammer.y
	go tool yacc -o grammer.go -p FunDB grammer.y
	gofmt -w grammer.go

clean:
	rm -f y.output grammer.go