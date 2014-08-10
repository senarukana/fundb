#!/bin/sh
rm *.pb.go
protoc --go_out=. field.proto
protoc --go_out=. --proto_path= field.proto record.proto
protoc --go_out=. table.proto
protoc --go_out=. request.proto

go build