#!/bin/sh

cd protocol
protoc --go_out=. field.proto
protoc --go_out=. --proto_path= field.proto record.proto
go build
cd ..

cd parser
make
go build
cd ..

go build