#!/bin/sh
rm *.pb.go
protoc --go_out=. node.proto
protoc --go_out=. field.proto
protoc --go_out=. --proto_path= field.proto record.proto
protoc --go_out=. request.proto
protoc --go_out=. shard.proto
protoc --go_out=. --proto_path= shard.proto table.proto

go build