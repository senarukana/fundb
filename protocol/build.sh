#!/bin/sh
rm *.pb.go
protoc --go_out=. --plugin=$GOPATH/bin/protoc-gen-go protocol.proto

go build