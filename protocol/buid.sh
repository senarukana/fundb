#!/bin/sh
protoc --go_out=. field.proto
protoc --go_out=. --proto_path= field.proto record.proto