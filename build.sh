#!/bin/sh
rm -rf data/
cd protocol
./build
cd ..

cd parser
make
cd ..

go build
go install