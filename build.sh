#!/bin/sh
rm -rf data/*
cd protocol
./build.sh
cd ..

cd parser
make
cd ..

cd apps/configd
go build
cd ../..

go build

# cd test
# go build