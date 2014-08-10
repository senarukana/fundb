#FunDB
A distributed database based on LevelDB.

| Don't use it. It's expreimental and under development.
#Progress
1. [Done] SQL parser, simple SQL Query.
2. [TODO] Write Buffer, Write Ahead log
3. [TODO] DIstributed shard

#Building
You'll need the following dependencies: protobuf, flex

    go get code.google.com/p/goprotobuf/

in linux:
    
    sudo apt-get install bison

in max:
    
    home brew bison

then:
    
    make