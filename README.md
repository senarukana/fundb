#FunDB
A distributed database based on LevelDB.

> Don't use it. It's expreimental and under development.

#Progress
1. [Done] SQL parser, simple SQL Query.
2. [Done] Config server
3. [Done] Write Buffer, Write Ahead log
4. [TODO] Shard
5. [TODO] Cross Shard Query

#Building
You'll need the following dependencies: protobuf, goprotobuf, bison

    go get code.google.com/p/goprotobuf/

in linux:
    
    sudo apt-get install bison

in max:
    
    home brew bison

then:
    
    make

#Architecture
![Alt architecture.png](/notes/architecture.png)