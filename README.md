# Description

A simple demo of using raft and gossip working togather to make a data store that is distrubuted, consistent and HA key value data store. 

## Build

```bash
$ go get github.com/epsniff/expodb
$ cd $GOPATH/src/github.com/epsniff/expodb
$ dep ensure
$ cd ./scripts
$ # Running three nodes, each in their own terminal.
$ bash run.sh 1 
$ bash run.sh 2 
$ bash run.sh 3 
```

## Thanks to

 Using the `github.com/jen20/hashiconf-raft` project, gave me a head start on understanding the hashicorp raft libary..
 
