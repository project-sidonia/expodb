# Description

A simple demo of using raft and gossip working togather to make a data store that is distrubuted, consistent and HA key value data store. 

## Build and Running

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

## Testing

```bash
# Set a key on node 0
curl -XPOST localhost:8000/key/_update -d'{"table":"t1", "key":"k1","column":"name", "value":"eric"}'
curl -XPOST localhost:8000/key/_update -d'{"table":"t1", "key":"k1","column":"state", "value":"WA"}'

# Now read that key from node 0
curl -XPOST localhost:8000/key/_fetch -d'{"table":"t1", "key":"k1"}'

```

## Thanks to

 Using the `github.com/jen20/hashiconf-raft` project, gave me a head start on understanding the hashicorp raft library.
 
