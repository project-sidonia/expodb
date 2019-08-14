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
# Set a key on node 1
curl -XPOST localhost:8001/key/foo -d'{"value":"bar-42"}'

# Now read that key from node 0
curl localhost:8000/key/foo # Should return "bar-42" 

# Now set a key using node 2
curl -XPOST localhost:8002/key/foo -d'{"value":"bar-24"}'

# Now read the key from node 1 again
curl localhost:8001/key/foo # Should return "bar-24"
```

## Thanks to

 Using the `github.com/jen20/hashiconf-raft` project, gave me a head start on understanding the hashicorp raft libary..
 
