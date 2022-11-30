# Description

A simple demo database that uses raft and gossip working togather to make a data store that is distrubuted, consistent, and HA. 

For this project, I always favor readablity and simplicity of the code over maintainablity.  The goal of this project is to be a teaching and learning tool, not something one would use in production. 

### TODO

- [x] Integrate gossip/epidemic protocal for node discovery 
- [x] Integrate Raft for leader election, and as the atomic commitment protocol (ACP) for the K/V store
- [x] Add multiple column support (only support strings out of the box)
- [ ] Add type support for differnt columns.
- [ ] Integrate QLBridge to add a SQL queries to begin with only support SQL as an API param - https://github.com/araddon/qlbridge 
- [ ] Add full MYSQL driver support using DataUX - https://github.com/dataux/dataux
- [ ] Stop using Raft for K/V storage ( I will conintue to use it for leader-discovery/metadata/leader election ). Switch K/V storage to CRAQ (Chain Replications with Apportioned Queries) - https://github.com/despreston/go-craq

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
curl -XPOST localhost:8001/key/_update -d'{"table":"t1", "key":"k1","column":"state", "value":"WA"}'

# Now read that key from any node
curl -XPOST localhost:8000/key/_fetch -d'{"table":"t1", "key":"k1"}'
curl -XPOST localhost:8001/key/_fetch -d'{"table":"t1", "key":"k1"}'

```

## Thanks to

 Using the `github.com/jen20/hashiconf-raft` project, gave me a head start on understanding the hashicorp raft library.
 
