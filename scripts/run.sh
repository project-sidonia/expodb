#!/usr/bin/env bash

set -o errexit
set -o pipefail

node_number=$1

function usage() {
    echo "Usage:"
    echo "   run-node.sh <node number (1-5)>"
}

if [[ "$PWD" == */scripts ]]; then
    echo "already in the scripts directory"
else
    echo "changing directory to scripts"
    pushd ./scripts
    function finish {
        popd
    }
    trap finish EXIT
fi

go build ../.

if [ -z ${node_number} ] ; then
    usage
    exit 1
fi

if ! [[ "${node_number}" =~ ^[1-5]$ ]] ; then
    usage
    exit 1
fi

node_serf_port=$((5999 + $node_number))
node_raft_port=$((6999 + $node_number))
node_http_port=$((7999 + $node_number))
data_directory="./node${node_number}"

args="--serf-data-dir=${data_directory}_serf"
args="${args} --raft-data-dir=${data_directory}_raft"
args="${args} --serf-port=${node_serf_port}"
args="${args} --raft-port=${node_raft_port}"
args="${args} --http-port=${node_http_port}"
args="${args} --node-name=${node_number}"

if [ "${node_number}" == 1 ] ; then
    args="${args} --bootstrap true --is-seed=true"
else if [ "${node_number}" == 2 ] ; then
    args="${args} --bootstrap true --serf-join=127.0.0.1:6000"
else if [ "${node_number}" == 3 ] ; then
    args="${args} --bootstrap true --serf-join=127.0.0.1:6000"
else
    args="${args}  --serf-join=127.0.0.1:6000"
fi

echo running ./expodb ${args}
./expodb ${args}

