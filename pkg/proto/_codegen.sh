#!/bin/bash

rm ../api-go/*.pb.go 2>/dev/null

protoc --go_out=../ *.proto
protoc --go-grpc_out=../ *.proto
