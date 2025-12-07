#!/bin/bash

rm ../api-go/*.pb.go 2>/dev/null
buf generate
