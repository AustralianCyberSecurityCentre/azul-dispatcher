#!/bin/bash

# this script will retrieve testing data from azul dispatcher, so that backup program can be verified.
# check source code for more details

set -e
# Remove old binary to ensure running latest build (in case build fails.)
[ -e ./bin/update ] && rm ./bin/update
go build -v -tags static_all -o ./bin/update ./update/main.go

chmod +x ./bin/*

./bin/update