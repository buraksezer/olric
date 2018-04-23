#!/bin/bash
TMP=$(mktemp /tmp/olricdb-coverage-XXXXX.txt)
BUILD=$1
OUT=$2
# create coverage output
echo 'mode: atomic' > $OUT
for PKG in $(go list ./...|grep -v -E 'vendor'); do
        go test -covermode=atomic -coverprofile=$TMP $PKG
        tail -n +2 $TMP >> $OUT
done
