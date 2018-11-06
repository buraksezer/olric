#!/bin/bash

TMP=$(mktemp /tmp/olric-coverage-XXXXX.txt)

BUILD=$1
OUT=$2

set -e

# create coverage output
echo 'mode: atomic' > $OUT
for PKG in $(go list ./...|grep -v -E 'vendor' | grep -v -E 'cmd' | 
        grep -v -E 'bufpool' | grep -v -E 'protocol' | grep -v -E 'transport'); do
  go test -covermode=atomic -coverprofile=$TMP $PKG
  tail -n +2 $TMP >> $OUT
done

popd &> /dev/null
