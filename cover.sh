#!/bin/bash

TMP=$(mktemp /tmp/olric-coverage-XXXXX.txt)

BUILD=$1
OUT=$2

set -e

# create coverage output
echo 'mode: atomic' > $OUT
for PKG in $(go list ./...| grep -v -E 'vendor'| grep -v -E 'cmd/olric-load'|
 grep -v -E 'cmd/olric-stats'| grep -v -E 'cmd/olricd'|grep -v -E 'hasher'|
 grep -v -E 'internal/bufpool'|grep -v -E 'cmd/discovery'|grep -v -E 'internal/flog'|
 grep -v -E 'serializer'|grep -v -E 'stats'); do
  go test -covermode=atomic -coverprofile=$TMP $PKG
  tail -n +2 $TMP >> $OUT
done

