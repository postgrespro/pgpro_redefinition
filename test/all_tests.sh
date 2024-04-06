#!/bin/bash
set -e
./deferred/03-copy-callback-auto/test.sh
./deferred/04-copy-callback-hand/test.sh
./deferred/05-any-callback-hand_update_col/test.sh
./deferred/06-copy-fdw-callback-auto/test.sh

./online/01/test.sh
./online/02/test.sh




