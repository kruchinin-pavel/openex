#!/usr/bin/env bash
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd $DIR || exit 1
java -jar ./build/libs/orderbook.jar
