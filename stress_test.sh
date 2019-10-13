#!/usr/bin/env bash
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd $DIR || exit 1
./gradlew clean test jar
java -cp ./build/libs/orderbook.jar StressTest
