#!/usr/bin/env bash
javac -d out/ -cp lib/*:out/:. src/pt/inescid/gsd/cachemining/*.java
jar cf lib/cache-mining.jar -C src/ .
