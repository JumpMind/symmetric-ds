#!/bin/sh

FIREBIRD_HOME=/opt/firebird

gcc -I "$FIREBIRD_HOME"/include -c -O -fpic sym_udf.c
ld -G sym_udf.o -lm -lc -lib_util -o sym_udf.so
cp sym_udf.so "$FIREBIRD_HOME"/UDF/
