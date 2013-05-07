#!/bin/sh

INTERBASE_HOME=/opt/interbase

gcc -I "$INTERBASE_HOME"/include -c -O -fpic sym_udf.c
ld -G sym_udf.o -lm -lc -lib_util -o sym_udf.so
cp sym_udf.so "$INTERBASE_HOME_HOME"/UDF/
