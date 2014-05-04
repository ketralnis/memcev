#!/bin/bash

set -e

clear

rm -fr build
python ./setup.py build

echo ----------------------------------------

export PYTHONPATH=.:build/lib.macosx-10.4-x86_64-2.7

if [ "$1" = "debug" ]; then
  gdb -batch -x <(printf 'run\nbt\n') --args $(which python) ./src/memcev.py
elif [ "$1" = "valgrind" ]; then
  valgrind $(which python) ./src/memcev.py
else
  python ./src/memcev.py
fi


