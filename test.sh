#!/bin/sh -e

clear

rm -fr build
python ./setup.py build

echo ----------------------------------------

PYTHONPATH=.:build/lib.macosx-10.4-x86_64-2.7 python ./src/memcev.py


