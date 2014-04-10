#!/bin/sh

del build; python ./setup.py build && PYTHONPATH=.:build/lib.macosx-10.4-x86_64-2.7 python src/hello.py

