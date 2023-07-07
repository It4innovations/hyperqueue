#!/bin/bash

set -e

cd `dirname $0`/..

python3 -m pytest tests --inline-snapshot=fix

# Reformat code to wrap long lines
./check.sh
