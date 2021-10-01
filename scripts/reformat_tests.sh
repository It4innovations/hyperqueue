#!/bin/bash

cd `dirname $0`/..
isort --profile black tests
black tests
flake8 tests
