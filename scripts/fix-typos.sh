#!/bin/bash

set -e

cd `dirname $0`/..

cargo install --locked typos-cli@1.34.0
typos -w
