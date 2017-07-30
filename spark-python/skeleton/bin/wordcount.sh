#!/usr/bin/env bash

BASEDIR=$(readlink -f $(dirname $0)/..)

source $BASEDIR/bin/functions.sh

run "$@"
