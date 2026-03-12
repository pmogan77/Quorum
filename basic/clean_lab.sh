#!/usr/bin/env bash
set -e

kathara lclean

rm -f *.startup
rm -f lab.conf

rm -rf s[0-9]*

rm -rf c[0-9]*

rm -rf client