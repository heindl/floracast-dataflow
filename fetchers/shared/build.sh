#!/usr/bin/env bash

rm -rf ./dist/
python setup.py sdist --formats=gztar
cp -f ./dist/shared-0.0.1.tar.gz ../protected_areas/dist/.
pip uninstall shared
pip install ./dist/shared-0.0.1.tar.gz