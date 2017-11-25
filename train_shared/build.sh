#!/usr/bin/env bash

rm -rf ./dist/
python setup.py sdist --formats=gztar
#cp -f ./dist/shared-0.0.1.tar.gz ../fetch_protected_areas/dist/.
#cp -f ./dist/shared-0.0.1.tar.gz ../fetch_random_areas/dist/.
#cp -f ./dist/shared-0.0.1.tar.gz ../fetch_occurrences/dist/.
pip uninstall train_shared
pip install ./dist/train_shared-0.0.1.tar.gz