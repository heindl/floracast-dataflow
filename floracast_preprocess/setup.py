#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Setup.py module for the workflow's worker utilities.
All the workflow related code is gathered in a package that will be built as a
source distribution, staged in the staging area for the workflow being run and
then installed in the workers when they start running.
This behavior is triggered by specifying the --setup_file command line option
when running the workflow for remote execution.
"""

from distutils.command.build import build as _build
import subprocess

import setuptools

if __name__ == '__main__':
    setuptools.setup(
        name='floracast_preprocess',
        version='0.0.1',
        description='Floracast dataflow pipeline.',
        packages=setuptools.find_packages(),
        install_requires=[
            "tensorflow",
            "tensorflow_transform",
            "apache-beam[gcp]",
            "google-cloud",
            "google-cloud-bigquery==0.26.0",
            "astral",
            "geopy",
            "google-cloud",
            "googlemaps",
            # "googledatastore",
            "mgrs",
            "numpy",
            "pandas",
            "pygeocoder",
            "python-dateutil"
        ]
    )